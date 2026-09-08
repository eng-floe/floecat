/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.cache.MemoryCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The pointer cache, wrapping the store rather than hooking each caller.
 *
 * <p>That placement is the whole design. Pointer keys reach the store from anywhere -- {@code
 * TxChange.target_pointer_key} is an RPC field, so a client can name any key it likes -- and no
 * enumeration of call sites could ever be complete. Wrapping the store means every write is seen by
 * construction, whoever built it, so there is no such thing as a write path that forgot to publish.
 *
 * <p>Which is also why this must cover <b>every</b> mutating method. All six: {@code
 * compareAndSet}, {@code compareAndDelete}, {@code compareAndSetBatch}, {@code delete}, {@code
 * deleteByPrefix} and {@code deleteByPrefixExcluding} -- the last of these being overridden by both
 * production stores rather than delegating, so it is a real sixth door and not a convenience over
 * the fifth. Missing one leaves the cache holding a pointer the store no longer has, and with no
 * expiry there is nothing to age it out.
 *
 * <p>Writes <b>publish</b> rather than invalidate: the writer already holds the new value, so the
 * next reader hits instead of paying for a reload. Pointer-specific version ordering lives here,
 * above the generic memory cache: a publish only replaces a key already cached and only when its
 * store-assigned version is newer.
 *
 * <p>The consistent reads are never served from the cache. They exist for deletion fencing and
 * mutation paths, where a cached answer would defeat the point of asking -- and because their
 * answer is authoritative, a single-key one also drops any cached entry it disagrees with.
 *
 * <p>Wired as the {@code @CachedPointerStore}. The unqualified store is an authoritative view over
 * this same object: ordinary reads bypass, while every write still comes back through this
 * decorator. Reaching the raw store past both views still requires {@code @RawPointerStore}, so no
 * write path can forget to publish.
 */
public final class CachingPointerStore implements PointerStore {

  private static final int MUTATION_STRIPES = 256;

  private final PointerStore delegate;
  private final MemoryCache<String, Pointer> pointers;
  private final ReentrantLock[] mutationLocks = new ReentrantLock[MUTATION_STRIPES];

  public CachingPointerStore(PointerStore delegate, MemoryCache<String, Pointer> pointers) {
    this.delegate = delegate;
    this.pointers = pointers;
    for (int stripe = 0; stripe < mutationLocks.length; stripe++) {
      mutationLocks[stripe] = new ReentrantLock();
    }
  }

  // ---------------------------------------------------------------- reads

  @Override
  public Optional<Pointer> get(String key) {
    // Absence is not cached here: a null value cannot be stored, and an absent pointer that later
    // appears must be visible. Completeness -- absence as an answer -- is a property of a loaded
    // account, not of a single miss, and arrives with the eager load.
    return Optional.ofNullable(pointers.get(key, k -> delegate.get(k).orElse(null)));
  }

  /**
   * Straight past the cache, and back through it. Its callers ask what a cache cannot answer for: a
   * CAS expected-version in the commit funnel, the resolving-pin root guard, and every read the GC
   * decides a deletion from -- all questions about precisely what the cache might be behind on.
   *
   * <p>The answer is authoritative, so it also repairs: an entry it disagrees with is dropped. A
   * caller that has just proved the cached entry wrong would otherwise have no way to say so -- the
   * store interface has no invalidation door -- and the entry would stay wrong until a local write,
   * since nothing expires.
   */
  @Override
  public Optional<Pointer> getConsistent(String key) {
    Optional<Pointer> fresh = delegate.getConsistent(key);
    repair(key, fresh);
    return fresh;
  }

  private void repair(String key, Optional<Pointer> fresh) {
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      // Drop rather than replace: a delete and recreate can restart below the cached version, and
      // the store read may itself have raced a local publish. Leaving the key absent is always
      // safe. Evict even when peek sees no entry, so an older in-flight load cannot install after
      // this authoritative read returned.
      if (pointers.peek(key).filter(cached -> cached.equals(fresh.orElse(null))).isEmpty()) {
        pointers.evict(key);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return Map.of();
    }
    // MemoryCache owns miss partitioning, one bulk store read, per-key telemetry, and fencing each
    // loaded value against a concurrent mutation. Omitted keys remain absent and are not cached.
    return pointers.getAll(keys, misses -> delegate.getBatch(List.copyOf(misses)));
  }

  /** The authoritative batch view bypasses the cache and repairs every key it read. */
  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return Map.of();
    }
    Map<String, Pointer> fresh = delegate.getBatchConsistent(keys);
    for (String key : keys) {
      repair(key, Optional.ofNullable(fresh.get(key)));
    }
    return fresh;
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    List<Pointer> fresh =
        delegate.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut);
    repairPrefix(prefix);
    return fresh;
  }

  @Override
  public int countByPrefix(String prefix) {
    return delegate.countByPrefix(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    int count = delegate.countByPrefixConsistent(prefix);
    repairPrefix(prefix);
    return count;
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return delegate.pageTokenAfterKey(key);
  }

  /** Straight through: emptiness is asked as a fence, and a cache cannot answer for the store. */
  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  // ---------------------------------------------------------------- writes

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    boolean won = delegate.compareAndSet(key, expectedVersion, next);
    if (won) {
      publish(key, next, expectedVersion + 1L);
    } else {
      // A rejected CAS is authoritative evidence that the expected cached state may be stale.
      // Drop it so a retry loop cannot spin forever on the same version.
      evict(key);
    }
    return won;
  }

  @Override
  public boolean delete(String key) {
    boolean deleted = delegate.delete(key);
    // Even a false result proves the store has no value now. A stale resident value must not
    // survive merely because another replica won the delete first.
    evict(key);
    return deleted;
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    boolean deleted = delegate.compareAndDelete(key, expectedVersion);
    // On failure the expected version may have come from a stale cached read. Evict so a retry can
    // observe what defeated it; on success the same eviction publishes absence.
    evict(key);
    return deleted;
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    boolean won = delegate.compareAndSetBatch(ops);
    if (ops == null) {
      return won;
    }
    if (!won) {
      // The backend does not identify the failed condition. Any participating key may have made a
      // cached precondition stale, so drop all of them and let the retry rebuild one coherent view.
      ops.forEach(op -> evict(op.key()));
      return false;
    }
    // The batch is atomic, so it either all applied or none of it did.
    for (CasOp op : ops) {
      switch (op) {
        case CasUpsert upsert ->
            publish(upsert.key(), upsert.next(), upsert.expectedVersion() + 1L);
        // The caller owns the version on this shape, so what it proposed IS what the store holds.
        case UnconditionalUpsert upsert ->
            publish(upsert.key(), upsert.next(), upsert.next().getVersion());
        case CasDelete delete -> evict(delete.key());
        // A successful check is authoritative but carries no value to publish. Evict even when the
        // cached version matches: delete-and-recreate can reuse a version for different content.
        case CasCheck check -> evict(check.key());
        case CasCheckAbsent check -> evict(check.key());
      }
    }
    return won;
  }

  @Override
  public int deleteByPrefix(String prefix) {
    int deleted = delegate.deleteByPrefix(prefix);
    evictPrefix(prefix, null);
    return deleted;
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    int deleted = delegate.deleteByPrefixExcluding(prefix, excludedKey);
    evictPrefix(prefix, excludedKey);
    return deleted;
  }

  /**
   * Publishes the pointer as the store now holds it, without asking the store.
   *
   * <p>The version is the one detail the caller does not own: a store assigns {@code
   * expectedVersion + 1} and ignores whatever the proposed pointer carried, so publishing the
   * proposal as-is would cache a version the store never wrote and every later CAS would read an
   * expected-version that cannot win. Reconstructing it is exact -- the write won, so the store
   * applied precisely this rule -- and it costs nothing, where re-reading cost a strongly
   * consistent round-trip per key, serially, inside a batch that had already committed. That turned
   * "the writer already holds the new value, so the next reader does not pay for a reload" into the
   * writer paying a bigger one.
   *
   * <p>Still version-guarded, because the publish is not atomic with the write: two writers can
   * interleave and the later commit must not be overwritten by the earlier one's publish. And still
   * presence-guarded, so a write to a key nobody has cached does not fill it from the write path.
   * Those are pointer semantics, not properties of {@link MemoryCache#put}.
   */
  private void publish(String key, Pointer next, long assignedVersion) {
    if (next == null) {
      return;
    }
    Pointer published = next.toBuilder().setKey(key).setVersion(assignedVersion).build();
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      Optional<Pointer> cached = pointers.peek(key);
      if (cached.isPresent() && cached.orElseThrow().getVersion() < assignedVersion) {
        pointers.put(key, published);
      } else if (cached.filter(published::equals).isEmpty()) {
        // An absent entry may be an in-flight load, so evict to move the MemoryCache fence. A
        // cached higher version may instead belong to an older incarnation of a key that was
        // deleted and recreated. Without an incarnation in the schema, absence is the only safe
        // common resolution; the next read reloads it.
        pointers.evict(key);
      }
    } finally {
      lock.unlock();
    }
  }

  private void evict(String key) {
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      pointers.evict(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Evicts unconditionally rather than only when the store reported deletions. A prefix delete that
   * removed nothing may still have raced one that did, and the cost of evicting a key that was
   * already gone is a reload.
   */
  private void evictPrefix(String prefix, String excludedKey) {
    lockAllMutations();
    try {
      pointers.evictPartition(
          key ->
              (prefix == null || prefix.isEmpty() || key.startsWith(prefix))
                  && !key.equals(excludedKey));
    } finally {
      unlockAllMutations();
    }
  }

  /**
   * A prefix answer can disprove any resident pointer below it. Until the listing layer owns a
   * complete name index that it can reconcile atomically, invalidating the prefix is the only safe
   * repair: a later cached get reloads instead of retaining a value the authoritative read may have
   * shown to be absent.
   */
  private void repairPrefix(String prefix) {
    evictPrefix(prefix, null);
  }

  private ReentrantLock mutationLock(String key) {
    int spread = key.hashCode() * 0x9E3779B9;
    return mutationLocks[(spread >>> 16) & (MUTATION_STRIPES - 1)];
  }

  private void lockAllMutations() {
    for (ReentrantLock lock : mutationLocks) {
      lock.lock();
    }
  }

  private void unlockAllMutations() {
    for (int stripe = mutationLocks.length - 1; stripe >= 0; stripe--) {
      mutationLocks[stripe].unlock();
    }
  }
}
