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
 * above the generic memory cache: complete indexes accept inserts so they remain complete, while
 * the evictable remainder updates only an entry already resident. Both paths reject an older
 * store-assigned version.
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
  private final PointerCache pointers;
  private final ReentrantLock[] mutationLocks = new ReentrantLock[MUTATION_STRIPES];

  public CachingPointerStore(PointerStore delegate, PointerCache pointers) {
    this.delegate = delegate;
    this.pointers = pointers;
    for (int stripe = 0; stripe < mutationLocks.length; stripe++) {
      mutationLocks[stripe] = new ReentrantLock();
    }
  }

  // ---------------------------------------------------------------- reads

  @Override
  public Optional<Pointer> get(String key) {
    return pointers.get(key);
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
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      Optional<Pointer> fresh = delegate.getConsistent(key);
      pointers.repair(key, fresh);
      return fresh;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return Map.of();
    }
    return pointers.getBatch(keys);
  }

  /** The authoritative batch view bypasses the cache and repairs every key it read. */
  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return Map.of();
    }
    int[] stripes = mutationStripesForKeys(keys);
    lockMutations(stripes);
    try {
      Map<String, Pointer> fresh = delegate.getBatchConsistent(keys);
      for (String key : keys) {
        pointers.repair(key, Optional.ofNullable(fresh.get(key)));
      }
      return fresh;
    } finally {
      unlockMutations(stripes);
    }
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return pointers.list(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    lockAllMutations();
    try {
      String sourceToken = pointers.tokenForSource(pageToken);
      List<Pointer> fresh =
          delegate.listPointersByPrefixConsistent(prefix, limit, sourceToken, nextTokenOut);
      boolean exhausted = nextTokenOut != null && nextTokenOut.isEmpty();
      pointers.repairPrefix(prefix, pageToken, fresh, exhausted);
      if (nextTokenOut != null && !nextTokenOut.isEmpty() && !fresh.isEmpty()) {
        nextTokenOut.setLength(0);
        nextTokenOut.append(pointers.pageTokenAfterKey(fresh.getLast().getKey()));
      }
      return fresh;
    } finally {
      unlockAllMutations();
    }
  }

  @Override
  public int countByPrefix(String prefix) {
    return pointers.count(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    lockAllMutations();
    try {
      int count = delegate.countByPrefixConsistent(prefix);
      pointers.verifyCount(prefix, count);
      return count;
    } finally {
      unlockAllMutations();
    }
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return pointers.pageTokenAfterKey(key);
  }

  /** Straight through: emptiness is asked as a fence, and a cache cannot answer for the store. */
  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  // ---------------------------------------------------------------- writes

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      boolean won = delegate.compareAndSet(key, expectedVersion, next);
      if (won) {
        publishHeld(key, next, expectedVersion + 1L);
      } else {
        // A rejected CAS is authoritative evidence that the expected state is stale. A complete
        // index cannot evict one key (absence would become a wrong answer), so repair it from the
        // store while this mutation stripe is still fenced.
        pointers.repair(key, delegate.getConsistent(key));
      }
      return won;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean delete(String key) {
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      boolean deleted = delegate.delete(key);
      if (deleted) {
        evictHeld(key);
      } else {
        // Dynamo's best-effort delete returns false both for absence and when a concurrent update
        // wins between its read and CAS delete. Only a fresh read can distinguish those outcomes;
        // evicting unconditionally could turn the winning value into authoritative absence.
        pointers.repair(key, delegate.getConsistent(key));
      }
      return deleted;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    ReentrantLock lock = mutationLock(key);
    lock.lock();
    try {
      boolean deleted = delegate.compareAndDelete(key, expectedVersion);
      if (deleted) {
        evictHeld(key);
      } else {
        pointers.repair(key, delegate.getConsistent(key));
      }
      return deleted;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return delegate.compareAndSetBatch(ops);
    }
    int[] stripes = mutationStripes(ops);
    lockMutations(stripes);
    try {
      boolean won = delegate.compareAndSetBatch(ops);
      if (!won) {
        // The backend does not identify the failed condition. Any participating key may have made
        // a cached precondition stale, so repair all of them in one authoritative batch while the
        // participating stripes remain fenced.
        List<String> keys = ops.stream().map(CasOp::key).distinct().toList();
        Map<String, Pointer> fresh = delegate.getBatchConsistent(keys);
        keys.forEach(key -> pointers.repair(key, Optional.ofNullable(fresh.get(key))));
        return false;
      }
      // Publish every insertion before any removal. A rename may transiently retain the old alias,
      // but it must never create an authoritative-absence window between the two names.
      for (CasOp op : ops) {
        switch (op) {
          case CasUpsert upsert ->
              publishHeld(upsert.key(), upsert.next(), upsert.expectedVersion() + 1L);
          // The caller owns this version, so what it proposed is what the store holds.
          case UnconditionalUpsert upsert ->
              publishHeld(upsert.key(), upsert.next(), upsert.next().getVersion());
          default -> {}
        }
      }
      for (CasOp op : ops) {
        switch (op) {
          case CasDelete delete -> evictHeld(delete.key());
          case CasCheck check -> pointers.checkedPresent(check.key(), check.expectedVersion());
          case CasCheckAbsent check -> evictHeld(check.key());
          default -> {}
        }
      }
      return true;
    } finally {
      unlockMutations(stripes);
    }
  }

  @Override
  public int deleteByPrefix(String prefix) {
    lockAllMutations();
    try {
      int deleted = delegate.deleteByPrefix(prefix);
      pointers.removePrefix(prefix, null);
      return deleted;
    } finally {
      unlockAllMutations();
    }
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    lockAllMutations();
    try {
      int deleted = delegate.deleteByPrefixExcluding(prefix, excludedKey);
      pointers.removePrefix(prefix, excludedKey);
      return deleted;
    } finally {
      unlockAllMutations();
    }
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
   * <p>Still version-guarded for writes performed by another process. A complete index accepts a
   * new addressing key because that is how it remains complete; the evictable remainder stays
   * presence-guarded so a write to a cold key does not fill it. Local mutations hold the key's
   * stripe from durable mutation through publication, making their cache effects follow store
   * order. Those are pointer semantics, not properties of {@link MemoryCache#put}.
   */
  private void publishHeld(String key, Pointer next, long assignedVersion) {
    if (next == null) {
      return;
    }
    Pointer published = next.toBuilder().setKey(key).setVersion(assignedVersion).build();
    if (pointers.publish(key, published)) {
      return;
    }
    Optional<Pointer> cached = pointers.peekResident(key);
    if (cached.isPresent() && cached.orElseThrow().getVersion() < assignedVersion) {
      pointers.putResident(key, published);
    } else if (cached.filter(published::equals).isEmpty()) {
      // An absent entry may be an in-flight load, so evict to move the MemoryCache fence. A cached
      // higher version may instead belong to an older incarnation of a key that was deleted and
      // recreated. Without an incarnation in the schema, absence is the only safe common
      // resolution; the next read reloads it.
      pointers.evictResident(key);
    }
  }

  private void evictHeld(String key) {
    if (!pointers.remove(key)) {
      pointers.evictResident(key);
    }
  }

  private int[] mutationStripes(List<CasOp> ops) {
    return mutationStripesForKeys(ops.stream().map(CasOp::key).toList());
  }

  private int[] mutationStripesForKeys(List<String> keys) {
    boolean[] selected = new boolean[MUTATION_STRIPES];
    int count = 0;
    for (String key : keys) {
      int stripe = mutationStripe(key);
      if (!selected[stripe]) {
        selected[stripe] = true;
        count++;
      }
    }
    int[] stripes = new int[count];
    int next = 0;
    for (int stripe = 0; stripe < selected.length; stripe++) {
      if (selected[stripe]) {
        stripes[next++] = stripe;
      }
    }
    return stripes;
  }

  private ReentrantLock mutationLock(String key) {
    return mutationLocks[mutationStripe(key)];
  }

  private int mutationStripe(String key) {
    int spread = key.hashCode() * 0x9E3779B9;
    return (spread >>> 16) & (MUTATION_STRIPES - 1);
  }

  private void lockMutations(int[] stripes) {
    for (int stripe : stripes) {
      mutationLocks[stripe].lock();
    }
  }

  private void unlockMutations(int[] stripes) {
    for (int offset = stripes.length - 1; offset >= 0; offset--) {
      mutationLocks[stripes[offset]].unlock();
    }
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
