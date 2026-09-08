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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.CaffeineMemoryCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CachingPointerStoreTest {

  private static final String ACCT = "acct-1";
  private static final String TBL = "tbl-1";

  private final InMemoryPointerStore store = new InMemoryPointerStore();
  private final CaffeineMemoryCache<String, Pointer> cache =
      new CaffeineMemoryCache<>(
          CacheFamily.POINTER, 1024L * 1024L, key -> 2L * key.length(), CacheEvents.none());
  private final CachingPointerStore caching = new CachingPointerStore(store, cache);

  private static Pointer pointer(String key, String blobUri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(version).build();
  }

  /**
   * A store whose reads block until released, so a write can be interleaved with a fill.
   *
   * <p>The two races this class guards are both "a write landed while a read-through was in
   * flight". Nothing in a single-threaded test can produce that interleaving, which is why they
   * went unnoticed: the fill window is invisible unless something holds it open.
   */
  private static final class BlockingReads extends InMemoryPointerStore {
    private final CountDownLatch reading = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    // Armed explicitly, after the fixture is seeded. Seeding goes through compareAndSet, which
    // reads on its way in -- arming at construction would spend the one blocked read there and
    // leave the test passing with the race window never opened.
    private volatile boolean armed = false;

    void armOneRead() {
      armed = true;
    }

    @Override
    public Optional<Pointer> get(String key) {
      if (!armed) {
        return super.get(key);
      }
      armed = false;
      // Read FIRST, then hold. The interleaving that matters is a loader that already has the
      // pre-write value in hand when the write lands -- blocking before the read would simply
      // observe the write and install the right thing, which proves nothing.
      Optional<Pointer> asOfNow = super.get(key);
      reading.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return asOfNow;
    }
  }

  @Test
  void aPublishDuringAFillIsNotLeftBehindByTheOlderValue() throws Exception {
    BlockingReads blocking = new BlockingReads();
    CaffeineMemoryCache<String, Pointer> cache =
        new CaffeineMemoryCache<>(
            CacheFamily.POINTER, 1024L * 1024L, key -> 2L * key.length(), CacheEvents.none());
    CachingPointerStore caching = new CachingPointerStore(blocking, cache);
    String key = Keys.tableRootByTable(ACCT, TBL);
    blocking.compareAndSet(key, 0L, pointer(key, "s3://v1", 0L));

    blocking.armOneRead();
    var reader = CompletableFuture.supplyAsync(() -> caching.get(key));
    assertThat(blocking.reading.await(10, TimeUnit.SECONDS)).isTrue();

    // The key is absent while mid-load, so the presence guard declines to publish and moves the
    // memory-cache fence instead. The old load may still serve its caller but cannot remain held.
    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          blocking.release.countDown();
        });
    assertThat(caching.compareAndSet(key, 1L, pointer(key, "s3://v2", 0L))).isTrue();
    reader.get(10, TimeUnit.SECONDS);

    // Either state is fine; one is not. The cache must not be left holding the pre-write value,
    // because nothing would ever remove it.
    assertThat(cache.peek(key).map(Pointer::getBlobUri)).isNotEqualTo(Optional.of("s3://v1"));
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://v2");
  }

  @Test
  void aPrefixDeleteDuringAFillDoesNotLeaveTheDeletedPointerCached() throws Exception {
    BlockingReads blocking = new BlockingReads();
    CaffeineMemoryCache<String, Pointer> cache =
        new CaffeineMemoryCache<>(
            CacheFamily.POINTER, 1024L * 1024L, key -> 2L * key.length(), CacheEvents.none());
    CachingPointerStore caching = new CachingPointerStore(blocking, cache);
    String key = Keys.tableRootByTable(ACCT, TBL);
    blocking.compareAndSet(key, 0L, pointer(key, "s3://gone", 0L));

    blocking.armOneRead();
    var reader = CompletableFuture.supplyAsync(() -> caching.get(key));
    assertThat(blocking.reading.await(10, TimeUnit.SECONDS)).isTrue();

    // The partition eviction moves every memory-cache fence, including a key that is mid-load and
    // therefore not yet visible in the resident key set.
    caching.deleteByPrefix("/accounts/" + ACCT + "/");

    blocking.release.countDown();
    reader.get(10, TimeUnit.SECONDS);

    assertThat(cache.peek(key)).isEmpty();
  }

  @Test
  void aRecreateBelowTheCachedVersionIsNotRefusedForever() {
    String key = Keys.tableRootByTable(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://old", 0L));
    store.compareAndSet(key, 1L, pointer(key, "s3://old2", 0L));
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://old2");

    // Deleted and recreated elsewhere: the store restarts at version 1, BELOW what is cached. The
    // version guard is right to refuse the publish, and wrong to leave the old entry standing --
    // nothing expires, so this replica would never see its own write.
    store.delete(key);
    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://new", 0L))).isTrue();

    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://new");
  }

  /**
   * The guard that keeps the design honest. A pointer key can reach the store from anywhere --
   * TxChange.target_pointer_key is an RPC field -- so the decorator, not the caller, is what makes
   * publishing complete. A mutating method added to PointerStore and not overridden here would
   * write behind the cache's back, and with no expiry nothing would age the stale entry out.
   */
  @Test
  void everyMutatingStoreMethodIsIntercepted() {
    Set<String> mutating =
        Set.of(
            "compareAndSet",
            "compareAndDelete",
            "compareAndSetBatch",
            "delete",
            "deleteByPrefix",
            "deleteByPrefixExcluding");

    // Every method that is NOT a known read is treated as a write. Filtering the interface by
    // `mutating` first -- which is what this used to do -- made the check circular: a seventh
    // mutating method added to the SPI was removed from the comparison before it could fail it,
    // and the test stayed green while writes went behind the cache.
    Set<String> knownReads =
        Set.of(
            "get",
            "getConsistent",
            "getBatch",
            "getBatchConsistent",
            "listPointersByPrefix",
            "listPointersByPrefixConsistent",
            "countByPrefix",
            "countByPrefixConsistent",
            "isEmpty",
            // Neither touches stored state: one derives a page token from a key, the other is a
            // diagnostic no-op by default.
            "pageTokenAfterKey",
            "dump");

    Set<String> unclassified =
        java.util.Arrays.stream(PointerStore.class.getMethods())
            .map(Method::getName)
            .filter(name -> !knownReads.contains(name))
            .filter(name -> !mutating.contains(name))
            .collect(Collectors.toSet());
    assertThat(unclassified)
        .as(
            "a method was added to PointerStore and classified as neither a read nor a write;"
                + " decide which it is, and override it here if it writes")
        .isEmpty();

    assertThat(
            java.util.Arrays.stream(PointerStore.class.getMethods())
                .map(Method::getName)
                .collect(Collectors.toSet()))
        .as("a mutating method was renamed or removed; revisit the decorator")
        .containsAll(mutating);

    Set<String> overridden =
        java.util.Arrays.stream(CachingPointerStore.class.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .map(Method::getName)
            .collect(Collectors.toSet());
    assertThat(overridden)
        .as("every mutating method must be intercepted, or writes bypass the cache")
        .containsAll(mutating);
  }

  @Test
  void aCachedFamilyIsReadThroughOnce() {
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));

    assertThat(caching.get(key)).isPresent();
    store.delete(key); // behind the decorator's back: a hit must not go back to the store
    assertThat(caching.get(key)).isPresent();
  }

  @Test
  void everyPointerFamilyIsCachedWithoutAnAllowlist() {
    String key = Keys.accountDeletionMarker(ACCT);
    store.compareAndSet(key, 0L, pointer(key, "s3://marker", 1L));

    assertThat(caching.get(key)).isPresent();
    store.delete(key); // behind the decorator: the second read proves this family was retained
    assertThat(caching.get(key)).isPresent();
  }

  @Test
  void aWritePublishesRatherThanInvalidating() {
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    caching.compareAndSet(key, 1L, pointer(key, "s3://v2", 2L));

    // Served from cache, already carrying the new value: the writer paid, not the next reader.
    assertThat(cache.peek(key).orElseThrow().getBlobUri()).isEqualTo("s3://v2");
  }

  @Test
  void aRejectedWriteDropsTheStaleVersionAClientMayHaveUsed() {
    String key = Keys.connectorPointerById(ACCT, "connector");
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.compareAndSet(key, 1L, pointer(key, "s3://v2", 2L)); // another replica

    assertThat(caching.compareAndSet(key, 1L, pointer(key, "s3://loser", 2L))).isFalse();
    assertThat(cache.peek(key)).isEmpty();
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://v2");
  }

  @Test
  void aRejectedBatchDropsEveryPossiblyStalePrecondition() {
    String first = Keys.connectorPointerById(ACCT, "first");
    String second = Keys.connectorPointerById(ACCT, "second");
    store.compareAndSet(first, 0L, pointer(first, "s3://first-v1", 1L));
    store.compareAndSet(second, 0L, pointer(second, "s3://second-v1", 1L));
    caching.getBatch(List.of(first, second));

    store.compareAndSet(second, 1L, pointer(second, "s3://second-v2", 2L));

    assertThat(
            caching.compareAndSetBatch(
                List.of(
                    new PointerStore.CasCheck(first, 1L), new PointerStore.CasCheck(second, 1L))))
        .isFalse();
    assertThat(cache.peek(first)).isEmpty();
    assertThat(cache.peek(second)).isEmpty();
  }

  @Test
  void aWriteToAnUncachedKeyDoesNotInsertIt() {
    String key = Keys.tablePointerById(ACCT, TBL);

    caching.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));

    // Presence guard: a sweep over cold objects must not fill the cache through the write path.
    assertThat(cache.entryCount()).isZero();
  }

  @Test
  void everyDeleteShapeDropsTheEntry() {
    String key = Keys.tablePointerById(ACCT, TBL);

    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));
    caching.get(key);
    caching.delete(key);
    assertThat(cache.entryCount()).isZero();

    store.compareAndSet(key, 0L, pointer(key, "s3://b", 1L));
    caching.get(key);
    caching.compareAndDelete(key, 1L);
    assertThat(cache.entryCount()).isZero();

    store.compareAndSet(key, 0L, pointer(key, "s3://c", 1L));
    caching.get(key);
    caching.compareAndSetBatch(List.of(new PointerStore.CasDelete(key, 1L)));
    assertThat(cache.entryCount()).isZero();
  }

  @Test
  void successfulBatchChecksDropCachedAnswersTheyCannotRepublish() {
    String present = Keys.tablePointerById(ACCT, "present-check");
    String absent = Keys.tablePointerById(ACCT, "absent-check");
    store.compareAndSet(present, 0L, pointer(present, "s3://present", 1L));
    caching.get(present);
    cache.put(absent, pointer(absent, "s3://stale", 1L));

    assertThat(
            caching.compareAndSetBatch(
                List.of(
                    new PointerStore.CasCheck(present, 1L),
                    new PointerStore.CasCheckAbsent(absent))))
        .isTrue();

    assertThat(cache.peek(present)).isEmpty();
    assertThat(cache.peek(absent)).isEmpty();
  }

  @Test
  void aPrefixDeleteDropsWhatItCovers() {
    // The sixth door: both production stores override deleteByPrefixExcluding rather than
    // delegating, so it has to be intercepted in its own right.
    String kept = Keys.tablePointerById(ACCT, "keep");
    String swept = Keys.tablePointerById(ACCT, "sweep");
    store.compareAndSet(kept, 0L, pointer(kept, "s3://keep", 1L));
    store.compareAndSet(swept, 0L, pointer(swept, "s3://sweep", 1L));
    caching.get(kept);
    caching.get(swept);

    caching.deleteByPrefixExcluding(Keys.tablePointerByIdPrefix(ACCT), kept);

    assertThat(cache.peek(kept)).isPresent();
    assertThat(cache.entryCount()).isEqualTo(1L);
  }

  @Test
  void aConsistentReadRepairsWhatItDisproves() {
    // A consistent read is authoritative, and the store interface has no invalidation door -- so a
    // caller that has just proved the cached entry wrong has no other way to say so.
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.compareAndSet(key, 1L, pointer(key, "s3://v2", 2L)); // behind the cache's back
    assertThat(cache.peek(key).orElseThrow().getBlobUri()).isEqualTo("s3://v1");

    assertThat(caching.getConsistent(key).orElseThrow().getBlobUri()).isEqualTo("s3://v2");
    assertThat(cache.peek(key)).isEmpty();
    assertThat(caching.get(key).orElseThrow().getBlobUri()).isEqualTo("s3://v2");
  }

  @Test
  void aConsistentReadRepairsAKeyWhoseVersionWENTBACKWARDS() {
    // Delete and recreate elsewhere restarts the key at version 1, so the authoritative value is
    // legitimately below the cached one. A version-guarded write-back would refuse it forever.
    String key = Keys.relationPointerByName(ACCT, "cat", "ns", "sales");
    store.compareAndSet(key, 0L, pointer(key, "s3://t1", 1L));
    store.compareAndSet(key, 1L, pointer(key, "s3://t1", 2L));
    store.compareAndSet(key, 2L, pointer(key, "s3://t1", 3L));
    caching.get(key);
    assertThat(cache.peek(key).orElseThrow().getVersion()).isEqualTo(3L);

    store.delete(key); // dropped and recreated on another replica
    store.compareAndSet(key, 0L, pointer(key, "s3://t2", 1L));

    assertThat(caching.getConsistent(key).orElseThrow().getBlobUri()).isEqualTo("s3://t2");
    assertThat(cache.peek(key)).isEmpty();
    assertThat(caching.get(key).orElseThrow().getBlobUri()).isEqualTo("s3://t2");
  }

  @Test
  void aConsistentReadDropsAnEntryTheStoreNoLongerHas() {
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.delete(key); // behind the cache's back
    assertThat(caching.getConsistent(key)).isEmpty();
    assertThat(cache.peek(key)).isEmpty();
  }

  @Test
  void aBatchReadHoldsWhatItLoaded() {
    // A batch load runs outside the per-entry lock, so a value it returns may already have been
    // superseded by a write that found the key absent and skipped its presence-guarded publish.
    // Each key here carries the fence its peek observed, so such a write leaves the key absent
    // instead of the batch making the older value permanent.
    String hit = Keys.tablePointerById(ACCT, "hit");
    String miss = Keys.tablePointerById(ACCT, "miss");
    store.compareAndSet(hit, 0L, pointer(hit, "s3://hit", 1L));
    store.compareAndSet(miss, 0L, pointer(miss, "s3://miss", 1L));
    caching.get(hit);
    assertThat(cache.entryCount()).isEqualTo(1L);

    assertThat(caching.getBatch(List.of(hit, miss)))
        .containsOnlyKeys(hit, miss)
        .extractingByKey(miss)
        .satisfies(p -> assertThat(p.getBlobUri()).isEqualTo("s3://miss"));

    assertThat(cache.entryCount()).isEqualTo(2L);
    assertThat(cache.peek(miss).map(Pointer::getBlobUri)).contains("s3://miss");
  }

  @Test
  void aBatchDoesNotHoldAKeyTheStoreDidNotReturn() {
    // Absence is never cached: a key the batch asked for and did not get back must stay absent, or
    // a pointer that appears later would be invisible until something dropped the entry.
    String present = Keys.tablePointerById(ACCT, "present");
    String absent = Keys.tablePointerById(ACCT, "absent");
    store.compareAndSet(present, 0L, pointer(present, "s3://present", 1L));

    assertThat(caching.getBatch(List.of(present, absent))).containsOnlyKeys(present);

    assertThat(cache.peek(absent)).isEmpty();
    assertThat(cache.peek(present).map(Pointer::getBlobUri)).contains("s3://present");
  }

  @Test
  void aBatchHoldsEveryPointerFamily() {
    String table = Keys.tablePointerById(ACCT, "t");
    String connector = Keys.connectorPointerById(ACCT, "conn");
    store.compareAndSet(table, 0L, pointer(table, "s3://t", 1L));
    store.compareAndSet(connector, 0L, pointer(connector, "s3://c", 1L));

    assertThat(caching.getBatch(List.of(table, connector))).containsOnlyKeys(table, connector);

    assertThat(cache.peek(table)).isPresent();
    assertThat(cache.peek(connector)).isPresent();
  }

  @Test
  void aConsistentListRepairsCachedEntriesBelowItsPrefix() {
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));
    caching.get(key);
    store.delete(key);

    StringBuilder next = new StringBuilder();
    assertThat(
            caching.listPointersByPrefixConsistent(Keys.tablePointerByIdPrefix(ACCT), 10, "", next))
        .isEmpty();
    assertThat(caching.get(key)).isEmpty();
  }

  @Test
  void aConsistentCountRepairsCachedEntriesBelowItsPrefix() {
    String key = Keys.tablePointerById(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://a", 1L));
    caching.get(key);
    store.delete(key);

    assertThat(caching.countByPrefixConsistent(Keys.tablePointerByIdPrefix(ACCT))).isZero();
    assertThat(caching.get(key)).isEmpty();
  }

  @Test
  void theOffSwitchInstallsTheRawStoreRatherThanAnEmptyCache() {
    // Off has to mean the decorator is absent. Wiring an empty cache instead would leave every
    // read going through it to report a miss, so the metrics would show a cache that is not
    // helping rather than one that was turned off -- and a zero budget, the other way someone
    // might try this, is refused at startup.
    var raw = new InMemoryPointerStore();
    var caches = new ai.floedb.floecat.service.cache.MetadataCaches();

    assertThat(caches.cachedPointerStore(raw, cache, false)).isSameAs(raw);
    PointerStore cached = caches.cachedPointerStore(raw, cache, true);
    assertThat(cached).isInstanceOf(CachingPointerStore.class);
    assertThat(caches.pointerStore(cached)).isInstanceOf(AuthoritativePointerStore.class);
  }
}
