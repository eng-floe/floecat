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
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CachingPointerStoreTest {

  private static final String ACCT = "acct-1";
  private static final String TBL = "tbl-1";
  private static final long CACHE_BYTES = 1024L * 1024L;

  private final InMemoryPointerStore store = new InMemoryPointerStore();
  private final PointerCache cache = cacheFor(store);
  private final CachingPointerStore caching = new CachingPointerStore(store, cache);

  private static PointerCache cacheFor(PointerStore source) {
    return new PointerCache(AuthoritativePointerStore.of(source), CACHE_BYTES, CacheEvents.none());
  }

  private static Pointer pointer(String key, String blobUri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(version).build();
  }

  /** Holds a source read open so tests can place a write inside the cache-fill window. */
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
      return blockIfArmed(super.get(key));
    }

    @Override
    public Optional<Pointer> getConsistent(String key) {
      return blockIfArmed(super.getConsistent(key));
    }

    private Optional<Pointer> blockIfArmed(Optional<Pointer> asOfNow) {
      // Read FIRST, then hold. The interleaving that matters is a loader that already has the
      // pre-write value in hand when the write lands -- blocking before the read would simply
      // observe the write and install the right thing, which proves nothing.
      if (!armed) {
        return asOfNow;
      }
      armed = false;
      reading.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return asOfNow;
    }
  }

  private static class CountingReads extends InMemoryPointerStore {
    final AtomicInteger consistentPointReads = new AtomicInteger();
    final AtomicInteger listingReads = new AtomicInteger();

    @Override
    public Optional<Pointer> getConsistent(String key) {
      consistentPointReads.incrementAndGet();
      return super.getConsistent(key);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listingReads.incrementAndGet();
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }
  }

  private static final class FailingListings extends CountingReads {
    @Override
    public List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listingReads.incrementAndGet();
      throw new IllegalStateException("listing unavailable");
    }
  }

  private static final class BlockingListings extends CountingReads {
    private final CountDownLatch loaded = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile boolean armed;

    void armOneListing() {
      armed = true;
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      List<Pointer> asOfNow = super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
      if (!armed) {
        return asOfNow;
      }
      armed = false;
      loaded.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while holding the listing race open", interrupted);
      }
      return asOfNow;
    }

    @Override
    public List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      return listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }
  }

  private static final class ConcurrentListings extends InMemoryPointerStore {
    private final CountDownLatch entered = new CountDownLatch(4);
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maximumActive = new AtomicInteger();

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      int now = active.incrementAndGet();
      maximumActive.accumulateAndGet(now, Math::max);
      entered.countDown();
      try {
        if (!entered.await(10, TimeUnit.SECONDS)) {
          throw new AssertionError("complete-index subtree loads did not run concurrently");
        }
        return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while loading pointer subtrees", interrupted);
      } finally {
        active.decrementAndGet();
      }
    }

    @Override
    public List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      return listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }
  }

  private static final class BlockingDelete extends InMemoryPointerStore {
    private final CountDownLatch deleted = new CountDownLatch(1);
    private final CountDownLatch recreated = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile boolean armed;
    private volatile boolean holdingDelete;

    void armOneDelete() {
      armed = true;
    }

    @Override
    public boolean delete(String key) {
      boolean result = super.delete(key);
      if (!armed) {
        return result;
      }
      armed = false;
      holdingDelete = true;
      deleted.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while holding the delete race open", interrupted);
      }
      return result;
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      boolean result = super.compareAndSet(key, expectedVersion, next);
      if (holdingDelete && expectedVersion == 0L) {
        recreated.countDown();
      }
      return result;
    }
  }

  private static final class BlockingConsistentRead extends InMemoryPointerStore {
    private final CountDownLatch read = new CountDownLatch(1);
    private final CountDownLatch wrote = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile boolean armed;
    private volatile boolean holdingRead;

    void armOneRead() {
      armed = true;
    }

    @Override
    public Optional<Pointer> getConsistent(String key) {
      Optional<Pointer> result = super.getConsistent(key);
      if (!armed) {
        return result;
      }
      armed = false;
      holdingRead = true;
      read.countDown();
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while holding the consistent read open", interrupted);
      } finally {
        holdingRead = false;
      }
      return result;
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      boolean result = super.compareAndSet(key, expectedVersion, next);
      if (holdingRead && expectedVersion == 1L) {
        wrote.countDown();
      }
      return result;
    }
  }

  private static final class ConcurrentUpdateWinsDelete extends InMemoryPointerStore {
    private boolean armed;

    void arm() {
      armed = true;
    }

    @Override
    public synchronized boolean delete(String key) {
      if (!armed) {
        return super.delete(key);
      }
      armed = false;
      Pointer current = super.getConsistent(key).orElseThrow();
      super.compareAndSet(
          key, current.getVersion(), current.toBuilder().setBlobUri("s3://winner").build());
      return false;
    }
  }

  @Test
  void aCompleteListingLoadsOnceThenPaginatesWithoutStoreReads() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String prefix = Keys.tablePointerByNamePrefix(ACCT, "cat", "ns");
    String first = Keys.tablePointerByName(ACCT, "cat", "ns", "a");
    String second = Keys.tablePointerByName(ACCT, "cat", "ns", "b");
    reads.compareAndSet(first, 0L, pointer(first, "s3://a", 0L));
    reads.compareAndSet(second, 0L, pointer(second, "s3://b", 0L));

    StringBuilder next = new StringBuilder();
    assertThat(caching.listPointersByPrefix(prefix, 1, "", next))
        .extracting(Pointer::getKey)
        .containsExactly(first);
    assertThat(next).isNotEmpty();
    int readsAfterLoad = reads.listingReads.get();

    StringBuilder end = new StringBuilder();
    assertThat(caching.listPointersByPrefix(prefix, 1, next.toString(), end))
        .extracting(Pointer::getKey)
        .containsExactly(second);
    assertThat(end).isEmpty();
    assertThat(reads.listingReads).hasValue(readsAfterLoad);
    assertThat(reads.listingReads)
        .as("one consistent scan for each complete account subtree")
        .hasValue(4);
  }

  @Test
  void accountSubtreesLoadWithTheConfiguredParallelism() {
    ConcurrentListings reads = new ConcurrentListings();
    PointerCache cache =
        new PointerCache(
            AuthoritativePointerStore.of(reads),
            CACHE_BYTES,
            CacheEvents.none(),
            MetadataFanout.concurrent(4));
    CachingPointerStore caching = new CachingPointerStore(reads, cache);

    assertThat(caching.get(Keys.relationPointerByName(ACCT, "cat", "ns", "missing"))).isEmpty();

    assertThat(reads.maximumActive).hasValue(4);
  }

  @Test
  void completeIndexEventsCarryTheLogicalAccount() {
    String account = "account/with/slashes";
    AtomicReference<String> observedAccount = new AtomicReference<>();
    CacheEvents events =
        new CacheEvents() {
          @Override
          public CacheEvents forAccount(String accountId) {
            observedAccount.set(accountId);
            return this;
          }
        };
    var pointers = new PointerCache(AuthoritativePointerStore.of(store), CACHE_BYTES, events);
    var cached = new CachingPointerStore(store, pointers);

    assertThat(cached.get(Keys.relationPointerByName(account, "cat", "ns", "missing"))).isEmpty();

    assertThat(observedAccount).hasValue(account);
  }

  @Test
  void allElevenAddressingFamiliesAreComplete() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    List<String> keys =
        List.of(
            Keys.accountPointerById(ACCT),
            Keys.accountPointerByName("account"),
            Keys.catalogPointerById(ACCT, "cat"),
            Keys.catalogPointerByName(ACCT, "catalog"),
            Keys.namespacePointerById(ACCT, "ns"),
            Keys.namespacePointerByPath(ACCT, "cat", List.of("namespace")),
            Keys.tablePointerById(ACCT, "table"),
            Keys.tablePointerByName(ACCT, "cat", "ns", "table"),
            Keys.viewPointerById(ACCT, "view"),
            Keys.viewPointerByName(ACCT, "cat", "ns", "view"),
            Keys.relationPointerByName(ACCT, "cat", "ns", "relation"));
    keys.forEach(key -> reads.compareAndSet(key, 0L, pointer(key, "s3://" + key, 0L)));

    assertThat(caching.getBatch(keys)).containsOnlyKeys(keys.toArray(String[]::new));
    assertThat(reads.listingReads)
        .as("two global and four account subtree scans load every complete family")
        .hasValue(6);

    keys.forEach(reads::delete);
    assertThat(caching.getBatch(keys)).containsOnlyKeys(keys.toArray(String[]::new));
    assertThat(reads.listingReads).hasValue(6);
  }

  @Test
  void everySourcePageIsExhaustedBeforeAbsenceBecomesAuthoritative() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String last = "";
    for (int index = 0; index <= 1_000; index++) {
      last = Keys.tablePointerByName(ACCT, "cat", "ns", String.format("table-%04d", index));
      reads.compareAndSet(last, 0L, pointer(last, "s3://table/" + index, 0L));
    }
    String missing = Keys.tablePointerByName(ACCT, "cat", "ns", "missing");

    assertThat(caching.get(last)).isPresent();
    assertThat(caching.get(missing)).isEmpty();
    int readsAfterLoad = reads.listingReads.get();

    assertThat(readsAfterLoad).isEqualTo(5);
    assertThat(caching.get(missing)).isEmpty();
    assertThat(reads.listingReads).hasValue(readsAfterLoad);
  }

  @Test
  void aFailedIndexLoadNeverMakesAbsenceAuthoritative() {
    FailingListings reads = new FailingListings();
    PointerCache cache = cacheFor(reads);
    CachingPointerStore caching = new CachingPointerStore(reads, cache);
    String missing = Keys.relationPointerByName(ACCT, "cat", "ns", "missing");

    assertThat(caching.get(missing)).isEmpty();
    int sourceReadsAfterFailure = reads.consistentPointReads.get();

    assertThat(cache.completeAccountCount()).isZero();
    assertThat(cache.degradedAccountCount()).isEqualTo(1L);
    assertThat(caching.get(missing)).isEmpty();
    assertThat(reads.consistentPointReads.get()).isGreaterThan(sourceReadsAfterFailure);
  }

  @Test
  void anEncodedAccountSegmentLoadsItsActualDurablePartition() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String account = "account/with/slashes";
    String key = Keys.relationPointerByName(account, "cat", "ns", "table");
    reads.compareAndSet(key, 0L, pointer(key, "s3://relation", 0L));

    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://relation");

    assertThat(reads.listingReads).hasValue(4);
  }

  @Test
  void aControlPointerInsideALoadedSubtreeIsNotMistakenForComplete() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String complete = Keys.catalogPointerByName(ACCT, "catalog");
    String control = Keys.catalogOverlaysMarker(ACCT, "cat");
    reads.compareAndSet(complete, 0L, pointer(complete, "s3://catalog", 0L));
    reads.compareAndSet(control, 0L, pointer(control, "s3://marker", 0L));

    assertThat(caching.get(complete)).isPresent();
    reads.delete(control);

    assertThat(caching.get(control)).isEmpty();
    assertThat(reads.consistentPointReads).hasValue(1);
  }

  @Test
  void aDescendantOfACompleteLeafIsNotMistakenForAnotherAddressingPointer() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String complete = Keys.catalogPointerById(ACCT, "cat");
    String control = complete + "/control";
    reads.compareAndSet(complete, 0L, pointer(complete, "s3://catalog", 0L));
    reads.compareAndSet(control, 0L, pointer(control, "s3://control", 0L));

    assertThat(caching.get(complete)).isPresent();
    reads.delete(control);

    assertThat(caching.get(control)).isEmpty();
    assertThat(reads.consistentPointReads).hasValue(1);
  }

  @Test
  void absenceIsAuthoritativeAfterTheAccountIndexLoads() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String missing = Keys.relationPointerByName(ACCT, "cat", "ns", "missing");

    assertThat(caching.get(missing)).isEmpty();
    int sourceReadsAfterLoad = reads.consistentPointReads.get();
    int listingReadsAfterLoad = reads.listingReads.get();

    assertThat(caching.get(missing)).isEmpty();
    assertThat(reads.consistentPointReads).hasValue(sourceReadsAfterLoad);
    assertThat(reads.listingReads).hasValue(listingReadsAfterLoad);
  }

  @Test
  void aWritePublishesAPreviouslyAbsentKeyIntoACompleteIndex() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "new_table");

    assertThat(caching.get(key)).isEmpty();
    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://new", 0L))).isTrue();
    int sourceReadsAfterWrite = reads.consistentPointReads.get();
    int listingReadsAfterWrite = reads.listingReads.get();

    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://new");
    assertThat(reads.consistentPointReads).hasValue(sourceReadsAfterWrite);
    assertThat(reads.listingReads).hasValue(listingReadsAfterWrite);
  }

  @Test
  void anOrdinaryWriteConflictRepairsRatherThanDegradingACompleteAccount() {
    CountingReads reads = new CountingReads();
    PointerCache cache = cacheFor(reads);
    CachingPointerStore caching = new CachingPointerStore(reads, cache);
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "orders");
    reads.compareAndSet(key, 0L, pointer(key, "s3://current", 0L));
    assertThat(caching.get(key)).isPresent();

    assertThat(caching.compareAndSet(key, 0L, pointer(key, "s3://stale-client", 0L))).isFalse();

    assertThat(cache.completeAccountCount()).isEqualTo(1L);
    assertThat(cache.degradedAccountCount()).isZero();
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://current");
  }

  @Test
  void anEqualConsistentReadDoesNotEvictAResidentPointer() {
    CountingReads reads = new CountingReads();
    CachingPointerStore caching = new CachingPointerStore(reads, cacheFor(reads));
    String key = Keys.tableRootByTable(ACCT, TBL);
    reads.compareAndSet(key, 0L, pointer(key, "s3://root", 0L));

    assertThat(caching.get(key)).isPresent();
    assertThat(caching.getConsistent(key)).isPresent();
    assertThat(caching.get(key)).isPresent();

    assertThat(reads.consistentPointReads).hasValue(2);
  }

  @Test
  void aWriteRacingTheAccountLoadWinsTheCompleteIndex() throws Exception {
    BlockingListings reads = new BlockingListings();
    PointerCache cache = cacheFor(reads);
    CachingPointerStore caching = new CachingPointerStore(reads, cache);
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "orders");
    reads.compareAndSet(key, 0L, pointer(key, "s3://v1", 0L));
    reads.armOneListing();

    CompletableFuture<Optional<Pointer>> loading =
        CompletableFuture.supplyAsync(() -> caching.get(key));
    assertThat(reads.loaded.await(10, TimeUnit.SECONDS)).isTrue();
    CompletableFuture<Boolean> writing =
        CompletableFuture.supplyAsync(
            () -> caching.compareAndSet(key, 1L, pointer(key, "s3://v2", 0L)));

    reads.release.countDown();
    loading.get(10, TimeUnit.SECONDS);
    assertThat(writing.get(10, TimeUnit.SECONDS)).isTrue();
    int readsAfterWrite = reads.listingReads.get();

    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://v2");
    assertThat(reads.listingReads).hasValue(readsAfterWrite);
  }

  @Test
  void anAccountThatDoesNotFitDegradesToCorrectStoreReads() {
    CountingReads reads = new CountingReads();
    String prefix = Keys.tablePointerByNamePrefix(ACCT, "cat", "ns");
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "too_large");
    reads.compareAndSet(key, 0L, pointer(key, "s3://large", 0L));
    CachingPointerStore caching =
        new CachingPointerStore(
            reads, new PointerCache(AuthoritativePointerStore.of(reads), 1L, CacheEvents.none()));

    assertThat(caching.listPointersByPrefix(prefix, 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(key);
    int readsAfterFirst = reads.listingReads.get();

    assertThat(caching.listPointersByPrefix(prefix, 10, "", new StringBuilder()))
        .extracting(Pointer::getKey)
        .containsExactly(key);
    assertThat(reads.listingReads.get()).isGreaterThan(readsAfterFirst);
  }

  @Test
  void aPublishDuringAFillIsNotLeftBehindByTheOlderValue() throws Exception {
    BlockingReads blocking = new BlockingReads();
    PointerCache cache = cacheFor(blocking);
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
    PointerCache cache = cacheFor(blocking);
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
  void anAccountSweepDropsReadinessBeforeTheAccountIdCanBeReused() {
    CountingReads reads = new CountingReads();
    PointerCache cache = cacheFor(reads);
    CachingPointerStore caching = new CachingPointerStore(reads, cache);
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "orders");
    reads.compareAndSet(key, 0L, pointer(key, "s3://old", 0L));
    assertThat(caching.get(key)).isPresent();

    caching.deleteByPrefix(Keys.accountRootPrefix(ACCT));
    reads.compareAndSet(key, 0L, pointer(key, "s3://new", 0L));

    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://new");
    assertThat(cache.completeAccountCount()).isEqualTo(1L);
  }

  @Test
  void sweepingTheGlobalAccountsRootDoesNotParseItAsOneAccount() {
    assertThat(caching.deleteByPrefix(Keys.accountRootPrefix())).isZero();
  }

  @Test
  void aDeleteCannotRemoveANewerRecreationFromTheCompleteIndex() throws Exception {
    BlockingDelete store = new BlockingDelete();
    PointerCache cache = cacheFor(store);
    CachingPointerStore caching = new CachingPointerStore(store, cache);
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "orders");
    store.compareAndSet(key, 0L, pointer(key, "s3://old", 0L));
    assertThat(caching.get(key)).isPresent();
    store.armOneDelete();

    CompletableFuture<Boolean> deleting = CompletableFuture.supplyAsync(() -> caching.delete(key));
    assertThat(store.deleted.await(10, TimeUnit.SECONDS)).isTrue();
    CompletableFuture<Boolean> recreating =
        CompletableFuture.supplyAsync(
            () -> caching.compareAndSet(key, 0L, pointer(key, "s3://new", 0L)));

    // Without a mutation fence, the recreation reaches the store and publishes while the older
    // delete is paused. With the fence it waits here; either way release after a bounded probe.
    if (store.recreated.await(1, TimeUnit.SECONDS)) {
      assertThat(recreating.get(10, TimeUnit.SECONDS)).isTrue();
    }
    store.release.countDown();
    assertThat(deleting.get(10, TimeUnit.SECONDS)).isTrue();
    assertThat(recreating.get(10, TimeUnit.SECONDS)).isTrue();
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://new");
  }

  @Test
  void anOlderConsistentReadCannotOverwriteANewerLocalWrite() throws Exception {
    BlockingConsistentRead store = new BlockingConsistentRead();
    PointerCache cache = cacheFor(store);
    CachingPointerStore caching = new CachingPointerStore(store, cache);
    String key = Keys.tableRootByTable(ACCT, TBL);
    store.compareAndSet(key, 0L, pointer(key, "s3://old", 0L));
    assertThat(caching.get(key)).isPresent();
    store.armOneRead();

    CompletableFuture<Optional<Pointer>> reading =
        CompletableFuture.supplyAsync(() -> caching.getConsistent(key));
    assertThat(store.read.await(10, TimeUnit.SECONDS)).isTrue();
    CompletableFuture<Boolean> writing =
        CompletableFuture.supplyAsync(
            () -> caching.compareAndSet(key, 1L, pointer(key, "s3://new", 0L)));

    // Without a shared fence, the write reaches the store while the older read is paused and that
    // read later repairs the cache backwards. With the fence, the write waits until repair ends.
    if (store.wrote.await(1, TimeUnit.SECONDS)) {
      assertThat(writing.get(10, TimeUnit.SECONDS)).isTrue();
    }
    store.release.countDown();
    assertThat(reading.get(10, TimeUnit.SECONDS).map(Pointer::getBlobUri)).contains("s3://old");
    assertThat(writing.get(10, TimeUnit.SECONDS)).isTrue();
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://new");
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
  void aRejectedWriteRepairsTheStaleVersionAClientMayHaveUsed() {
    String key = Keys.connectorPointerById(ACCT, "connector");
    store.compareAndSet(key, 0L, pointer(key, "s3://v1", 1L));
    caching.get(key);

    store.compareAndSet(key, 1L, pointer(key, "s3://v2", 2L)); // another replica

    assertThat(caching.compareAndSet(key, 1L, pointer(key, "s3://loser", 2L))).isFalse();
    assertThat(cache.peek(key).map(Pointer::getBlobUri)).contains("s3://v2");
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://v2");
  }

  @Test
  void aRejectedBatchRepairsEveryPossiblyStalePrecondition() {
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
    assertThat(cache.peek(first).map(Pointer::getBlobUri)).contains("s3://first-v1");
    assertThat(cache.peek(second).map(Pointer::getBlobUri)).contains("s3://second-v2");
  }

  @Test
  void aDeleteLostToAConcurrentUpdateRepairsInsteadOfPublishingAbsence() {
    ConcurrentUpdateWinsDelete store = new ConcurrentUpdateWinsDelete();
    PointerCache cache = cacheFor(store);
    CachingPointerStore caching = new CachingPointerStore(store, cache);
    String key = Keys.tablePointerByName(ACCT, "cat", "ns", "table");
    store.compareAndSet(key, 0L, pointer(key, "s3://old", 0L));
    assertThat(caching.get(key)).isPresent();
    store.arm();

    assertThat(caching.delete(key)).isFalse();

    assertThat(cache.peek(key).map(Pointer::getBlobUri)).contains("s3://winner");
    assertThat(caching.get(key).map(Pointer::getBlobUri)).contains("s3://winner");
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
  void successfulBatchChecksKeepOnlyACompleteAnswerTheyProve() {
    String present = Keys.tablePointerById(ACCT, "present-check");
    String absent = Keys.tablePointerById(ACCT, "absent-check");
    store.compareAndSet(present, 0L, pointer(present, "s3://present", 1L));
    store.compareAndSet(absent, 0L, pointer(absent, "s3://stale", 1L));
    caching.get(present);
    store.delete(absent);

    assertThat(
            caching.compareAndSetBatch(
                List.of(
                    new PointerStore.CasCheck(present, 1L),
                    new PointerStore.CasCheckAbsent(absent))))
        .isTrue();

    assertThat(cache.peek(present).map(Pointer::getVersion)).contains(1L);
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
  void aBatchReadHoldsWhatItLoaded() {
    // A batch load runs outside the per-entry lock, so a value it returns may already have been
    // superseded by a write that found the key absent and skipped its presence-guarded publish.
    // Each key here carries the fence its peek observed, so such a write leaves the key absent
    // instead of the batch making the older value permanent.
    String hit = Keys.connectorPointerById(ACCT, "hit");
    String miss = Keys.connectorPointerById(ACCT, "miss");
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
