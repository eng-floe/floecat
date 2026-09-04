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
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class PointerCacheRecoveryTest {

  private static final String ACCOUNT = "acct-1";
  private static final long CACHE_BYTES = 1024L * 1024L;
  private static final Duration RETRY_DELAY = Duration.ofSeconds(30);

  private static final class RecoveringStore extends InMemoryPointerStore {
    private final AtomicInteger failuresRemaining = new AtomicInteger(1);
    private final AtomicInteger consistentPointReads = new AtomicInteger();
    private final AtomicInteger listingReads = new AtomicInteger();
    private final AtomicBoolean blockRecovery = new AtomicBoolean();
    private final CountDownLatch recoveryStarted = new CountDownLatch(1);
    private final CountDownLatch releaseRecovery = new CountDownLatch(1);

    void blockRecovery() {
      blockRecovery.set(true);
    }

    @Override
    public Optional<Pointer> getConsistent(String key) {
      consistentPointReads.incrementAndGet();
      return super.getConsistent(key);
    }

    @Override
    public List<Pointer> listPointersByPrefixConsistent(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listingReads.incrementAndGet();
      if (failuresRemaining.getAndDecrement() > 0) {
        throw new IllegalStateException("listing temporarily unavailable");
      }
      if (blockRecovery.compareAndSet(true, false)) {
        recoveryStarted.countDown();
        try {
          if (!releaseRecovery.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out while holding the recovery load open");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError("interrupted while holding the recovery load open", interrupted);
        }
      }
      return super.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut);
    }
  }

  private static final class MutableNanoTime implements LongSupplier {
    private final AtomicLong now = new AtomicLong();

    @Override
    public long getAsLong() {
      return now.get();
    }

    void advance(Duration elapsed) {
      now.addAndGet(elapsed.toNanos());
    }
  }

  @Test
  void aDegradedAccountRetriesAndBecomesCompleteAfterTheCooldown() {
    RecoveringStore store = new RecoveringStore();
    MutableNanoTime nanoTime = new MutableNanoTime();
    PointerCache cache = cacheFor(store, nanoTime);
    CachingPointerStore caching = new CachingPointerStore(store, cache);
    String missing = Keys.relationPointerByName(ACCOUNT, "cat", "ns", "missing");

    assertThat(caching.get(missing)).isEmpty();
    int listingsAfterFailure = store.listingReads.get();
    assertThat(cache.degradedAccountCount()).isEqualTo(1L);

    assertThat(caching.get(missing)).isEmpty();
    assertThat(store.listingReads).hasValue(listingsAfterFailure);

    nanoTime.advance(RETRY_DELAY);
    assertThat(caching.get(missing)).isEmpty();
    int sourceReadsAfterRecovery = store.consistentPointReads.get();

    assertThat(cache.completeAccountCount()).isEqualTo(1L);
    assertThat(cache.degradedAccountCount()).isZero();
    assertThat(caching.get(missing)).isEmpty();
    assertThat(store.consistentPointReads).hasValue(sourceReadsAfterRecovery);
  }

  @Test
  void readsFallBackInsteadOfWaitingForARecoveryLoad() throws Exception {
    RecoveringStore store = new RecoveringStore();
    MutableNanoTime nanoTime = new MutableNanoTime();
    PointerCache cache = cacheFor(store, nanoTime);
    CachingPointerStore caching = new CachingPointerStore(store, cache);
    String first = Keys.relationPointerByName(ACCOUNT, "cat", "ns", "first-missing");
    String second = Keys.relationPointerByName(ACCOUNT, "cat", "ns", "second-missing");

    assertThat(caching.get(first)).isEmpty();
    nanoTime.advance(RETRY_DELAY);
    store.blockRecovery();
    CompletableFuture<Optional<Pointer>> recovering =
        CompletableFuture.supplyAsync(() -> caching.get(first));
    assertThat(store.recoveryStarted.await(10, TimeUnit.SECONDS)).isTrue();

    CompletableFuture<Optional<Pointer>> fallback =
        CompletableFuture.supplyAsync(() -> caching.get(second));
    assertThat(fallback.get(1, TimeUnit.SECONDS)).isEmpty();
    assertThat(cache.loadingAccountCount()).isEqualTo(1L);

    store.releaseRecovery.countDown();
    assertThat(recovering.get(10, TimeUnit.SECONDS)).isEmpty();
    assertThat(cache.completeAccountCount()).isEqualTo(1L);
  }

  private static PointerCache cacheFor(RecoveringStore store, LongSupplier nanoTime) {
    return new PointerCache(
        AuthoritativePointerStore.of(store),
        CACHE_BYTES,
        CacheEvents.none(),
        MetadataFanout.serial(),
        RETRY_DELAY,
        nanoTime);
  }
}
