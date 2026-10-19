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

package ai.floedb.floecat.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CaffeineCoalescingExpiringCacheTest {

  private static final Duration HOLD = Duration.ofSeconds(30);

  private final AtomicLong nanos = new AtomicLong();
  private final ExecutorService pool = Executors.newFixedThreadPool(8);

  @AfterEach
  void shutDown() {
    pool.shutdownNow();
  }

  @Test
  void concurrentSameKeyMissesShareOneLoad() throws Exception {
    CoalescingExpiringCache<String, String> cache = cache(100, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    List<Future<String>> results = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      results.add(
          pool.submit(
              () ->
                  cache.get(
                      "key",
                      ignored -> {
                        loads.incrementAndGet();
                        started.countDown();
                        await(release);
                        return "value";
                      })));
    }

    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    release.countDown();
    for (Future<String> result : results) {
      assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("value");
    }
    assertThat(loads).hasValue(1);
  }

  @Test
  void slowLoadDoesNotBlockSameBinKeysOrInvalidation() throws Exception {
    CoalescingExpiringCache<SameBin, String> cache = cache(100, CacheEvents.none());
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> slow =
        pool.submit(
            () ->
                cache.get(
                    new SameBin(0),
                    ignored -> {
                      started.countDown();
                      await(release);
                      return "slow";
                    }));

    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(
              pool.submit(() -> cache.get(new SameBin(1), ignored -> "other"))
                  .get(5, TimeUnit.SECONDS))
          .isEqualTo("other");
      assertThat(
              pool.submit(
                      () -> {
                        cache.invalidate(new SameBin(2));
                        return true;
                      })
                  .get(5, TimeUnit.SECONDS))
          .isTrue();
    } finally {
      release.countDown();
    }
    assertThat(slow.get(5, TimeUnit.SECONDS)).isEqualTo("slow");
  }

  @Test
  void invalidationDuringLoadPreventsStaleAdmission() throws Exception {
    CoalescingExpiringCache<String, String> cache = cache(100, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> stale =
        pool.submit(
            () ->
                cache.get(
                    "key",
                    ignored -> {
                      loads.incrementAndGet();
                      started.countDown();
                      await(release);
                      return "stale";
                    }));

    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    cache.invalidate("key");
    release.countDown();

    assertThat(stale.get(5, TimeUnit.SECONDS)).isEqualTo("stale");
    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v2");
    assertThat(loads).hasValue(2);
  }

  @Test
  void expiresAndCanBeInvalidated() {
    CoalescingExpiringCache<String, String> cache = cache(100, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();

    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v1");
    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v1");
    cache.invalidate("key");
    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v2");
    nanos.addAndGet(HOLD.plusNanos(1).toNanos());
    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v3");

    assertThat(cache.estimatedSize()).isEqualTo(1);
    cache.invalidateAll();
    assertThat(cache.estimatedSize()).isZero();
  }

  @Test
  void failuresAreNotHeld() {
    CoalescingExpiringCache<String, String> cache = cache(100, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();

    assertThatThrownBy(
            () ->
                cache.get(
                    "key",
                    ignored -> {
                      loads.incrementAndGet();
                      throw new IllegalStateException("source unavailable");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(cache.get("key", ignored -> "v" + loads.incrementAndGet())).isEqualTo("v2");
    assertThat(loads).hasValue(2);
  }

  @Test
  void reportsSharedCacheEvents() {
    List<String> seen = new ArrayList<>();
    CoalescingExpiringCache<String, String> cache = cache(100, recording(seen));

    cache.get("key", ignored -> "value");
    cache.get("key", ignored -> "unused");
    assertThatThrownBy(
        () ->
            cache.get(
                "failed",
                ignored -> {
                  throw new IllegalStateException("boom");
                }));

    assertThat(seen).containsExactly("miss", "load", "hit", "miss", "failed");
  }

  @Test
  void maximumSizeBoundsEntries() {
    CoalescingExpiringCache<String, String> cache = cache(2, CacheEvents.none());

    cache.get("one", key -> key);
    cache.get("two", key -> key);
    cache.get("three", key -> key);

    assertThat(cache.estimatedSize()).isLessThanOrEqualTo(2);
  }

  private <K> CoalescingExpiringCache<K, String> cache(
      long maximumSize, CacheEvents events) {
    return CaffeineCoalescingExpiringCache.create(maximumSize, ignored -> HOLD, nanos::get, events);
  }

  private static CacheEvents recording(List<String> seen) {
    return new CacheEvents() {
      @Override
      public void hit(Duration served) {
        seen.add("hit");
      }

      @Override
      public void miss() {
        seen.add("miss");
      }

      @Override
      public void loadTime(Duration elapsed) {
        seen.add("load");
      }

      @Override
      public void loadFailed(Duration elapsed, RuntimeException error) {
        seen.add("failed");
      }
    };
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(error);
    }
  }

  /** A key whose hash puts every instance in the same map bin. */
  private record SameBin(int id) {
    @Override
    public int hashCode() {
      return 0;
    }
  }
}
