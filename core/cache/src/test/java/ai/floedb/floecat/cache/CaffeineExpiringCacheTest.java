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
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CaffeineExpiringCacheTest {

  private static final Duration HOLD = Duration.ofMinutes(10);

  private final AtomicLong nanos = new AtomicLong();
  private final ExecutorService pool = Executors.newFixedThreadPool(4);

  @AfterEach
  void shutDown() {
    pool.shutdownNow();
  }

  @Test
  void loadsOnceAndServesTheHeldValue() {
    ExpiringCache<String, String> cache = cache(value -> HOLD, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();

    assertThat(cache.get("key", key -> "v" + loads.incrementAndGet())).isEqualTo("v1");
    assertThat(cache.get("key", key -> "v" + loads.incrementAndGet())).isEqualTo("v1");
    assertThat(loads).hasValue(1);
  }

  @Test
  void aSlowLoadNeverBlocksAnotherKey() throws Exception {
    ExpiringCache<SameBin, String> cache = cache(value -> HOLD, CacheEvents.none());
    BlockingLoad slow = new BlockingLoad(cache, new SameBin(0));
    try {
      // Every key shares the slow key's map bin, so a load run under the map's lock would stall
      // them all.
      for (int id = 1; id <= 8; id++) {
        SameBin key = new SameBin(id);
        assertThat(
                pool.submit(() -> cache.get(key, ignored -> "v" + key.id()))
                    .get(5, TimeUnit.SECONDS))
            .isEqualTo("v" + id);
      }
    } finally {
      slow.release();
    }
    assertThat(slow.result()).isEqualTo("slow");
  }

  @Test
  void aFailedLoadIsNotHeld() {
    ExpiringCache<String, String> cache = cache(value -> HOLD, CacheEvents.none());
    IllegalStateException failure = new IllegalStateException("upstream down");

    assertThatThrownBy(
            () ->
                cache.get(
                    "key",
                    key -> {
                      throw failure;
                    }))
        .isSameAs(failure);
    assertThat(cache.get("key", key -> "recovered")).isEqualTo("recovered");
  }

  @Test
  void aNullLoadIsNotHeld() {
    ExpiringCache<String, String> cache = cache(value -> HOLD, CacheEvents.none());

    assertThat(cache.get("key", key -> null)).isNull();
    assertThat(cache.get("key", key -> "loaded")).isEqualTo("loaded");
  }

  @Test
  void eachValueIsHeldForItsOwnDurationAndReadsDoNotExtendIt() {
    ExpiringCache<String, Duration> cache = cache(value -> value, CacheEvents.none());
    AtomicInteger loads = new AtomicInteger();
    cache.get("short", key -> Duration.ofMinutes(1));
    cache.get("long", key -> Duration.ofMinutes(10));
    cache.get(
        "none",
        key -> {
          loads.incrementAndGet();
          return Duration.ZERO;
        });

    cache.get(
        "none",
        key -> {
          loads.incrementAndGet();
          return Duration.ZERO;
        });
    assertThat(loads).hasValue(2);

    nanos.addAndGet(Duration.ofMinutes(2).toNanos());
    assertThat(cache.get("short", key -> Duration.ofMinutes(99))).isEqualTo(Duration.ofMinutes(99));
    assertThat(cache.get("long", key -> Duration.ofMinutes(99))).isEqualTo(Duration.ofMinutes(10));
    nanos.addAndGet(Duration.ofMinutes(9).toNanos());
    assertThat(cache.get("long", key -> Duration.ofMinutes(99))).isEqualTo(Duration.ofMinutes(99));
  }

  @Test
  void reportsHitsMissesAndFailures() {
    List<String> seen = new ArrayList<>();
    ExpiringCache<String, String> cache = cache(value -> HOLD, recording(seen, false));

    cache.get("key", key -> "v");
    assertThat(seen).containsExactly("miss", "load");
    cache.get("key", key -> "v");
    assertThat(seen).containsExactly("miss", "load", "hit");
    assertThatThrownBy(
        () ->
            cache.get(
                "other",
                key -> {
                  throw new IllegalStateException("boom");
                }));

    assertThat(seen).containsExactly("miss", "load", "hit", "miss", "failed");
  }

  @Test
  void throwingTelemetryDoesNotChangeARead() {
    ExpiringCache<String, String> cache = cache(value -> HOLD, recording(new ArrayList<>(), true));

    assertThat(cache.get("key", key -> "loaded")).isEqualTo("loaded");
    assertThat(cache.get("key", key -> "unused")).isEqualTo("loaded");
    IllegalStateException failure = new IllegalStateException("upstream down");
    assertThatThrownBy(
            () ->
                cache.get(
                    "other",
                    key -> {
                      throw failure;
                    }))
        .isSameAs(failure);
  }

  private <K, V> ExpiringCache<K, V> cache(
      Function<? super V, Duration> holdFor, CacheEvents events) {
    return CaffeineExpiringCache.create(1000, holdFor, nanos::get, events);
  }

  private static CacheEvents recording(List<String> seen, boolean throwing) {
    return new CacheEvents() {
      @Override
      public void hit(Duration served) {
        record("hit");
      }

      @Override
      public void miss() {
        record("miss");
      }

      @Override
      public void loadTime(Duration elapsed) {
        record("load");
      }

      @Override
      public void loadFailed(Duration elapsed, RuntimeException error) {
        record("failed");
      }

      private void record(String event) {
        seen.add(event);
        if (throwing) {
          throw new IllegalStateException("telemetry down");
        }
      }
    };
  }

  /** A caller on the pool whose load returns "slow" once released. */
  private final class BlockingLoad {
    private final CountDownLatch release = new CountDownLatch(1);
    private final Future<String> result;

    BlockingLoad(ExpiringCache<SameBin, String> cache, SameBin key) throws InterruptedException {
      CountDownLatch loading = new CountDownLatch(1);
      result =
          pool.submit(
              () ->
                  cache.get(
                      key,
                      ignored -> {
                        loading.countDown();
                        await(release);
                        return "slow";
                      }));
      loading.await();
    }

    void release() {
      release.countDown();
    }

    String result() throws Exception {
      return result.get(5, TimeUnit.SECONDS);
    }
  }

  /** A key whose hash puts every instance in the same map bin. */
  private record SameBin(int id) {
    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
