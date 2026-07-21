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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The engine's contract: immutable content is loaded once and served from memory thereafter;
 * absence and failures are never cached; concurrent misses collapse to one load.
 */
class ImmutableBlobCacheTest {

  private static ImmutableBlobCache cache() {
    return new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5));
  }

  @Test
  void aHitServesFromMemoryWithoutReloading() {
    var cache = cache();
    AtomicInteger loads = new AtomicInteger();
    var loader =
        (java.util.function.Function<String, Optional<StringValue>>)
            uri -> {
              loads.incrementAndGet();
              return Optional.of(StringValue.of("v"));
            };

    assertEquals("v", cache.get("s3://t/a.pb", loader).orElseThrow().getValue());
    assertEquals("v", cache.get("s3://t/a.pb", loader).orElseThrow().getValue());

    assertEquals(1, loads.get(), "second read must be a cache hit, not a reload");
  }

  @Test
  void absenceIsNeverCached() {
    var cache = cache();
    AtomicInteger loads = new AtomicInteger();

    assertTrue(
        cache
            .get(
                "s3://t/missing.pb",
                uri -> {
                  loads.incrementAndGet();
                  return Optional.<StringValue>empty();
                })
            .isEmpty());
    // The blob "appears" (e.g. finalize published it) — the next read must load it, not serve a
    // cached absence.
    assertEquals(
        "late",
        cache
            .get(
                "s3://t/missing.pb",
                uri -> {
                  loads.incrementAndGet();
                  return Optional.of(StringValue.of("late"));
                })
            .orElseThrow()
            .getValue());
    assertEquals(2, loads.get());
  }

  @Test
  void aLoaderFailureIsNotCached() {
    var cache = cache();
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            cache.get(
                "s3://t/flaky.pb",
                uri -> {
                  throw new IllegalStateException("transient");
                }));
    assertEquals(
        "ok",
        cache
            .<StringValue>get("s3://t/flaky.pb", uri -> Optional.of(StringValue.of("ok")))
            .orElseThrow()
            .getValue());
  }

  @Test
  void concurrentMissesCollapseToOneLoad() throws Exception {
    var cache = cache();
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    Runnable read =
        () -> {
          try {
            start.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          cache.get(
              "s3://t/hot.pb",
              uri -> {
                loads.incrementAndGet();
                try {
                  Thread.sleep(20); // hold the load open so the second reader piles up behind it
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return Optional.of(StringValue.of("hot"));
              });
        };
    Thread t1 = new Thread(read);
    Thread t2 = new Thread(read);
    t1.start();
    t2.start();
    start.countDown();
    t1.join();
    t2.join();

    assertEquals(1, loads.get(), "a cold burst on one URI must collapse to a single load");
  }

  @Test
  void disabledCachePassesEveryReadThrough() {
    var cache = new ImmutableBlobCache(false, 1024, Duration.ofMinutes(1));
    AtomicInteger loads = new AtomicInteger();
    var loader =
        (java.util.function.Function<String, Optional<StringValue>>)
            uri -> {
              loads.incrementAndGet();
              return Optional.of(StringValue.of("v"));
            };
    cache.get("s3://t/a.pb", loader);
    cache.get("s3://t/a.pb", loader);
    assertEquals(2, loads.get());
    assertTrue(cache.<StringValue>getAllPresent(List.of("s3://t/a.pb")).isEmpty());
  }

  @Test
  void batchProbeServesHitsAndAcceptsPutBack() {
    var cache = cache();
    cache.put("s3://t/1.pb", StringValue.of("one"));

    Map<String, StringValue> present = cache.getAllPresent(List.of("s3://t/1.pb", "s3://t/2.pb"));
    assertEquals(1, present.size());
    assertEquals("one", present.get("s3://t/1.pb").getValue());

    cache.put("s3://t/2.pb", StringValue.of("two"));
    assertEquals(
        "two",
        cache.<StringValue>getAllPresent(List.of("s3://t/2.pb")).get("s3://t/2.pb").getValue());
  }

  @Test
  void weightBoundEvictsInsteadOfGrowingUnbounded() {
    // Max weight ~2 entries of this size: inserting many must keep the cache bounded, not OOM.
    var cache = new ImmutableBlobCache(true, 3_000, Duration.ofMinutes(5));
    for (int i = 0; i < 100; i++) {
      cache.put("s3://t/blob-" + i + ".pb", StringValue.of("x".repeat(1000)));
    }
    cache.cleanUp(); // weight eviction is async; force pending maintenance before asserting
    Map<String, StringValue> present =
        cache.getAllPresent(
            java.util.stream.IntStream.range(0, 100)
                .mapToObj(i -> "s3://t/blob-" + i + ".pb")
                .toList());
    assertTrue(present.size() <= 3, "byte-weight bound must cap resident entries");
  }
}
