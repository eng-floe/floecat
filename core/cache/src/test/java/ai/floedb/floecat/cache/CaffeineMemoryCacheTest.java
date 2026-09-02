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

import ai.floedb.floecat.cache.CacheFixtures.RecordingEvents;
import ai.floedb.floecat.cache.CacheFixtures.Versioned;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CaffeineMemoryCacheTest {

  private static MemoryCache<String, Versioned> cache() {
    return CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET);
  }

  @Test
  void reportsHitsMissesAndLoadTime() {
    // The three a misbehaving cache is debugged from: whether it is used, how often it loads, and
    // whether loads are failing.
    var events = new RecordingEvents();
    var cache = CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);

    cache.get("k", key -> new Versioned("v", 1L));
    cache.get("k", key -> new Versioned("v", 1L));

    assertThat(events.misses).isEqualTo(1);
    assertThat(events.hits).isEqualTo(1);

    assertThatThrownBy(
            () ->
                cache.get(
                    "boom",
                    key -> {
                      throw new IllegalStateException("load failed");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(events.failures).isEqualTo(1);
  }

  @Test
  void bytesCountEntryMachineryNotJustPayload() {
    // A cache full of tiny values must reach its ceiling on machinery rather than reading as
    // nearly empty, which is what makes a byte budget also an entry budget.
    MemoryCache<String, Versioned> cache = cache();
    cache.get("k", key -> new Versioned("", 1L));

    assertThat(cache.bytes()).isGreaterThanOrEqualTo(125L);
  }

  @Test
  void budgetsAboveTwoGibibytesStillChargeAnEntriesFullWeight() {
    long budget = 4L * 1024 * 1024 * 1024;
    long retained = 3L * 1024 * 1024 * 1024;
    record Huge(long bytes) implements WeightedValue {
      @Override
      public long estimatedWeightBytes() {
        return bytes;
      }
    }
    MemoryCache<String, Huge> cache =
        new CaffeineMemoryCache<>(
            CacheFamily.POINTER, budget, CacheFixtures::keyWeight, CacheEvents.none());

    cache.get("huge", ignored -> new Huge(retained));

    assertThat(cache.bytes()).isGreaterThanOrEqualTo(retained).isLessThanOrEqualTo(budget);
  }

  @Test
  void anEntryLargerThanAnExactlyIntegerMaxBudgetCannotHideBehindTheWeightLimit() {
    long budget = Integer.MAX_VALUE;
    long retained = budget + 1L;
    record Huge(long bytes) implements WeightedValue {
      @Override
      public long estimatedWeightBytes() {
        return bytes;
      }
    }
    MemoryCache<String, Huge> cache =
        new CaffeineMemoryCache<>(
            CacheFamily.POINTER, budget, CacheFixtures::keyWeight, CacheEvents.none());

    assertThat(cache.get("too-large", ignored -> new Huge(retained))).isEqualTo(new Huge(retained));
    assertThat(cache.bytes()).isLessThanOrEqualTo(budget);
    assertThat(cache.peek("too-large")).isEmpty();
  }

  @Test
  void peekReadsWithoutLoadingOrCounting() {
    // peek must not move the hit rate: it is not a question anyone asked the cache.
    var events = new RecordingEvents();
    var cache = CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);

    assertThat(cache.peek("absent")).isEmpty();
    assertThat(cache.entryCount()).isZero();

    cache.get("k", key -> new Versioned("v", 1L));
    events.reset();

    assertThat(cache.peek("k")).map(Versioned::value).hasValue("v");
    assertThat(events.hits).isZero();
    assertThat(events.misses).isZero();
  }

  @Test
  void reportsEvictionsWhenTheBudgetIsExceeded() {
    // The eviction event, and the weight that went with it.
    var events = new RecordingEvents();
    var small = CacheFixtures.cache(CacheFixtures.TIGHT_BUDGET, events);

    for (int i = 0; i < 500; i++) {
      int n = i;
      small.get("key-" + n, key -> new Versioned("value-" + n, n));
    }
    small.bytes(); // forces the maintenance that runs eviction

    assertThat(events.evictions).isPositive();
    assertThat(events.evictedBytes).isPositive();
  }

  @Test
  void aBulkReadReportsPerKeyHitsAndMissesAndOneLoadTime() {
    var events = new RecordingEvents();
    var cache = CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    cache.put("hit", new Versioned("held", 1L));

    assertThat(
            cache.getAll(
                List.of("hit", "loaded", "absent", "loaded"),
                missing -> Map.of("loaded", new Versioned("value", 1L))))
        .containsOnlyKeys("hit", "loaded");

    assertThat(events.hits).isEqualTo(1);
    assertThat(events.misses).isEqualTo(2);
    assertThat(events.loadTimes).hasSize(1).allSatisfy(d -> assertThat(d).isPositive());
  }

  @Test
  void aValueTheWeigherRefusesIsNotReportedAsAFailedLoad() {
    // The weigher runs inside the compute, so its refusal comes back out of get. Counting it as a
    // load failure would point an operator at a store that answered correctly; the defect is in
    // the value type. Deleting UnweighableValueException in favour of IllegalArgumentException
    // makes this red.
    record Unweighable(long a) {}
    var events = new RecordingEvents();
    MemoryCache<String, Unweighable> cache = CacheFixtures.cacheForAnyValue(events);

    assertThatThrownBy(() -> cache.get("k", key -> new Unweighable(1)))
        .isInstanceOf(UnweighableValueException.class);

    assertThat(events.failures).isZero();
    assertThat(events.misses).isEqualTo(1);
  }

  @Test
  void hitsCarryAMeasuredDurationRatherThanAConstant() {
    // Summed over many hits so this cannot hinge on clock granularity: a constant zero, which is
    // what a hit would report if the duration were not measured, sums to zero. The duration is
    // what separates a warm-map hit from a caller that waited on someone else's load.
    var events = new RecordingEvents();
    MemoryCache<String, Versioned> cache = CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    cache.get("k", key -> new Versioned("v", 1L));
    events.reset();

    for (int i = 0; i < 1_000; i++) {
      cache.get("k", key -> new Versioned("never", 9L));
    }

    assertThat(events.hits).isEqualTo(1_000);
    assertThat(events.hitTimes.stream().mapToLong(Duration::toNanos).sum()).isPositive();
  }

  @Test
  void everyDelegatingDoorIsActuallyWired() {
    // Single-flight, size bounding and the two drop doors are Caffeine's. What is ours is that
    // they are connected at all, which one test covers: each of these was green with the method
    // body deleted before it existed.
    var loads = new AtomicInteger();
    MemoryCache<String, Versioned> cache = cache();

    cache.get("acct/1/a", key -> new Versioned("a", loads.incrementAndGet()));
    cache.get("acct/1/a", key -> new Versioned("a", loads.incrementAndGet()));
    assertThat(loads).hasValue(1);

    cache.get("acct/1/b", key -> new Versioned("b", 1L));
    cache.get("acct/2/a", key -> new Versioned("c", 1L));

    cache.evict("acct/1/a");
    assertThat(cache.peek("acct/1/a")).isEmpty();

    cache.evictPartition(key -> key.startsWith("acct/1/"));
    assertThat(cache.peek("acct/1/b")).isEmpty();
    assertThat(cache.peek("acct/2/a")).map(Versioned::value).hasValue("c");

    MemoryCache<String, Versioned> tight = CacheFixtures.cache(CacheFixtures.TIGHT_BUDGET);
    for (int i = 0; i < 500; i++) {
      int n = i;
      tight.get("key-" + n, key -> new Versioned("value-" + n, n));
    }
    assertThat(tight.bytes()).isLessThanOrEqualTo(CacheFixtures.TIGHT_BUDGET);
    assertThat(tight.entryCount()).isLessThan(500L);
  }

  @Test
  void putDoesNotAcceptANull() {
    MemoryCache<String, Versioned> cache = CacheFixtures.cacheForAnyValue(CacheEvents.none());
    cache.get("k", key -> new Versioned("v1", 1L));

    assertThatThrownBy(() -> cache.put("absent", null)).isInstanceOf(NullPointerException.class);

    assertThat(cache.peek("k")).map(Versioned::value).hasValue("v1");
  }

  @Test
  void aBulkLoaderCannotReturnAnUnrequestedKeyOrANullValue() {
    MemoryCache<String, Versioned> cache = cache();

    assertThatThrownBy(
            () ->
                cache.getAll(
                    List.of("requested"), ignored -> Map.of("other", new Versioned("value", 1L))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unrequested key");
    assertThatThrownBy(
            () ->
                cache.getAll(
                    List.of("requested"),
                    ignored -> java.util.Collections.singletonMap("requested", null)))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void aLoadThatCachedNothingReportsNoRefusal() throws Exception {
    // A loader returning null caches nothing, so a fence that moved has nothing to undo. Counting
    // a refusal here would inflate the one signal meaning "this cache stopped warming", on a path
    // that never fills.
    //
    // The write lands from another thread while the load is held. It cannot come from inside the
    // loader: Caffeine forbids a mapping function from touching other keys, and may throw if it
    // detects the recursion.
    String loaded = CacheFixtures.STRIPE_SHARER_A;
    String written = CacheFixtures.STRIPE_SHARER_B;
    var events = new RecordingEvents();
    MemoryCache<String, Versioned> cache = CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    cache.get(written, key -> new Versioned("v1", 1L));
    events.reset();

    var reading = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var loader =
        CompletableFuture.supplyAsync(
            () ->
                cache.get(
                    loaded,
                    key -> {
                      reading.countDown();
                      try {
                        release.await(10, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return null;
                    }));
    assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();

    // These two keys share a fence stripe, so this moves the fence the held load is watching.
    cache.evict(written);
    release.countDown();

    assertThat(loader.get(10, TimeUnit.SECONDS)).isNull();
    assertThat(events.loadsDiscarded).isZero();
    assertThat(events.misses).isEqualTo(1);
  }

  @Test
  void aProtobufReportingANegativeSizeIsRefusedRatherThanLeftToCaffeine() {
    // Protobuf reports its size as an int, so a message above 2 GB reports it negative. Refused
    // here it reads as a value defect; left alone the entry weighs negative and Caffeine's weigher
    // throws a bare IllegalArgumentException, which surfaces as a failed store read.
    assertThatThrownBy(() -> CacheWeights.entry(new CacheFixtures.OversizedMessage(), 0L))
        .isInstanceOf(UnweighableValueException.class)
        .hasMessageContaining("negative serialized size");
  }

  @Test
  void theSharedStripeFixtureStillCollides() {
    // Two tests land a write "during" a load by writing a key that shares the loaded key's fence
    // stripe. Asserted here so that changing stripeFor or FENCE_STRIPES fails as a stale fixture
    // rather than as the fence itself being broken.
    var cache = CacheFixtures.cacheForAnyValue(CacheEvents.none());

    assertThat(cache.stripeFor(CacheFixtures.STRIPE_SHARER_A))
        .as("CacheFixtures.STRIPE_SHARER_A and STRIPE_SHARER_B must share a fence stripe")
        .isEqualTo(cache.stripeFor(CacheFixtures.STRIPE_SHARER_B));
  }

  @Test
  void aValueTheWeigherRefusesDuringABulkLoadIsNotReportedAsAStoreFailure() {
    record Unweighable(long a) {}
    var events = new RecordingEvents();
    MemoryCache<String, Object> cache = CacheFixtures.cacheForAnyValue(events);

    assertThatThrownBy(() -> cache.getAll(List.of("k"), ignored -> Map.of("k", new Unweighable(1))))
        .isInstanceOf(UnweighableValueException.class);

    assertThat(cache.peek("k")).isEmpty();
    assertThat(events.failures).isZero();
  }
}
