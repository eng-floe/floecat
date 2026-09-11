/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CaffeineMemoryCacheTest {

  @Test
  void reportsHitsMissesAndLoadTime() {
    var events = new CacheFixtures.RecordingEvents();
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    cache.get("k", ignored -> new CacheFixtures.Versioned("v", 1));
    cache.get("k", ignored -> new CacheFixtures.Versioned("wrong", 2));
    assertThat(events.misses).isEqualTo(1);
    assertThat(events.hits).isEqualTo(1);
    assertThat(events.loadTimes).hasSize(1);
  }

  @Test
  void telemetryCannotTurnAReadIntoFailure() {
    CacheEvents events =
        new CacheEvents() {
          @Override
          public void miss() {
            throw new IllegalStateException("telemetry unavailable");
          }

          @Override
          public void loadTime(Duration ignored) {
            throw new IllegalStateException("telemetry unavailable");
          }
        };
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    assertThat(cache.get("k", ignored -> new CacheFixtures.Versioned("v", 1)))
        .isEqualTo(new CacheFixtures.Versioned("v", 1));
  }

  @Test
  void budgetsIncludeEntryMachineryAndStayBounded() {
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.TIGHT_BUDGET);
    for (int i = 0; i < 500; i++) {
      int n = i;
      cache.get("key-" + n, ignored -> new CacheFixtures.Versioned("value-" + n, n));
    }
    assertThat(cache.bytes()).isLessThanOrEqualTo(CacheFixtures.TIGHT_BUDGET);
    assertThat(cache.entryCount()).isLessThan(500);
  }

  @Test
  void oversizedValuesAreRejectedByTheWeigher() {
    record Unweighable(long bytes) {}
    var events = new CacheFixtures.RecordingEvents();
    MemoryCache<String, Unweighable> cache = CacheFixtures.cacheForAnyValue(events);
    assertThatThrownBy(() -> cache.get("k", ignored -> new Unweighable(1)))
        .isInstanceOf(UnweighableValueException.class);
    assertThat(events.failures).isZero();
  }

  @Test
  void aCacheCanReserveItsWholeBudget() {
    var cache = CacheFixtures.<CacheFixtures.Versioned>cacheForAnyValue(CacheEvents.none());
    cache.get("k", ignored -> new CacheFixtures.Versioned("v", 1));
    cache.maximumBytes(0L);
    assertThat(cache.bytes()).isZero();
    assertThat(cache.entryCount()).isZero();
    assertThatThrownBy(() -> cache.maximumBytes(-1L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void loadFailureIsReportedAndPropagated() {
    var events = new CacheFixtures.RecordingEvents();
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    assertThatThrownBy(
            () ->
                cache.get(
                    "k",
                    ignored -> {
                      throw new IllegalStateException("bad");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(events.failures).isEqualTo(1);
  }

  @Test
  void warmBulkReadsAreVisibleAsHits() {
    var events = new CacheFixtures.RecordingEvents();
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    cache.get("a", ignored -> new CacheFixtures.Versioned("A", 1));
    cache.get("b", ignored -> new CacheFixtures.Versioned("B", 1));
    events.reset();

    assertThat(cache.getAll(java.util.List.of("a", "b"), ignored -> java.util.Map.of()))
        .containsOnlyKeys("a", "b");
    assertThat(events.hits).isEqualTo(2);
    assertThat(events.misses).isZero();
  }

  @Test
  void bulkLoaderFailureIsReportedOnce() {
    var events = new CacheFixtures.RecordingEvents();
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET, events);
    assertThatThrownBy(
            () ->
                cache.getAll(
                    java.util.List.of("a", "b"),
                    ignored -> {
                      throw new IllegalStateException("bulk failed");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(events.failures).isEqualTo(1);
  }

  @Test
  void emptyBulkReadDoesNotCallLoader() {
    AtomicInteger calls = new AtomicInteger();
    MemoryCache<String, CacheFixtures.Versioned> cache =
        CacheFixtures.cache(CacheFixtures.AMPLE_BUDGET);
    assertThat(
            cache.getAll(
                java.util.List.of(),
                ignored -> {
                  calls.incrementAndGet();
                  return java.util.Map.of();
                }))
        .isEmpty();
    assertThat(calls).hasValue(0);
  }
}
