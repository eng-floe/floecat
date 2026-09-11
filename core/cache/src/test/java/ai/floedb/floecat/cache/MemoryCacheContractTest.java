/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.cache.CacheFixtures.Versioned;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Behaviour every immutable memory-cache implementation owes. */
abstract class MemoryCacheContractTest {

  protected abstract MemoryCache<String, Versioned> cache();

  @Test
  void nativeCaffeineLoadingMergesConcurrentSameKeyReads() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch followerEntered = new CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();
    var leader =
        CompletableFuture.supplyAsync(
            () ->
                cache.get(
                    "k",
                    ignored -> {
                      loads.incrementAndGet();
                      started.countDown();
                      await(release);
                      return new Versioned("value", 1);
                    }));
    assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    var follower =
        CompletableFuture.supplyAsync(
            () -> {
              followerEntered.countDown();
              var result = cache.get("k", ignored -> new Versioned("wrong", 2));
              return result;
            });
    // The leader keeps the native Caffeine load open. The follower must therefore still be
    // waiting here; no timing assumption is needed to establish that it joined the same load.
    assertThat(started.getCount()).isZero();
    assertThat(followerEntered.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(follower).isNotCompleted();
    release.countDown();
    assertThat(leader.get(10, TimeUnit.SECONDS)).isEqualTo(new Versioned("value", 1));
    assertThat(follower.get(10, TimeUnit.SECONDS)).isEqualTo(new Versioned("value", 1));
    assertThat(loads).hasValue(1);
  }

  @Test
  void bulkLoadsDistinctMissesAndLeavesAbsenceUncached() {
    MemoryCache<String, Versioned> cache = cache();
    AtomicInteger calls = new AtomicInteger();
    Map<String, Versioned> values =
        cache.getAll(
            List.of("a", "b", "missing", "a"),
            keys -> {
              calls.incrementAndGet();
              assertThat(keys).containsExactlyInAnyOrder("a", "b", "missing");
              return Map.of("a", new Versioned("A", 1), "b", new Versioned("B", 1));
            });
    assertThat(values).containsOnlyKeys("a", "b");
    assertThat(cache.peek("missing")).isEmpty();
    assertThat(calls).hasValue(1);
  }

  @Test
  void malformedBulkResultsAreRejected() {
    MemoryCache<String, Versioned> cache = cache();
    assertThatThrownBy(
            () ->
                cache.getAll(
                    Set.of("requested"), ignored -> Map.of("other", new Versioned("bad", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unrequested key");
  }

  @Test
  void evictionAndPartitionEvictionDropResidentValues() {
    MemoryCache<String, Versioned> cache = cache();
    cache.get("account/1/a", ignored -> new Versioned("a", 1));
    cache.get("account/1/b", ignored -> new Versioned("b", 1));
    cache.get("account/2/a", ignored -> new Versioned("c", 1));
    cache.evict("account/1/a");
    cache.evictPartition(key -> key.startsWith("account/1/"));
    assertThat(cache.peek("account/1/a")).isEmpty();
    assertThat(cache.peek("account/1/b")).isEmpty();
    assertThat(cache.peek("account/2/a")).isPresent();
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for test coordination");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for test coordination", e);
    }
  }
}
