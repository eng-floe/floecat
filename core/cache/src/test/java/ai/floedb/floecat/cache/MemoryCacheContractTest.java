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

import ai.floedb.floecat.cache.CacheFixtures.Versioned;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Observable behaviour every {@link MemoryCache} implementation must preserve, especially around an
 * in-flight load. The tests intentionally do not prescribe how an implementation synchronizes.
 */
abstract class MemoryCacheContractTest {

  /** The implementation under test, with a budget large enough that nothing evicts. */
  protected abstract MemoryCache<String, Versioned> cache();

  /** A loader that reports when it has read and then holds until released. */
  private static final class HeldLoad {
    private final CountDownLatch reading = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    Versioned load(Versioned value) {
      reading.countDown();
      await(release);
      return value;
    }
  }

  @Test
  void evictAppliesToAKeyThatIsStillLoading() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();

    var reader =
        CompletableFuture.supplyAsync(() -> cache.get("k", k -> held.load(new Versioned("v1", 1))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    // evict reaches the map through a compute, so it contends with the load rather than missing
    // it. It has to be released by someone else, and that blocking IS the property.
    //
    // A dedicated thread, not the common pool: the parked loader already holds one of its threads,
    // and on a two-CPU runner the releaser would not be scheduled until the loader's own timeout
    // fired. The test would still pass, ten seconds later.
    Thread releaser =
        new Thread(
            () -> {
              sleep();
              held.release.countDown();
            });
    releaser.start();
    cache.evict("k");
    reader.get(10, TimeUnit.SECONDS);
    join(releaser);

    assertThat(cache.peek("k")).isEmpty();
  }

  @Test
  void aSweepBlindToAnInFlightLoadStillLeavesTheKeyAbsent() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();

    var reader =
        CompletableFuture.supplyAsync(() -> cache.get("k", k -> held.load(new Versioned("v1", 1))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    // Partition eviction iterates the key set, which this key is not in yet, so the sweep passes
    // it by.
    cache.evictPartition(key -> true);

    held.release.countDown();
    assertThat(reader.get(10, TimeUnit.SECONDS)).isEqualTo(new Versioned("v1", 1));

    // The sweep never reached the key, and the load still leaves it absent: the sweep moved every
    // fence, so the load undoes its own install.
    assertThat(cache.peek("k")).isEmpty();
  }

  @Test
  void aLoadStartingDuringASweepStillLeavesTheKeyAbsent() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    cache.get("held", key -> new Versioned("held", 1));
    var sweeping = new CountDownLatch(1);
    var releaseSweep = new CountDownLatch(1);

    var sweep =
        CompletableFuture.runAsync(
            () ->
                cache.evictPartition(
                    key -> {
                      if (key.equals("held")) {
                        sweeping.countDown();
                        await(releaseSweep);
                      }
                      return true;
                    }));
    assertThat(sweeping.await(10, TimeUnit.SECONDS)).isTrue();

    HeldLoad held = new HeldLoad();
    var reader =
        CompletableFuture.supplyAsync(
            () -> cache.get("k", key -> held.load(new Versioned("before-sweep", 1))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    releaseSweep.countDown();
    sweep.get(10, TimeUnit.SECONDS);
    held.release.countDown();
    assertThat(reader.get(10, TimeUnit.SECONDS)).isEqualTo(new Versioned("before-sweep", 1));

    assertThat(cache.peek("k")).isEmpty();
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

  private static void join(Thread thread) {
    try {
      thread.join(TimeUnit.SECONDS.toMillis(10));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while joining test thread", e);
    }
    assertThat(thread.isAlive()).as("test thread completed").isFalse();
  }

  private static void sleep() {
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted during test coordination", e);
    }
  }

  @Test
  void aBulkLoadDoesNotOverwriteAValueLoadedConcurrently() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();

    var reader =
        CompletableFuture.supplyAsync(() -> cache.get("k", k -> held.load(new Versioned("v1", 1))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    var bulkStarted = new CountDownLatch(1);
    Thread releaser =
        new Thread(
            () -> {
              await(bulkStarted);
              sleep();
              held.release.countDown();
            });
    releaser.start();
    assertThat(
            cache.getAll(
                List.of("k"),
                missing -> {
                  bulkStarted.countDown();
                  return Map.of("k", new Versioned("stale", 1));
                }))
        .containsEntry("k", new Versioned("v1", 1));
    reader.get(10, TimeUnit.SECONDS);
    join(releaser);

    assertThat(cache.peek("k")).contains(new Versioned("v1", 1));
  }

  @Test
  void aSecondCallerDuringALoadIsServedByItRatherThanLoadingAgain() throws Exception {
    // Single-flight, which the contract calls a correctness property: a hot key on a cold cache
    // must not send every in-flight request to the store. The second caller is admitted only once
    // the first has reported it is reading, so it demonstrably arrives during the load.
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();
    var loads = new java.util.concurrent.atomic.AtomicInteger();

    var leader =
        CompletableFuture.supplyAsync(
            () ->
                cache.get(
                    "k",
                    k -> {
                      loads.incrementAndGet();
                      return held.load(new Versioned("v1", 1));
                    }));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    // A dedicated thread, not the common pool: the parked leader holds one of its workers, and
    // CountDownLatch.await is not a ManagedBlocker, so on a two-worker pool the follower would sit
    // queued until the leader finished -- arriving after the load and proving nothing.
    var attempting = new CountDownLatch(1);
    var duplicateLoad = new CountDownLatch(1);
    var served = new CompletableFuture<Versioned>();
    Thread follower =
        new Thread(
            () -> {
              attempting.countDown();
              try {
                served.complete(
                    cache.get(
                        "k",
                        k -> {
                          loads.incrementAndGet();
                          duplicateLoad.countDown();
                          return new Versioned("never", 9);
                        }));
              } catch (Throwable failure) {
                served.completeExceptionally(failure);
              }
            });
    follower.start();
    assertThat(attempting.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(duplicateLoad.await(250, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(served).isNotCompleted();

    held.release.countDown();
    leader.get(10, TimeUnit.SECONDS);
    assertThat(served.get(10, TimeUnit.SECONDS)).isEqualTo(new Versioned("v1", 1));
    join(follower);

    assertThat(loads).hasValue(1);
  }

  @Test
  void putPublishesToAColdKey() {
    MemoryCache<String, Versioned> cache = cache();

    cache.put("k", new Versioned("published", 1));

    assertThat(cache.get("k", ignored -> new Versioned("never", 2)))
        .isEqualTo(new Versioned("published", 1));
  }

  @Test
  void putWinsAgainstAnInFlightLoad() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();
    var reader =
        CompletableFuture.supplyAsync(
            () -> cache.get("k", key -> held.load(new Versioned("loaded", 1))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    var published = CompletableFuture.runAsync(() -> cache.put("k", new Versioned("published", 2)));
    held.release.countDown();
    reader.get(10, TimeUnit.SECONDS);
    published.get(10, TimeUnit.SECONDS);

    assertThat(cache.peek("k")).contains(new Versioned("published", 2));
  }

  @Test
  void putWinsEvenWhenItPublishesTheSameObjectAnInFlightLoadReturns() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();
    Versioned shared = new Versioned("shared", 1);
    var reader = CompletableFuture.supplyAsync(() -> cache.get("k", key -> held.load(shared)));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    var published = CompletableFuture.runAsync(() -> cache.put("k", shared));
    held.release.countDown();
    reader.get(10, TimeUnit.SECONDS);
    published.get(10, TimeUnit.SECONDS);

    assertThat(cache.peek("k")).contains(shared);
  }

  @Test
  void getAllLoadsDistinctMissesInOneCallAndLeavesAbsenceUncached() {
    MemoryCache<String, Versioned> cache = cache();
    cache.put("hit", new Versioned("cached", 1));

    Map<String, Versioned> values =
        cache.getAll(
            List.of("hit", "loaded", "absent", "loaded"),
            missing -> {
              assertThat(missing).containsExactlyInAnyOrder("loaded", "absent");
              return Map.of("loaded", new Versioned("from-loader", 1));
            });

    assertThat(values)
        .containsOnly(
            Map.entry("hit", new Versioned("cached", 1)),
            Map.entry("loaded", new Versioned("from-loader", 1)));
    assertThat(cache.peek("absent")).isEmpty();
    assertThat(cache.getAll(Set.of(), ignored -> Map.of())).isEmpty();
  }

  @Test
  void anEvictionRacingABulkLoadLeavesTheLoadedKeyAbsent() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();

    var reader =
        CompletableFuture.supplyAsync(
            () ->
                cache.getAll(
                    List.of("k"), ignored -> Map.of("k", held.load(new Versioned("loaded", 1)))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    cache.evict("k");
    held.release.countDown();

    assertThat(reader.get(10, TimeUnit.SECONDS)).containsEntry("k", new Versioned("loaded", 1));
    assertThat(cache.peek("k")).isEmpty();
  }

  @Test
  void putWinsAgainstAnInFlightBulkLoad() throws Exception {
    MemoryCache<String, Versioned> cache = cache();
    HeldLoad held = new HeldLoad();

    var reader =
        CompletableFuture.supplyAsync(
            () ->
                cache.getAll(
                    List.of("k"), ignored -> Map.of("k", held.load(new Versioned("loaded", 1)))));
    assertThat(held.reading.await(10, TimeUnit.SECONDS)).isTrue();

    cache.put("k", new Versioned("published", 2));
    held.release.countDown();

    assertThat(reader.get(10, TimeUnit.SECONDS)).containsEntry("k", new Versioned("published", 2));
    assertThat(cache.peek("k")).contains(new Versioned("published", 2));
  }
}
