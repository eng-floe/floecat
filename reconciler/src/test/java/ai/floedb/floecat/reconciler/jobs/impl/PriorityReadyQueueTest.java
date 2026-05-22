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

package ai.floedb.floecat.reconciler.jobs.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PriorityReadyQueueTest {

  // ---- empty queue behaviour -----------------------------------------------

  @Test
  void pollHighestOnEmptyQueueReturnsNull() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      assertNull(q.pollHighest(cls), "expected null for " + cls);
    }
  }

  @Test
  void sizeByClassIsZeroOnNewQueue() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      assertEquals(0L, q.sizeByClass(cls));
    }
    assertEquals(0L, q.totalSize());
  }

  // ---- basic enqueue / poll ------------------------------------------------

  @Test
  void singleEnqueueIsReturnedByPoll() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("job-1", StatsPriorityClass.P3_BACKGROUND, 0L);
    assertEquals("job-1", q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
    assertNull(q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
  }

  @Test
  void pollDoesNotCrossClassBoundary() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("p3-job", StatsPriorityClass.P3_BACKGROUND, 100L);
    // Polling P0 should not return the P3 job
    assertNull(q.pollHighest(StatsPriorityClass.P0_SYNC));
    assertNull(q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertNull(q.pollHighest(StatsPriorityClass.P2_REPAIR));
    assertEquals("p3-job", q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
  }

  // ---- class priority ordering (P0 before P3) ------------------------------

  @Test
  void p0JobDispatchedBeforeP3Regardless_ofInsertionOrder() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("p3-job", StatsPriorityClass.P3_BACKGROUND, 999L); // high score, but low class
    q.enqueue("p0-job", StatsPriorityClass.P0_SYNC, 0L); // low score, but top class

    // Caller iterates P0→P1→P2→P3; first non-null wins.
    String first =
        firstNonNull(
            q.pollHighest(StatsPriorityClass.P0_SYNC),
            q.pollHighest(StatsPriorityClass.P1_FRESHNESS),
            q.pollHighest(StatsPriorityClass.P2_REPAIR),
            q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
    assertEquals("p0-job", first);
  }

  @Test
  void allFourClassesDispatchedInUrgencyOrder_whenOnlyOneJobEach() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("bg", StatsPriorityClass.P3_BACKGROUND, 0L);
    q.enqueue("repair", StatsPriorityClass.P2_REPAIR, 0L);
    q.enqueue("fresh", StatsPriorityClass.P1_FRESHNESS, 0L);
    q.enqueue("sync", StatsPriorityClass.P0_SYNC, 0L);

    List<String> order = new ArrayList<>();
    for (StatsPriorityClass cls : StatsPriorityClass.values()) { // P0→P3 by enum ordinal
      String j = q.pollHighest(cls);
      if (j != null) order.add(j);
    }
    assertEquals(List.of("sync", "fresh", "repair", "bg"), order);
  }

  // ---- score ordering within a class ---------------------------------------

  @Test
  void higherScoreDispatchedFirst_withinSameClass() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("low", StatsPriorityClass.P2_REPAIR, 10L);
    q.enqueue("high", StatsPriorityClass.P2_REPAIR, 100L);
    q.enqueue("medium", StatsPriorityClass.P2_REPAIR, 50L);

    assertEquals("high", q.pollHighest(StatsPriorityClass.P2_REPAIR));
    assertEquals("medium", q.pollHighest(StatsPriorityClass.P2_REPAIR));
    assertEquals("low", q.pollHighest(StatsPriorityClass.P2_REPAIR));
    assertNull(q.pollHighest(StatsPriorityClass.P2_REPAIR));
  }

  @Test
  void jobsWithEqualScoreDispatchedFifo() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("first", StatsPriorityClass.P1_FRESHNESS, 42L);
    q.enqueue("second", StatsPriorityClass.P1_FRESHNESS, 42L);
    q.enqueue("third", StatsPriorityClass.P1_FRESHNESS, 42L);

    assertEquals("first", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertEquals("second", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertEquals("third", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
  }

  @Test
  void negativeScoreIsTreatedAsZero() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("neg", StatsPriorityClass.P3_BACKGROUND, -100L);
    q.enqueue("zero", StatsPriorityClass.P3_BACKGROUND, 0L);
    // Both at score 0 → FIFO; "neg" was added first
    assertEquals("neg", q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
    assertEquals("zero", q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
  }

  // ---- requeue -------------------------------------------------------------

  @Test
  void requeuePutsJobBackIntoCorrectBucket() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("job-a", StatsPriorityClass.P1_FRESHNESS, 50L);
    q.enqueue("job-b", StatsPriorityClass.P1_FRESHNESS, 50L);

    String polled = q.pollHighest(StatsPriorityClass.P1_FRESHNESS);
    assertEquals("job-a", polled);

    // Requeue at the same score: goes to the tail of the score-50 deque
    q.requeue(polled, StatsPriorityClass.P1_FRESHNESS, 50L);

    assertEquals("job-b", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertEquals("job-a", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertNull(q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
  }

  @Test
  void removeJobRemovesEntriesAcrossAllBuckets() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("shared", StatsPriorityClass.P3_BACKGROUND, 10L);
    q.enqueue("shared", StatsPriorityClass.P2_REPAIR, 20L);
    q.enqueue("shared", StatsPriorityClass.P1_FRESHNESS, 30L);
    q.enqueue("other", StatsPriorityClass.P1_FRESHNESS, 40L);

    long removed = q.removeJob("shared");
    assertEquals(3L, removed);
    assertEquals("other", q.pollHighest(StatsPriorityClass.P1_FRESHNESS));
    assertNull(q.pollHighest(StatsPriorityClass.P2_REPAIR));
    assertNull(q.pollHighest(StatsPriorityClass.P3_BACKGROUND));
  }

  // ---- size tracking -------------------------------------------------------

  @Test
  void sizeByClassTracksEnqueueAndPoll() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    assertEquals(0L, q.sizeByClass(StatsPriorityClass.P0_SYNC));

    q.enqueue("a", StatsPriorityClass.P0_SYNC, 0L);
    assertEquals(1L, q.sizeByClass(StatsPriorityClass.P0_SYNC));
    assertEquals(1L, q.totalSize());

    q.enqueue("b", StatsPriorityClass.P0_SYNC, 0L);
    assertEquals(2L, q.sizeByClass(StatsPriorityClass.P0_SYNC));

    q.pollHighest(StatsPriorityClass.P0_SYNC);
    assertEquals(1L, q.sizeByClass(StatsPriorityClass.P0_SYNC));

    q.pollHighest(StatsPriorityClass.P0_SYNC);
    assertEquals(0L, q.sizeByClass(StatsPriorityClass.P0_SYNC));
    assertEquals(0L, q.totalSize());
  }

  @Test
  void sizeByAllClassesReturnsAllFour() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    q.enqueue("j0", StatsPriorityClass.P0_SYNC, 0L);
    q.enqueue("j1a", StatsPriorityClass.P1_FRESHNESS, 0L);
    q.enqueue("j1b", StatsPriorityClass.P1_FRESHNESS, 0L);

    Map<StatsPriorityClass, Long> sizes = q.sizeByAllClasses();
    assertEquals(4, sizes.size());
    assertEquals(1L, sizes.get(StatsPriorityClass.P0_SYNC));
    assertEquals(2L, sizes.get(StatsPriorityClass.P1_FRESHNESS));
    assertEquals(0L, sizes.get(StatsPriorityClass.P2_REPAIR));
    assertEquals(0L, sizes.get(StatsPriorityClass.P3_BACKGROUND));
  }

  @Test
  void sizeByAllClassesIsUnmodifiable() {
    PriorityReadyQueue q = new PriorityReadyQueue();
    Map<StatsPriorityClass, Long> sizes = q.sizeByAllClasses();
    assertThrows(
        UnsupportedOperationException.class, () -> sizes.put(StatsPriorityClass.P0_SYNC, 99L));
  }

  // ---- concurrent stress ---------------------------------------------------

  /**
   * Enqueues 1 000 jobs across all classes from N producer threads and polls them from M consumer
   * threads. Asserts:
   *
   * <ul>
   *   <li>Every enqueued job is polled exactly once (no loss, no duplication).
   *   <li>Total size returns to 0 after all polls.
   * </ul>
   */
  @Test
  void concurrentEnqueueAndPoll_noLossNoDuplication() throws InterruptedException {
    final int JOBS_PER_THREAD = 250;
    final int PRODUCERS = 4;
    final int CONSUMERS = 4;
    final int TOTAL_JOBS = JOBS_PER_THREAD * PRODUCERS;

    PriorityReadyQueue q = new PriorityReadyQueue();
    StatsPriorityClass[] classes = StatsPriorityClass.values();
    Set<String> polled = ConcurrentHashMap.newKeySet();
    AtomicInteger pollCount = new AtomicInteger(0);

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch producerDone = new CountDownLatch(PRODUCERS);
    CountDownLatch consumerDone = new CountDownLatch(CONSUMERS);

    ExecutorService pool = Executors.newFixedThreadPool(PRODUCERS + CONSUMERS);

    // Producers
    for (int p = 0; p < PRODUCERS; p++) {
      final int producerId = p;
      pool.submit(
          () -> {
            try {
              startLatch.await();
              for (int i = 0; i < JOBS_PER_THREAD; i++) {
                String jobId = "p" + producerId + "-j" + i;
                StatsPriorityClass cls = classes[(producerId + i) % classes.length];
                q.enqueue(jobId, cls, (long) i);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              producerDone.countDown();
            }
          });
    }

    // Consumers: poll until they've collectively drained TOTAL_JOBS
    for (int c = 0; c < CONSUMERS; c++) {
      final int consumerId = c;
      pool.submit(
          () -> {
            try {
              startLatch.await();
              while (pollCount.get() < TOTAL_JOBS) {
                for (StatsPriorityClass cls : classes) {
                  String job = q.pollHighest(cls);
                  if (job != null) {
                    polled.add(job);
                    pollCount.incrementAndGet();
                  }
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              consumerDone.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(producerDone.await(10, TimeUnit.SECONDS), "Producers did not finish in time");
    assertTrue(consumerDone.await(10, TimeUnit.SECONDS), "Consumers did not finish in time");
    pool.shutdown();

    assertEquals(TOTAL_JOBS, polled.size(), "Expected exactly " + TOTAL_JOBS + " distinct jobs");
    assertEquals(0L, q.totalSize(), "Queue should be empty after draining");
  }

  /**
   * Concurrent enqueuers targeting the same class+score bucket must not produce duplicate entries
   * in the deque (i.e. {@code computeIfAbsent} must not create two deques for the same key).
   */
  @Test
  void concurrentEnqueueSameScoreSameClass_noJobLost() throws InterruptedException {
    final int THREADS = 8;
    final int JOBS_PER_THREAD = 100;
    PriorityReadyQueue q = new PriorityReadyQueue();
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(THREADS);
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);

    for (int t = 0; t < THREADS; t++) {
      final int tid = t;
      pool.submit(
          () -> {
            try {
              latch.await();
              for (int i = 0; i < JOBS_PER_THREAD; i++) {
                q.enqueue("t" + tid + "-j" + i, StatsPriorityClass.P1_FRESHNESS, 77L);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }

    latch.countDown();
    assertTrue(done.await(10, TimeUnit.SECONDS));
    pool.shutdown();

    int expected = THREADS * JOBS_PER_THREAD;
    assertEquals(expected, q.sizeByClass(StatsPriorityClass.P1_FRESHNESS));

    Set<String> drained = ConcurrentHashMap.newKeySet();
    String job;
    while ((job = q.pollHighest(StatsPriorityClass.P1_FRESHNESS)) != null) {
      drained.add(job);
    }
    assertEquals(expected, drained.size(), "All jobs should be drained exactly once");
  }

  // ---- helpers -------------------------------------------------------------

  @SafeVarargs
  private static <T> T firstNonNull(T... values) {
    for (T v : values) {
      if (v != null) return v;
    }
    return null;
  }
}
