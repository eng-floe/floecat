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
package ai.floedb.floecat.service.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BoundedFanoutTest {

  @Test
  void resultsAreReturnedInInputOrder() {
    List<Integer> inputs = IntStream.range(0, 50).boxed().toList();
    // Reverse the completion order (later items sleep less) to prove ordering is by input, not
    // completion.
    List<Integer> out =
        BoundedFanout.mapOrdered(
            inputs,
            8,
            ForkJoinPool.commonPool(),
            i -> {
              try {
                Thread.sleep((50 - i) % 5);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return i * 10;
            });
    assertThat(out).isEqualTo(inputs.stream().map(i -> i * 10).toList());
  }

  @Test
  void neverRunsMoreThanPermitsAtOnce() {
    int permits = 3;
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    BoundedFanout.mapOrdered(
        IntStream.range(0, 40).boxed().toList(),
        permits,
        ForkJoinPool.commonPool(),
        i -> {
          int now = inFlight.incrementAndGet();
          peak.accumulateAndGet(now, Math::max);
          try {
            Thread.sleep(2);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            inFlight.decrementAndGet();
          }
          return i;
        });
    assertThat(peak.get()).isLessThanOrEqualTo(permits);
  }

  @Test
  void taskFailureSurfacesUnwrapped() {
    assertThatThrownBy(
            () ->
                BoundedFanout.mapOrdered(
                    List.of(1, 2, 3),
                    4,
                    ForkJoinPool.commonPool(),
                    i -> {
                      if (i == 2) {
                        throw new IllegalStateException("boom");
                      }
                      return i;
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom");
  }

  @Test
  void submissionFailureWaitsForAlreadySubmittedTasks() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch allowTaskCompletion = new CountDownLatch(1);
    CountDownLatch mapReturned = new CountDownLatch(1);
    AtomicInteger submissions = new AtomicInteger();
    Executor rejectSecondSubmission =
        command -> {
          if (submissions.getAndIncrement() == 0) {
            CompletableFuture.runAsync(command);
            return;
          }
          throw new RejectedExecutionException("executor saturated");
        };

    CompletableFuture<Throwable> result =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                BoundedFanout.mapOrdered(
                    List.of(1, 2),
                    2,
                    rejectSecondSubmission,
                    ignored -> {
                      taskStarted.countDown();
                      try {
                        allowTaskCompletion.await();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("task interrupted", e);
                      }
                      return ignored;
                    });
                return null;
              } catch (Throwable failure) {
                return failure;
              } finally {
                mapReturned.countDown();
              }
            });

    assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(mapReturned.await(100, TimeUnit.MILLISECONDS)).isFalse();
    allowTaskCompletion.countDown();
    assertThat(result.join())
        .isInstanceOf(RejectedExecutionException.class)
        .hasMessage("executor saturated");
  }

  @Test
  void cancellationInterruptsRunningTaskAndReturnsWithoutWaitingForIt() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskInterrupted = new CountDownLatch(1);
    CountDownLatch allowTaskCompletion = new CountDownLatch(1);
    AtomicBoolean cancelled = new AtomicBoolean();

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      CompletableFuture<Throwable> result =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  BoundedFanout.mapOrdered(
                      List.of(1, 2),
                      2,
                      executor,
                      ignored -> {
                        taskStarted.countDown();
                        try {
                          allowTaskCompletion.await();
                        } catch (InterruptedException e) {
                          taskInterrupted.countDown();
                          Thread.currentThread().interrupt();
                          throw new CancellationException("task interrupted");
                        }
                        return ignored;
                      },
                      cancelled::get);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });

      assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();
      cancelled.set(true);
      try {
        assertThat(result.get(250, TimeUnit.MILLISECONDS))
            .isInstanceOf(CancellationException.class)
            .hasMessage("fan-out cancelled");
        assertThat(taskInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
      } finally {
        allowTaskCompletion.countDown();
      }
    }
  }

  @Test
  void rejectsZeroAndNegativePermits() {
    // A 0/negative permit count would hand out no permits, blocking every task forever on an
    // uninterruptible acquire while the caller blocks joining — a permanent hang. Fail fast.
    for (int badPermits : new int[] {0, -1, -8}) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.mapOrdered(
                      List.of(1, 2, 3), badPermits, ForkJoinPool.commonPool(), i -> i))
          .as("permits=%d", badPermits)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("permits must be >= 1");
    }
  }
}
