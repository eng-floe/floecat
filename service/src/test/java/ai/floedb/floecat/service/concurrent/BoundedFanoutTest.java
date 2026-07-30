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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Verifies bounded scheduling, ordered delivery, fail-fast errors, and prompt cancellation. */
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
  void refillsPermitsWhileTheFirstResultIsBlocked() throws Exception {
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch laterItemStarted = new CountDownLatch(1);
    CountDownLatch initialWindowSubmitted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    AtomicInteger submissions = new AtomicInteger();
    LinkedBlockingQueue<Runnable> queuedTasks = new LinkedBlockingQueue<>();
    Executor countingExecutor =
        command -> {
          if (submissions.incrementAndGet() == 3) {
            initialWindowSubmitted.countDown();
          }
          queuedTasks.add(command);
        };

    CompletableFuture<List<Integer>> result =
        CompletableFuture.supplyAsync(
            () ->
                BoundedFanout.mapOrdered(
                    IntStream.range(0, 100).boxed().toList(),
                    3,
                    countingExecutor,
                    value -> {
                      if (value == 0) {
                        firstStarted.countDown();
                        try {
                          releaseFirst.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                      } else if (value >= 3) {
                        laterItemStarted.countDown();
                      }
                      return value;
                    }));

    // Before any submitted task completes, only the bounded window may reach the executor.
    assertThat(initialWindowSubmitted.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(submissions.get()).isEqualTo(3);
    ExecutorService workers = Executors.newFixedThreadPool(3);
    try {
      try {
        for (int i = 0; i < 3; i++) {
          workers.submit(
              () -> {
                try {
                  while (!result.isDone()) {
                    Runnable submitted = queuedTasks.poll(10, TimeUnit.MILLISECONDS);
                    if (submitted != null) {
                      submitted.run();
                    }
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
        }
        assertThat(firstStarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(laterItemStarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(submissions.get()).isGreaterThan(3);
      } finally {
        releaseFirst.countDown();
      }
      assertThat(result.get(1, TimeUnit.SECONDS)).hasSize(100);
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  void refillsBeforeDeliveringCompletionToTheCaller() throws Exception {
    CountDownLatch firstCompletionDelivered = new CountDownLatch(1);
    CountDownLatch thirdTaskStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstCompletion = new CountDownLatch(1);
    CountDownLatch releaseSecondTask = new CountDownLatch(1);

    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      CompletableFuture<Void> result =
          CompletableFuture.runAsync(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      executor,
                      value -> {
                        if (value == 1) {
                          try {
                            releaseSecondTask.await();
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                          }
                        }
                        if (value == 2) {
                          thirdTaskStarted.countDown();
                        }
                        return value;
                      },
                      value -> {
                        if (value == 0) {
                          firstCompletionDelivered.countDown();
                          try {
                            releaseFirstCompletion.await();
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                          }
                        }
                      },
                      () -> false));

      try {
        assertThat(firstCompletionDelivered.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(thirdTaskStarted.await(250, TimeUnit.MILLISECONDS)).isTrue();
      } finally {
        releaseFirstCompletion.countDown();
        releaseSecondTask.countDown();
      }
      result.get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  void orderedConsumerFailureCancelsActiveSiblingsAndReturnsPromptly() throws Exception {
    CountDownLatch activeSiblingsStarted = new CountDownLatch(2);
    CountDownLatch activeSiblingInterrupted = new CountDownLatch(1);
    AtomicInteger highestStarted = new AtomicInteger(-1);

    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () -> {
                BoundedFanout.forEachOrdered(
                    List.of(0, 1, 2, 3, 4),
                    2,
                    executor,
                    value -> {
                      highestStarted.accumulateAndGet(value, Math::max);
                      if (value > 0) {
                        activeSiblingsStarted.countDown();
                        try {
                          new CountDownLatch(1).await();
                        } catch (InterruptedException e) {
                          activeSiblingInterrupted.countDown();
                          Thread.currentThread().interrupt();
                          throw new CancellationException("active sibling interrupted");
                        }
                      }
                      return value;
                    },
                    value -> {
                      if (value == 0) {
                        throw new IllegalStateException("ordered merge failed");
                      }
                    },
                    () -> false);
              });

      assertThat(activeSiblingsStarted.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("ordered merge failed");
      assertThat(activeSiblingInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
      assertThat(highestStarted.get()).isEqualTo(2);
    }
  }

  @Test
  void orderedTaskFailureDoesNotWaitForStalledSibling() throws Exception {
    CountDownLatch siblingStarted = new CountDownLatch(1);
    CountDownLatch releaseSibling = new CountDownLatch(1);

    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () -> {
                BoundedFanout.forEachOrdered(
                    List.of(0, 1),
                    2,
                    executor,
                    value -> {
                      if (value == 0) {
                        try {
                          siblingStarted.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                        throw new IllegalStateException("ordered task failed");
                      }
                      siblingStarted.countDown();
                      while (releaseSibling.getCount() != 0) {
                        try {
                          releaseSibling.await();
                        } catch (InterruptedException ignored) {
                          // Deliberately ignore interruption: the known ordered failure must still
                          // return without waiting for this unrelated downstream call.
                        }
                      }
                      return value;
                    },
                    ignored -> {},
                    () -> false);
              });

      assertThat(siblingStarted.await(1, TimeUnit.SECONDS)).isTrue();
      try {
        assertThat(result.get(250, TimeUnit.MILLISECONDS))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("ordered task failed");
      } finally {
        releaseSibling.countDown();
      }
    }
  }

  @Test
  void orderedMapFailureDoesNotWaitForStalledSibling() throws Exception {
    CountDownLatch siblingStarted = new CountDownLatch(1);
    CountDownLatch releaseSibling = new CountDownLatch(1);

    CompletableFuture<Throwable> result =
        captureAsyncFailure(
            () ->
                BoundedFanout.mapOrdered(
                    List.of(0, 1),
                    2,
                    ForkJoinPool.commonPool(),
                    value -> {
                      if (value == 0) {
                        try {
                          siblingStarted.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                        throw new IllegalStateException("ordered task failed");
                      }
                      siblingStarted.countDown();
                      while (releaseSibling.getCount() != 0) {
                        try {
                          releaseSibling.await();
                        } catch (InterruptedException ignored) {
                          // CompletableFuture cancellation need not interrupt its running action.
                        }
                      }
                      return value;
                    }));

    assertThat(siblingStarted.await(1, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(result.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("ordered task failed");
    } finally {
      releaseSibling.countDown();
    }
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
  void submissionFailureDoesNotWaitForAlreadySubmittedTasks() throws Exception {
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
        captureAsyncFailure(
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
              } finally {
                mapReturned.countDown();
              }
            });

    assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(mapReturned.await(250, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(result.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("executor saturated");
    } finally {
      allowTaskCompletion.countDown();
    }
  }

  @Test
  void refillSubmissionFailureDoesNotWaitForAlreadySubmittedTasks() throws Exception {
    CountDownLatch blockingTaskStarted = new CountDownLatch(1);
    CountDownLatch allowBlockingTaskCompletion = new CountDownLatch(1);
    CountDownLatch mapReturned = new CountDownLatch(1);
    AtomicInteger submissions = new AtomicInteger();
    Executor rejectRefillSubmission =
        command -> {
          if (submissions.getAndIncrement() < 2) {
            CompletableFuture.runAsync(command);
            return;
          }
          throw new RejectedExecutionException("executor saturated during refill");
        };

    CompletableFuture<Throwable> result =
        captureAsyncFailure(
            () -> {
              try {
                BoundedFanout.mapOrdered(
                    List.of(1, 2, 3),
                    2,
                    rejectRefillSubmission,
                    value -> {
                      if (value == 1) {
                        try {
                          blockingTaskStarted.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError("first task interrupted", e);
                        }
                      }
                      if (value == 2) {
                        blockingTaskStarted.countDown();
                        try {
                          allowBlockingTaskCompletion.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError("task interrupted", e);
                        }
                      }
                      return value;
                    });
              } finally {
                mapReturned.countDown();
              }
            });

    assertThat(blockingTaskStarted.await(1, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(mapReturned.await(250, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(result.get(250, TimeUnit.MILLISECONDS))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("executor saturated during refill");
    } finally {
      allowBlockingTaskCompletion.countDown();
    }
  }

  @Test
  void cancellationInterruptsRunningTaskAndReturnsWithoutWaitingForIt() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskInterrupted = new CountDownLatch(1);
    CountDownLatch allowTaskCompletion = new CountDownLatch(1);
    AtomicBoolean cancelled = new AtomicBoolean();

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () -> {
                BoundedFanout.forEachOrdered(
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
                    ignored -> {},
                    cancelled::get);
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
  void rejectedSubmissionWinsWhenCancellationInterruptsActiveTasks() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskInterrupted = new CountDownLatch(1);
    AtomicBoolean cancelled = new AtomicBoolean();
    try (ExecutorService executor =
        new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.AbortPolicy())) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () -> {
                BoundedFanout.forEachOrdered(
                    List.of(1, 2),
                    2,
                    executor,
                    ignored -> {
                      taskStarted.countDown();
                      try {
                        new CountDownLatch(1).await();
                        return ignored;
                      } catch (InterruptedException e) {
                        taskInterrupted.countDown();
                        Thread.currentThread().interrupt();
                        throw new CancellationException("task interrupted");
                      }
                    },
                    ignored -> {},
                    cancelled::get);
              });

      assertThat(taskStarted.await(1, TimeUnit.SECONDS)).isTrue();
      cancelled.set(true);
      Throwable failure = result.get(250, TimeUnit.MILLISECONDS);
      assertThat(failure).isInstanceOf(RejectedExecutionException.class);
      assertThat(taskInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
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

  /** Run one assertion scenario asynchronously and expose its terminal failure as a value. */
  private static CompletableFuture<Throwable> captureAsyncFailure(Runnable scenario) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            scenario.run();
            return null;
          } catch (Throwable failure) {
            return failure;
          }
        });
  }
}
