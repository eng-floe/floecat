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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Verifies bounded scheduling, ordered delivery, fail-fast errors, and prompt cancellation. */
class BoundedFanoutTest {

  @Test
  void resultsAreReturnedInInputOrder() {
    List<Integer> inputs = IntStream.range(0, 50).boxed().toList();
    // Reverse the completion order (later items sleep less) to prove ordering is by input, not
    // completion. Needs a pool with real parallelism — on a 1-2 core agent commonPool() would run
    // these serially in submission order, so there would be nothing to reorder.
    try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
      List<Integer> out =
          BoundedFanout.mapOrdered(
              inputs,
              8,
              pool,
              i -> {
                try {
                  Thread.sleep((50 - i) % 5);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return i * 10;
              },
              () -> false);
      assertThat(out).isEqualTo(inputs.stream().map(i -> i * 10).toList());
    }
  }

  @Test
  void nonCancellableCallerSchedulesThroughTheBlockingTakePath() throws Exception {
    // Passing NEVER_CANCELLED by identity selects the blocking managedTake path (no cancellation to
    // poll). Assert it still schedules, orders, and refills correctly across an async executor —
    // reverse the completion order so a wrong take/refill would reorder results.
    try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
      List<Integer> out =
          BoundedFanout.mapOrdered(
              IntStream.range(0, 30).boxed().toList(),
              4,
              pool,
              i -> {
                try {
                  Thread.sleep((30 - i) % 3);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return i * 2;
              },
              BoundedFanout.NEVER_CANCELLED);
      assertThat(out).isEqualTo(IntStream.range(0, 30).map(i -> i * 2).boxed().toList());
    }
  }

  @Test
  void sameForkJoinPoolSchedulerSchedulesWhileAwaitingSubmittedTasks() throws Exception {
    // Scheduling only, not compensation: these tasks never block, so the single worker runs them
    // from its own deque and the scheduler never has to wait. Compensation on the polling path is
    // in fact broken for a same-pool ForkJoinPool — see the class javadoc, which no longer claims
    // that shape is supported.
    ForkJoinPool executor = new ForkJoinPool(1);
    try {
      Future<List<Integer>> result =
          executor.submit(
              () ->
                  BoundedFanout.mapOrdered(
                      List.of(1, 2), 1, executor, value -> value, () -> false));

      assertThat(result.get(5, TimeUnit.SECONDS)).containsExactly(1, 2);
    } finally {
      // See above: close() would hang on the regression this test catches.
      executor.shutdownNow();
    }
  }

  @Test
  void neverRunsMoreThanPermitsAtOnce() {
    // A pool with more threads than permits, rather than commonPool(): its parallelism is
    // max(1, cores - 1), so on a 1-2 core CI agent peak could never exceed 1 and the upper bound
    // would hold vacuously — a regression submitting the whole batch at once would still pass.
    int permits = 3;
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    try (ExecutorService pool = Executors.newFixedThreadPool(permits * 3)) {
      BoundedFanout.mapOrdered(
          IntStream.range(0, 40).boxed().toList(),
          permits,
          pool,
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
          },
          () -> false);
    }
    assertThat(peak.get()).isLessThanOrEqualTo(permits);
    // Lower bound so the upper bound cannot pass vacuously: with 40 tasks each sleeping 2ms on a
    // 9-thread pool, genuine overlap must occur, which is what makes the bound above meaningful.
    assertThat(peak.get()).as("the permit window must actually be exercised").isGreaterThan(1);
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
                          releaseFirst.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                      } else if (value >= 3) {
                        laterItemStarted.countDown();
                      }
                      return value;
                    },
                    () -> false));

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
                            releaseSecondTask.await(30, TimeUnit.SECONDS);
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
                            releaseFirstCompletion.await(30, TimeUnit.SECONDS);
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                          }
                        }
                      },
                      () -> false));

      try {
        assertThat(firstCompletionDelivered.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(thirdTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        releaseFirstCompletion.countDown();
        releaseSecondTask.countDown();
      }
      result.get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  void orderedConsumerFailureCancelsActiveSiblingsAndReturnsPromptly() throws Exception {
    AtomicInteger activeSiblingsStarted = new AtomicInteger();
    CountDownLatch activeSiblingInterrupted = new CountDownLatch(1);
    CountDownLatch activeSiblingRunning = new CountDownLatch(1);
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
                        activeSiblingsStarted.incrementAndGet();
                        activeSiblingRunning.countDown();
                        try {
                          // Bounded so a cancellation regression fails fast instead of parking this
                          // task — and ExecutorService.close() — for ~24h; the interrupt arrives
                          // well
                          // within this if cancellation works.
                          new CountDownLatch(1).await(30, TimeUnit.SECONDS);
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
                        // Wait for a sibling to be genuinely running before failing, so the
                        // cancellation this test is named for is always exercised. Without this the
                        // sibling may not have been picked up yet on a low-core agent and the
                        // assertion below had to be skipped, leaving the behaviour unasserted.
                        awaitLatch(activeSiblingRunning);
                        throw new IllegalStateException("ordered merge failed");
                      }
                    },
                    () -> false);
              });

      assertThat(result.get(5, TimeUnit.SECONDS))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("ordered merge failed");
      assertThat(activeSiblingsStarted.get())
          .as("a sibling must have been running for this to test anything")
          .isGreaterThan(0);
      assertThat(activeSiblingInterrupted.await(5, TimeUnit.SECONDS))
          .as("the consumer's failure must interrupt the active sibling")
          .isTrue();
      // Two separate claims. The upper bound is the real one: permits=2 must never let input 3 or
      // 4 start. The lower bound asserts the run got far enough to mean anything — a vacuous
      // isBetween(0, ...) would have passed even if only input 0 ever ran.
      assertThat(highestStarted.get())
          .as("permits=2 must not admit an input beyond index 2")
          .isLessThanOrEqualTo(2);
      assertThat(highestStarted.get())
          .as("the window must have opened past the head for this to test the bound")
          .isGreaterThanOrEqualTo(1);
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
                          siblingStarted.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                        throw new IllegalStateException("ordered task failed");
                      }
                      siblingStarted.countDown();
                      while (releaseSibling.getCount() != 0) {
                        try {
                          releaseSibling.await(30, TimeUnit.SECONDS);
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

      try {
        // Inside the try: this assertion can fail, and the sibling below swallows interrupts, so a
        // release that only ran on the happy path would park a pool worker until close() gives up.
        assertThat(siblingStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get(5, TimeUnit.SECONDS))
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
    // A dedicated 2-thread pool, not commonPool(): this scenario needs input 1 to run WHILE input 0
    // waits on it, and commonPool parallelism is max(1, cores - 1). On a 1-2 core CI agent input 1
    // would never get a worker, and input 0 — whose wait deliberately ignores interrupts to model a
    // non-interruptible store call — would park a shared pool worker for the rest of the JVM,
    // starving every later commonPool test.
    ExecutorService siblingPool = Executors.newFixedThreadPool(2);

    CompletableFuture<Throwable> result =
        captureAsyncFailure(
            () ->
                BoundedFanout.mapOrdered(
                    List.of(0, 1),
                    2,
                    siblingPool,
                    value -> {
                      if (value == 0) {
                        try {
                          siblingStarted.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(e);
                        }
                        throw new IllegalStateException("ordered task failed");
                      }
                      siblingStarted.countDown();
                      while (releaseSibling.getCount() != 0) {
                        try {
                          releaseSibling.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException ignored) {
                          // CompletableFuture cancellation need not interrupt its running action.
                        }
                      }
                      return value;
                    },
                    () -> false));

    try {
      // Inside the try, as above — and this pool is non-daemon, so a skipped shutdownNow would keep
      // the surefire JVM alive forever rather than just failing the test.
      assertThat(siblingStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(5, TimeUnit.SECONDS))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("ordered task failed");
    } finally {
      releaseSibling.countDown();
      siblingPool.shutdownNow();
      siblingPool.awaitTermination(5, TimeUnit.SECONDS);
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
                    },
                    () -> false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom");
  }

  @Test
  void submissionFailureDoesNotWaitForAlreadySubmittedTasks() throws Exception {
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
                      try {
                        allowTaskCompletion.await(30, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("task interrupted", e);
                      }
                      return ignored;
                    },
                    () -> false);
              } finally {
                mapReturned.countDown();
              }
            });

    try {
      assertThat(mapReturned.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(5, TimeUnit.SECONDS))
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
                          blockingTaskStarted.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError("first task interrupted", e);
                        }
                      }
                      if (value == 2) {
                        blockingTaskStarted.countDown();
                        try {
                          allowBlockingTaskCompletion.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError("task interrupted", e);
                        }
                      }
                      return value;
                    },
                    () -> false);
              } finally {
                mapReturned.countDown();
              }
            });

    assertThat(blockingTaskStarted.await(1, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(mapReturned.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(result.get(5, TimeUnit.SECONDS))
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
                        allowTaskCompletion.await(30, TimeUnit.SECONDS);
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

      assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();
      cancelled.set(true);
      try {
        assertThat(result.get(5, TimeUnit.SECONDS))
            .isInstanceOf(CancellationException.class)
            .hasMessage("fan-out cancelled");
        assertThat(taskInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        allowTaskCompletion.countDown();
      }
    }
  }

  @Test
  void aRejectedSubmissionSurfacesWhileActiveTasksAreInterrupted() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskInterrupted = new CountDownLatch(1);
    // Reject the second submission deterministically — only after the first task has signalled it
    // started — so afterFailure's interruption of the active task cannot race ahead of taskStarted
    // and leave the first task cancelled before it ever ran (the prior SynchronousQueue+AbortPolicy
    // setup was timing-dependent and intermittently flaky).
    RejectedExecutionHandler rejectAfterFirstStarted =
        (r, exec) -> {
          try {
            taskStarted.await(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          throw new RejectedExecutionException("second submission rejected");
        };
    try (ExecutorService executor =
        new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), rejectAfterFirstStarted)) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(1, 2),
                      2,
                      executor,
                      ignored -> {
                        taskStarted.countDown();
                        try {
                          // Bounded so a cancellation regression fails fast instead of parking this
                          // task — and ExecutorService.close() — for ~24h; the interrupt arrives
                          // well
                          // within this if cancellation works.
                          new CountDownLatch(1).await(30, TimeUnit.SECONDS);
                          return ignored;
                        } catch (InterruptedException e) {
                          taskInterrupted.countDown();
                          Thread.currentThread().interrupt();
                          throw new CancellationException("task interrupted");
                        }
                      },
                      ignored -> {},
                      () -> false));

      Throwable failure = result.get(5, TimeUnit.SECONDS);
      assertThat(failure)
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("second submission rejected");
      assertThat(taskInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void cancellationDuringRefillDoesNotPublishTheCompletedValue() throws Exception {
    // Task 0 completes and flips cancellation; the window then refills, whose beforeSubmit observes
    // the cancellation. The already-completed value must NOT be delivered to the consumer after
    // cancellation was observed — the stage throws CancellationException instead.
    AtomicBoolean cancelled = new AtomicBoolean();
    java.util.List<Integer> published = new java.util.ArrayList<>();
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      1,
                      executor,
                      i -> {
                        if (i == 0) {
                          cancelled.set(true);
                        }
                        return i;
                      },
                      published::add,
                      cancelled::get));
      assertThat(result.get(1, TimeUnit.SECONDS)).isInstanceOf(CancellationException.class);
      assertThat(published).isEmpty();
    }
  }

  @Test
  void aReachableFailureStopsTheRefillRatherThanRacingItsRejection() {
    // The executor here would reject a third submission, but never sees one: once input 1's queued
    // failure is reconciled it is reachable in input order, and fillWindow stops before submitting
    // input 2. So a reachable failure cannot race a refill rejection at all -- it is prevented
    // structurally rather than resolved by precedence. Asserting the submission count is what makes
    // that claim testable; without it this reads as a rejection test that silently never rejects.
    AtomicInteger submissions = new AtomicInteger();
    try (ExecutorService rejectsAThirdSubmission =
        countingDirectExecutor(submissions, 2, "submission rejected")) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      rejectsAThirdSubmission,
                      i -> {
                        if (i == 1) {
                          throw new IllegalStateException("store error on input 1");
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 1");
    }
    assertThat(submissions.get())
        .as("input 2 must never be submitted once input 1's failure is reachable")
        .isEqualTo(2);
  }

  @Test
  void aRefillRejectionStillDeliversTheSuccessesAlreadyReadyBeforeIt() {
    // The rejection path in surfaceSubmissionFailure delivers ready outcomes before rethrowing, so
    // work that already succeeded is not silently dropped when the executor saturates mid-batch.
    // Inputs 0 and 1 both succeed inline; the refill of input 2 is rejected. Both successes must
    // reach the consumer, and only then the RejectedExecutionException.
    AtomicInteger submissions = new AtomicInteger();
    java.util.List<Integer> published = new java.util.ArrayList<>();
    try (ExecutorService rejectsAThirdSubmission =
        countingDirectExecutor(submissions, 2, "executor saturated")) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      rejectsAThirdSubmission,
                      i -> i,
                      published::add,
                      () -> false))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("executor saturated");
    }
    assertThat(published)
        .as("successes ready before the rejection must still be delivered")
        .containsExactly(0, 1);
    assertThat(submissions.get()).isEqualTo(3);
  }

  @Test
  void aQueuedFailureSurvivesACancellationWhileAnEarlierSuccessIsStillQueued() {
    // As above, but cancellation is observed instead of a rejection. White-box: with the direct
    // executor the poll sequence is [beforeSubmit0, task0, beforeSubmit1, task1,
    // post-initial-fill-recheck] (take/outcome drain without reading the signal), so the signal
    // flips at index 4 -- in completeAll, right after the initial fill. There is no refill poll:
    // once input 1's queued failure is reconciled, fillWindow stops before submitting input 2.
    // Draining input 0's queued success makes input 1's failure reachable and win over the
    // cancellation.
    AtomicInteger polls = new AtomicInteger();
    BooleanSupplier cancelledAfterTheFailure = () -> polls.getAndIncrement() >= 4;
    try (ExecutorService direct = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      direct,
                      i -> {
                        if (i == 1) {
                          throw new IllegalStateException("store error on input 1");
                        }
                        return i;
                      },
                      ignored -> {},
                      cancelledAfterTheFailure))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 1");
    }
  }

  @Test
  void aReachableFailureIsSurfacedWithoutSubmittingLaterWork() {
    // Once recording a completion exposes a failure reachable in input order, the operation is
    // terminal and no further task may be submitted: on an inline/caller-runs executor that submit
    // would run the next task on the caller thread and could stall before the failure is ever
    // surfaced; on an async executor it is wasted work. permits=1 runs input 0 inline (it fails),
    // so the refill of input 1 must be skipped -- input 1's task never runs -- and input 0's
    // failure surfaces immediately.
    List<Integer> executed = new java.util.ArrayList<>();
    try (ExecutorService inline = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      1,
                      inline,
                      i -> {
                        executed.add(i);
                        if (i == 0) {
                          throw new IllegalStateException("store error on input 0");
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 0");
    }
    assertThat(executed).containsExactly(0);
  }

  @Test
  void anInitialFillRejectionSurfacesWhenNoFailureIsReachableYet() throws Exception {
    // The initial-fill rejection arm, which only runs when nothing has failed yet: input 0 blocks
    // on an async executor so no outcome is reachable, and input 1's submission is rejected inside
    // the initial-fill loop. The caller must see the RejectedExecutionException, and input 0 must
    // be interrupted rather than abandoned to run on.
    // (When a failure IS already reachable, fillWindow stops before this submit -- see
    // aReachableFailureStopsTheRefillRatherThanRacingItsRejection. That is why this test blocks
    // input 0 instead of failing it: a failing input 0 would mean no rejection ever fires.)
    AtomicInteger submissions = new AtomicInteger();
    CountDownLatch input0Started = new CountDownLatch(1);
    CountDownLatch input0Interrupted = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Executor rejectsTheSecondSubmission =
        command -> {
          if (submissions.getAndIncrement() >= 1) {
            // Reject only once input 0 is genuinely running. Otherwise the rejection unwinds and
            // cancels input 0 before the pool ever starts it, and the interrupt assertion below
            // would be testing a task that never ran.
            awaitLatch(input0Started);
            throw new RejectedExecutionException("initial fill rejected");
          }
          pool.execute(command);
        };
    try {
      CompletableFuture<Throwable> result =
          captureAsyncFailure(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      2,
                      rejectsTheSecondSubmission,
                      i -> {
                        input0Started.countDown();
                        try {
                          new CountDownLatch(1).await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          input0Interrupted.countDown();
                          Thread.currentThread().interrupt();
                          throw new CancellationException("input 0 interrupted");
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false));

      assertThat(result.get(5, TimeUnit.SECONDS))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("initial fill rejected");
      assertThat(input0Interrupted.await(5, TimeUnit.SECONDS))
          .as("the already-submitted sibling must be interrupted, not left running")
          .isTrue();
      assertThat(submissions.get())
          .as("the rejection this test is named for must actually fire")
          .isEqualTo(2);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aTerminalUnnotifiedFailureBeatsACancellationOnTheFinalCompletion() throws Exception {
    // Pins completeAll's post-completion reconciliation, and it is the only case that can: once
    // every item has been submitted, fillWindow's loop (gated on next < items.size()) never runs,
    // so it never reconciles. That leaves this recheck as the sole caller of
    // recordTerminalCompletions before delivery.
    //
    // permits=2 submits both inputs, so no refill remains. Input 1 fails and its future goes
    // terminal, but the barrier holds its queue notification. Input 0 then succeeds and flips
    // cancellation. The reconciliation must pick up input 1's held failure so it beats the
    // cancellation; without it, input 1's outcome is still unrecorded when deliverReady checks,
    // earliestReachableFailure sees nothing, and the caller gets CancellationException — the store
    // error masked by teardown, which is the failure mode this whole precedence rule exists to
    // prevent.
    var input1Terminal = new CountDownLatch(1);
    var releaseInput1Notification = new CountDownLatch(1);
    var cancelled = new AtomicBoolean(false);
    IntConsumer barrier =
        index -> {
          if (index == 1) {
            input1Terminal.countDown();
            awaitLatch(releaseInput1Notification);
          }
        };
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      2,
                      pool,
                      i -> {
                        if (i == 1) {
                          throw new IllegalStateException("store error on input 1");
                        }
                        awaitLatch(input1Terminal); // complete only once input 1 is terminal
                        cancelled.set(true); // flips as input 0 finishes, before delivery
                        return i;
                      },
                      ignored -> {},
                      cancelled::get,
                      barrier))
          .as("the held store failure must beat the cancellation, not be masked by it")
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 1");
    } finally {
      releaseInput1Notification.countDown();
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void cancellationRaisedByTheFinalConsumerCallbackStillAborts() {
    // Pins deliverReady's trailing abortIfCancelled — the one after the publish loop. When the LAST
    // outcome's callback raises cancellation there is no next publish to recheck before, and the
    // window is already drained, so the scheduler's loop would exit and forEachOrdered would return
    // NORMALLY on a cancelled request. (The per-publish check covers a callback that cancels
    // mid-prefix; only the final callback reaches this one.)
    var cancelled = new AtomicBoolean(false);
    var published = new java.util.ArrayList<Integer>();
    try (ExecutorService direct = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0),
                      1,
                      direct,
                      i -> i,
                      i -> {
                        published.add(i);
                        cancelled.set(true); // the final callback cancels
                      },
                      cancelled::get))
          .isInstanceOf(CancellationException.class);
    }
    assertThat(published).containsExactly(0);
  }

  @Test
  void aReachableFailureBeatsACancellationRaisedByAnEarlierConsumerCallback() {
    // Pins abortIfCancelled's reachable-failure precedence (as opposed to a bare throw
    // cancelled()).
    // permits=3 with a direct executor: fillWindow reconciles before each submit, so input 1's
    // failure is recorded — and stops the fill before input 2 — while input 0's success is still
    // undelivered. deliverReady then publishes input 0, whose callback cancels, and the recheck
    // before the next publish sees BOTH a set cancellation and input 1's reachable failure. The
    // caller must get the store error, the actual diagnosis, not the cancellation that raced it.
    var cancelled = new AtomicBoolean(false);
    var published = new java.util.ArrayList<Integer>();
    try (ExecutorService direct = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      3,
                      direct,
                      i -> {
                        if (i == 1) {
                          throw new IllegalStateException("store error on input 1");
                        }
                        return i;
                      },
                      i -> {
                        published.add(i);
                        cancelled.set(true); // input 0's callback cancels
                      },
                      cancelled::get))
          .as("a reachable failure must outrank the cancellation the callback raised")
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 1");
    }
    assertThat(published).containsExactly(0);
  }

  @Test
  void anInterruptRaisedMidDeliveryStopsTheRestOfTheReadyPrefix() throws Exception {
    // Pins abortIfCancelled's interrupt disjunct specifically. The pre-interrupt test above aborts
    // in checkCancelled, before delivery ever starts; here the interrupt arrives from input 0's
    // callback, with input 1's success already recorded and ready. The cooperative signal is
    // hard-false, so only the interrupt can stop the prefix — and it must, rather than draining the
    // remaining ready results under a thread the container is unwinding.
    // Runs on its own thread so the interrupt cannot leak into sibling tests.
    var published = new java.util.ArrayList<Integer>();
    var outcome = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread scheduler =
        new Thread(
            () -> {
              try (ExecutorService direct = directExecutorService()) {
                BoundedFanout.forEachOrdered(
                    List.of(0, 1, 2),
                    3,
                    direct,
                    i -> i,
                    i -> {
                      published.add(i);
                      if (i == 0) {
                        Thread.currentThread().interrupt();
                      }
                    },
                    BoundedFanout.NEVER_CANCELLED);
              } catch (Throwable t) {
                outcome.set(t);
              }
            });
    scheduler.start();
    scheduler.join(5_000);

    assertThat(outcome.get())
        .as("an interrupt mid-delivery must abort the batch")
        .isInstanceOf(CancellationException.class);
    assertThat(published)
        .as("the already-ready sibling must not be published after the interrupt")
        .containsExactly(0);
  }

  @Test
  void aPreInterruptedSchedulerThreadAbortsInsteadOfDrainingTheBatch() throws Exception {
    // Pins the Thread.isInterrupted() disjuncts in checkCancelled and abortIfCancelled. The
    // cooperative signal is hard-false (NEVER_CANCELLED), so the interrupt is the only stop signal:
    // a container unwinding this worker must not have the whole batch drained under it just because
    // every task's result is already queued.
    var completed = new java.util.concurrent.atomic.AtomicInteger();
    var outcome = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread scheduler =
        new Thread(
            () -> {
              Thread.currentThread().interrupt(); // as a container unwind leaves it
              try (ExecutorService direct = directExecutorService()) {
                BoundedFanout.forEachOrdered(
                    List.of(0, 1, 2),
                    2,
                    direct,
                    i -> {
                      completed.incrementAndGet();
                      return i;
                    },
                    ignored -> {},
                    BoundedFanout.NEVER_CANCELLED);
              } catch (Throwable t) {
                outcome.set(t);
              }
            });
    scheduler.start();
    scheduler.join(5_000);

    assertThat(outcome.get())
        .as("a pre-interrupted scheduler must abandon the batch")
        .isInstanceOf(CancellationException.class);
    // Exact, not a bound: checkCancelled runs at the very first beforeSubmit, so a pre-interrupted
    // scheduler submits nothing at all. A looser assertion would also pass if that check were gone
    // and the abort came from deliverReady instead — after the initial fill had already run
    // permits-worth of store calls for a request that is already being torn down.
    assertThat(completed.get())
        .as("a pre-interrupted scheduler must submit no work at all")
        .isEqualTo(0);
  }

  @Test
  void aSoleTaskFailureKeepsPrecedenceOverCancellationFlippedBeforeItReturns() {
    // The reconciliation must not blindly mask an already-recorded failure with the cancellation: a
    // sole unit that both fails and flips cancellation surfaces its store failure, not
    // CancellationException.
    AtomicBoolean cancelled = new AtomicBoolean(false);
    try (ExecutorService direct = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0),
                      1,
                      direct,
                      i -> {
                        cancelled.set(true);
                        throw new IllegalStateException("store error on input " + i);
                      },
                      ignored -> {},
                      cancelled::get))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 0");
    }
  }

  private static ExecutorService directExecutorService() {
    return directExecutor(Integer.MAX_VALUE);
  }

  /**
   * Same-thread executor: runs the first {@code runFirst} submissions inline and rejects the rest.
   */
  private static ExecutorService countingDirectExecutor(
      AtomicInteger submissions, int runFirst, String rejectionMessage) {
    return new java.util.concurrent.AbstractExecutorService() {
      @Override
      public void execute(Runnable command) {
        if (submissions.getAndIncrement() >= runFirst) {
          throw new RejectedExecutionException(rejectionMessage);
        }
        command.run();
      }

      @Override
      public void shutdown() {}

      @Override
      public List<Runnable> shutdownNow() {
        return List.of();
      }

      @Override
      public boolean isShutdown() {
        return false;
      }

      @Override
      public boolean isTerminated() {
        return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
      }
    };
  }

  private static ExecutorService directExecutor(int runFirst) {
    return new java.util.concurrent.AbstractExecutorService() {
      private final AtomicInteger submissions = new AtomicInteger();

      @Override
      public void execute(Runnable command) {
        if (submissions.getAndIncrement() >= runFirst) {
          throw new RejectedExecutionException("submission rejected");
        }
        command.run();
      }

      @Override
      public void shutdown() {}

      @Override
      public List<Runnable> shutdownNow() {
        return List.of();
      }

      @Override
      public boolean isShutdown() {
        return true;
      }

      @Override
      public boolean isTerminated() {
        return true;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
      }
    };
  }

  @Test
  void rejectsZeroAndNegativePermits() {
    // A 0/negative permit count would hand out no permits, blocking every task forever on an
    // uninterruptible acquire while the caller blocks joining — a permanent hang. Fail fast.
    for (int badPermits : new int[] {0, -1, -8}) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.mapOrdered(
                      List.of(1, 2, 3), badPermits, ForkJoinPool.commonPool(), i -> i, () -> false))
          .as("permits=%d", badPermits)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("permits must be >= 1");
    }
  }

  @Test
  void aTerminalButUnnotifiedFailureIsReconciledBeforeRefill() throws Exception {
    // A sibling's future becomes terminal before its queue notification lands (FutureTask publishes
    // its state before done()). At a refill decision that window must not let an already-completed
    // failure look unreachable and admit more work. permits=2 runs inputs 0 (fails) and 1
    // (succeeds); the barrier holds input 0's notification while its future is already terminal,
    // and
    // input 1 completes only once input 0 is terminal. Recording input 1 must then reconcile input
    // 0's terminal failure and skip the refill of input 2. Without that reconciliation input 2 is
    // submitted — and its task releases the held notification so the run finishes and the assertion
    // fails cleanly instead of deadlocking.
    var input0Terminal = new CountDownLatch(1);
    var releaseInput0Notification = new CountDownLatch(1);
    var input2Submitted = new AtomicBoolean(false);
    IntConsumer barrier =
        index -> {
          if (index == 0) {
            input0Terminal.countDown();
            awaitLatch(releaseInput0Notification);
          }
        };
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      pool,
                      i -> {
                        if (i == 0) {
                          throw new IllegalStateException("store error on input 0");
                        }
                        if (i == 1) {
                          awaitLatch(input0Terminal); // complete only once input 0 is terminal
                        }
                        if (i == 2) {
                          input2Submitted.set(true);
                          releaseInput0Notification.countDown(); // reached only on the buggy path
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false,
                      barrier))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 0");
      assertThat(input2Submitted.get())
          .as("input 2 must not be submitted once input 0's failure has completed")
          .isFalse();
    } finally {
      releaseInput0Notification.countDown();
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aCancellationDuringTheInitialFillIsReconciledBeforeDelivery() {
    // With more than one permit and a synchronous executor the initial fill records earlier
    // successes while it continues. If the last filled task flips cancellation before returning,
    // those successes must not be published — the initial delivery reconciles cancellation just as
    // the main loop does, so a stopped stream sees nothing.
    var cancelled = new AtomicBoolean(false);
    var published = new java.util.ArrayList<Integer>();
    try (ExecutorService inline = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      2,
                      inline,
                      i -> {
                        if (i == 1) {
                          cancelled.set(true); // last filled task flips cancellation as it finishes
                        }
                        return i;
                      },
                      published::add,
                      cancelled::get))
          .isInstanceOf(CancellationException.class);
    }
    assertThat(published).isEmpty();
  }

  @Test
  void aCancelledSiblingInterruptDoesNotMaskTheCancellation() {
    // Input 0 succeeds; input 1 is blocked inside a store call when the request cancels. Cancelling
    // the batch interrupts input 1, but reconciliation must not record input 1's cancelled future
    // as
    // a task failure and surface it in place of the request's cancellation.
    var input1Started = new CountDownLatch(1);
    var cancelled = new AtomicBoolean(false);
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2),
                      2,
                      pool,
                      i -> {
                        if (i == 0) {
                          awaitLatch(input1Started); // let input 1 start blocking first
                          cancelled.set(true); // request cancels while input 1 is in its store call
                          return 0;
                        }
                        if (i == 1) {
                          input1Started.countDown();
                          try {
                            new CountDownLatch(1).await(10, TimeUnit.SECONDS); // until interrupted
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("interrupted store call on input 1");
                          }
                        }
                        return i;
                      },
                      ignored -> {},
                      cancelled::get))
          .isInstanceOf(CancellationException.class)
          .hasMessage("fan-out cancelled");
    }
  }

  @Test
  void anInlineInitialFillStopsAtAReachableFailure() {
    // The initial fill must reconcile a task that failed inline during the preceding submit before
    // starting the next. permits=2 on a direct executor runs input 0 inline (it fails); input 1
    // must never be submitted — its side effect never runs — and input 0's failure surfaces.
    var executed = new java.util.ArrayList<Integer>();
    try (ExecutorService inline = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      2,
                      inline,
                      i -> {
                        executed.add(i);
                        if (i == 0) {
                          throw new IllegalStateException("store error on input 0");
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("store error on input 0");
    }
    assertThat(executed).containsExactly(0);
  }

  @Test
  void aBurstOfCompletionsRefillsEveryReopenedSlot() throws Exception {
    // recordTerminalCompletions can free several slots from a single consumed notification; the
    // window must refill all of them, not one, or the run collapses to concurrency 1. permits=3
    // submits inputs 0..2; inputs 1 and 2 finish and their notifications are held (terminal but not
    // queued) while input 0 completes last as the only consumed notification. Recording input 0
    // must
    // then reopen and refill all three slots, so inputs 3, 4 and 5 all reach the executor together.
    var input1Held = new CountDownLatch(1);
    var input2Held = new CountDownLatch(1);
    var releaseHeldNotifications = new CountDownLatch(1);
    var laterTasksInFlight = new CountDownLatch(3);
    var releaseLaterTasks = new CountDownLatch(1);
    IntConsumer barrier =
        index -> {
          if (index == 1) {
            input1Held.countDown();
            awaitLatch(releaseHeldNotifications);
          } else if (index == 2) {
            input2Held.countDown();
            awaitLatch(releaseHeldNotifications);
          }
        };
    ExecutorService pool = Executors.newCachedThreadPool();
    try {
      CompletableFuture<Void> fanout =
          CompletableFuture.runAsync(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2, 3, 4, 5),
                      3,
                      pool,
                      i -> {
                        if (i == 0) {
                          awaitLatch(input1Held); // input 0 finishes only once 1 and 2 are terminal
                          awaitLatch(input2Held);
                        } else if (i >= 3) {
                          laterTasksInFlight.countDown();
                          awaitLatch(releaseLaterTasks);
                        }
                        return i;
                      },
                      ignored -> {},
                      () -> false,
                      barrier));
      assertThat(laterTasksInFlight.await(5, TimeUnit.SECONDS))
          .as("all three reopened slots must be refilled, not just one")
          .isTrue();
      releaseLaterTasks.countDown();
      releaseHeldNotifications.countDown();
      fanout.get(5, TimeUnit.SECONDS);
    } finally {
      releaseLaterTasks.countDown();
      releaseHeldNotifications.countDown();
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aTaskThrownCancellationDoesNotAbandonAnEarlierStillRunningTask() throws Exception {
    // A task body throwing CancellationException is a task failure, not a scheduler cancellation of
    // the batch. permits=2: input 1 throws it while input 0 is still running; input 0 must NOT be
    // interrupted/abandoned — input 1's failure is recorded at its own index and surfaces only in
    // input order, never by cancelling the earlier task.
    var input1Threw = new CountDownLatch(1);
    var releaseInput0 = new CountDownLatch(1);
    var input0Interrupted = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CompletableFuture<Void> fanout =
          CompletableFuture.runAsync(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1),
                      2,
                      pool,
                      i -> {
                        if (i == 1) {
                          input1Threw.countDown();
                          throw new CancellationException("task cancel at input 1");
                        }
                        try {
                          releaseInput0.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          input0Interrupted.countDown();
                          throw new CancellationException("input 0 interrupted");
                        }
                        return 0;
                      },
                      ignored -> {},
                      () -> false));
      assertThat(input1Threw.await(5, TimeUnit.SECONDS)).isTrue();
      // This asserts an absence, so it waits for the bad event rather than sleeping and looking
      // afterwards: a regression that abandons input 0 interrupts it while it is blocked here and
      // trips the latch at once (fast failure), while correct behaviour simply burns the timeout.
      // A fixed sleep would have had the same floor with none of the fast failure.
      assertThat(input0Interrupted.await(500, TimeUnit.MILLISECONDS))
          .as("a later task's cancellation must not abandon an earlier still-running task")
          .isFalse();
      releaseInput0.countDown();
      assertThatThrownBy(fanout::join)
          .hasCauseInstanceOf(CancellationException.class)
          .hasRootCauseMessage("task cancel at input 1");
      assertThat(input0Interrupted.getCount())
          .as("input 0 must still not have been interrupted once the batch has settled")
          .isEqualTo(1);
    } finally {
      releaseInput0.countDown();
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aSynchronousExecutorDeliversIncrementallyRatherThanRunningTheWholeBatchFirst() {
    // With a same-thread executor, recordTerminalCompletions empties active every iteration;
    // without
    // a per-call submission cap, one fillWindow call would run the entire input before delivering
    // any result (and rescan a growing prefix, O(N^2)). Assert the first result is delivered before
    // the last input runs.
    var events = new java.util.ArrayList<String>();
    try (ExecutorService inline = directExecutorService()) {
      BoundedFanout.forEachOrdered(
          List.of(0, 1, 2, 3, 4, 5),
          2,
          inline,
          i -> {
            events.add("run " + i);
            return i;
          },
          i -> events.add("deliver " + i),
          () -> false);
    }
    assertThat(events.indexOf("deliver 0"))
        .as("the first result must be delivered before the last input runs")
        .isLessThan(events.indexOf("run 5"));
  }

  @Test
  void aThrowingCompletionBarrierDoesNotOrphanTheTerminalSlot() {
    // The barrier runs in done() before the slot is enqueued. If a throw skipped that enqueue, a
    // sole-slot fan-out would wait forever on a future that is already terminal — no sibling
    // notification exists to wake it. The slot must be enqueued regardless, so the scheduler still
    // makes progress and delivers the completed result.
    IntConsumer throwingBarrier =
        index -> {
          throw new IllegalStateException("barrier failed for index " + index);
        };
    List<Integer> delivered = new java.util.ArrayList<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () ->
              BoundedFanout.forEachOrdered(
                  List.of(0), 1, pool, i -> i, delivered::add, () -> false, throwingBarrier),
          "a barrier failure must not orphan the slot and hang take()");
    }
    assertThat(delivered).containsExactly(0);
  }

  @Test
  void cancellationRaisedByAConsumerCallbackStopsTheRestOfTheReadyPrefix() {
    // A whole prefix can be ready at once (here: an inline executor runs every task before delivery
    // begins). If the first callback cancels, the remaining already-ready values must not be
    // published to the now-stopped stream, and the call must not return normally just because the
    // window is already drained.
    var cancelled = new AtomicBoolean(false);
    var published = new java.util.ArrayList<Integer>();
    try (ExecutorService inline = directExecutorService()) {
      assertThatThrownBy(
              () ->
                  BoundedFanout.forEachOrdered(
                      List.of(0, 1, 2, 3),
                      4,
                      inline,
                      i -> i,
                      i -> {
                        published.add(i);
                        cancelled.set(true); // the first delivery stops the stream
                      },
                      cancelled::get))
          .isInstanceOf(CancellationException.class);
    }
    assertThat(published)
        .as("no further ready result may be published after the consumer cancelled")
        .containsExactly(0);
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      // Bounded so a test regression that never releases the latch fails fast rather than parking a
      // worker task — and the managing ExecutorService.close() — for up to a day.
      if (!latch.await(30, TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for a test coordination latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted");
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
