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

import ai.floedb.floecat.service.context.PropagatedContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs independent, mostly-blocking tasks with a concurrency bound.
 *
 * <p>All items are submitted immediately and wait on the internal semaphore, so callers must use an
 * executor that can queue the full input set without rejecting or starving runnable tasks.
 */
public final class BoundedFanout {

  private static final long CANCELLATION_POLL_MILLIS = 10;

  private BoundedFanout() {}

  /**
   * Apply {@code task} to each item on {@code executor}, at most {@code permits} running at once,
   * and return the results in input order. Each task runs under the caller's request context
   * (OpenTelemetry, engine/principal/correlation, MDC) re-established via {@link
   * PropagatedContext}, so ambient reads behave off-thread as they do on the caller's thread. A
   * task failure surfaces unwrapped — its original {@link RuntimeException} or {@link Error}, never
   * a {@link CompletionException} wrapper. With no cancellation, every task future completes before
   * the first joined failure propagates, so callers can safely clean up task-owned resources.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> items, int permits, Executor executor, Function<I, O> task) {
    validatePermits(permits);
    Semaphore gate = new Semaphore(permits);
    PropagatedContext context = PropagatedContext.capture();
    List<CompletableFuture<O>> futures = new ArrayList<>(items.size());
    for (I item : items) {
      submitCompletionFuture(
          futures,
          () ->
              CompletableFuture.supplyAsync(
                  () -> runTask(gate, context, task, item, () -> false), executor));
    }
    return collectCompletedFutures(futures);
  }

  /**
   * As {@link #mapOrdered(List, int, Executor, Function)}, but consumes each successful result on
   * the caller thread before observing the next result. This preserves input-order validation
   * precedence when a result consumer can itself fail, while still awaiting all submitted tasks
   * before surfacing that failure so callers can safely release task-owned resources.
   */
  public static <I, O> void forEachOrdered(
      List<I> items,
      int permits,
      Executor executor,
      Function<I, O> task,
      Consumer<? super O> consumer) {
    validatePermits(permits);
    Semaphore gate = new Semaphore(permits);
    PropagatedContext context = PropagatedContext.capture();
    List<CompletableFuture<O>> futures = new ArrayList<>(items.size());
    for (I item : items) {
      submitCompletionFuture(
          futures,
          () ->
              CompletableFuture.supplyAsync(
                  () -> runTask(gate, context, task, item, () -> false), executor));
    }
    consumeCompletedFutures(futures, consumer);
  }

  /**
   * As {@link #mapOrdered(List, int, Executor, Function)}, but {@code cancelled} is polled so a
   * cancelled stream interrupts every submitted task and returns without waiting for a slow task's
   * completion. Tasks must cooperate with interruption while blocked in downstream calls. The
   * permit wait is interruptible for the same reason. A non-cancellation task failure waits for
   * every already-submitted sibling before surfacing, so its latency is bounded by the slowest
   * sibling; this completion guarantee lets callers safely release task-owned resources.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> items,
      int permits,
      ExecutorService executor,
      Function<I, O> task,
      BooleanSupplier cancelled) {
    validatePermits(permits);
    Semaphore gate = new Semaphore(permits);
    PropagatedContext context = PropagatedContext.capture();
    List<Future<O>> futures = new ArrayList<>(items.size());
    try {
      for (I item : items) {
        if (cancelled.getAsBoolean()) {
          cancelSubmittedTasks(futures);
          throw cancelled();
        }
        submitTask(
            futures, () -> executor.submit(() -> runTask(gate, context, task, item, cancelled)));
      }
    } catch (CancellationException cancellationFailure) {
      cancelSubmittedTasks(futures);
      throw cancellationFailure;
    }

    List<O> results = new ArrayList<>(items.size());
    RuntimeException firstRuntimeFailure = null;
    Error firstErrorFailure = null;
    for (int index = 0; index < futures.size(); index++) {
      Future<O> future = futures.get(index);
      try {
        results.add(awaitCancellable(future, futures, cancelled));
      } catch (RuntimeException e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstRuntimeFailure = e;
        }
      } catch (Error e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstErrorFailure = e;
        }
      }
    }
    if (firstRuntimeFailure != null) {
      throw firstRuntimeFailure;
    }
    if (firstErrorFailure != null) {
      throw firstErrorFailure;
    }
    return results;
  }

  /**
   * Cancellation-aware ordered result consumption. A consumer failure has the same ordered
   * precedence as a task failure, while submitted work is cancelled promptly when requested. A
   * non-cancellation failure waits for already-submitted siblings before it surfaces, preserving
   * the task-resource completion guarantee at the cost of slowest-sibling failure latency.
   */
  public static <I, O> void forEachOrdered(
      List<I> items,
      int permits,
      ExecutorService executor,
      Function<I, O> task,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled) {
    validatePermits(permits);
    Semaphore gate = new Semaphore(permits);
    PropagatedContext context = PropagatedContext.capture();
    List<Future<O>> futures = new ArrayList<>(items.size());
    try {
      for (I item : items) {
        if (cancelled.getAsBoolean()) {
          cancelSubmittedTasks(futures);
          throw cancelled();
        }
        submitTask(
            futures, () -> executor.submit(() -> runTask(gate, context, task, item, cancelled)));
      }
    } catch (CancellationException cancellationFailure) {
      cancelSubmittedTasks(futures);
      throw cancellationFailure;
    }

    RuntimeException firstRuntimeFailure = null;
    Error firstErrorFailure = null;
    for (int index = 0; index < futures.size(); index++) {
      Future<O> future = futures.get(index);
      try {
        O result = awaitCancellable(future, futures, cancelled);
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          consumer.accept(result);
        }
      } catch (RuntimeException e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstRuntimeFailure = e;
        }
      } catch (Error e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstErrorFailure = e;
        }
      }
    }
    if (firstRuntimeFailure != null) {
      throw firstRuntimeFailure;
    }
    if (firstErrorFailure != null) {
      throw firstErrorFailure;
    }
  }

  /** Run one task under the captured context while holding one concurrency permit. */
  private static <I, O> O runTask(
      Semaphore gate,
      PropagatedContext context,
      Function<I, O> task,
      I item,
      BooleanSupplier cancelled) {
    acquire(gate);
    try {
      return context.supply(
          () -> {
            if (cancelled.getAsBoolean()) {
              throw cancelled();
            }
            return task.apply(item);
          });
    } finally {
      gate.release();
    }
  }

  /** Collect every completed future, preserving the first unwrapped task failure. */
  private static <O> List<O> collectCompletedFutures(List<CompletableFuture<O>> futures) {
    List<O> results = new ArrayList<>(futures.size());
    RuntimeException firstRuntimeFailure = null;
    Error firstErrorFailure = null;
    for (int index = 0; index < futures.size(); index++) {
      CompletableFuture<O> future = futures.get(index);
      try {
        results.add(Futures.join(future));
      } catch (RuntimeException e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstRuntimeFailure = e;
        }
      } catch (Error e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstErrorFailure = e;
        }
      }
    }
    if (firstRuntimeFailure != null) {
      throw firstRuntimeFailure;
    }
    if (firstErrorFailure != null) {
      throw firstErrorFailure;
    }
    return results;
  }

  /** Consume completed futures in input order, preserving the first task or consumer failure. */
  private static <O> void consumeCompletedFutures(
      List<CompletableFuture<O>> futures, Consumer<? super O> consumer) {
    RuntimeException firstRuntimeFailure = null;
    Error firstErrorFailure = null;
    for (int index = 0; index < futures.size(); index++) {
      CompletableFuture<O> future = futures.get(index);
      try {
        O result = Futures.join(future);
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          consumer.accept(result);
        }
      } catch (RuntimeException e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstRuntimeFailure = e;
        }
      } catch (Error e) {
        if (firstRuntimeFailure == null && firstErrorFailure == null) {
          firstErrorFailure = e;
        }
      }
    }
    if (firstRuntimeFailure != null) {
      throw firstRuntimeFailure;
    }
    if (firstErrorFailure != null) {
      throw firstErrorFailure;
    }
  }

  /** Await each already-submitted completion future after a submission failure. */
  private static <O> void submitCompletionFuture(
      List<CompletableFuture<O>> futures, Supplier<CompletableFuture<O>> submission) {
    try {
      futures.add(submission.get());
    } catch (RuntimeException | Error submissionFailure) {
      awaitCompletedFutures(futures);
      throw submissionFailure;
    }
  }

  /** Await each already-submitted task after an executor submission failure. */
  private static <O> void submitTask(List<Future<O>> futures, Supplier<Future<O>> submission) {
    try {
      futures.add(submission.get());
    } catch (RuntimeException | Error submissionFailure) {
      awaitSubmittedTasks(futures);
      throw submissionFailure;
    }
  }

  /** Await each already-submitted completion future after a submission failure. */
  private static void awaitCompletedFutures(List<? extends CompletableFuture<?>> futures) {
    for (CompletableFuture<?> future : futures) {
      try {
        Futures.join(future);
      } catch (RuntimeException | Error ignored) {
        // The submission failure is the caller-visible outcome; task failures only complete
        // cleanup.
      }
    }
  }

  /** Await each already-submitted task while preserving an executor submission failure. */
  private static void awaitSubmittedTasks(List<? extends Future<?>> futures) {
    boolean interrupted = false;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        interrupted = true;
        cancelSubmittedTasks(futures);
        break;
      } catch (ExecutionException | CancellationException ignored) {
        // The submission failure is the caller-visible outcome; task failures only complete
        // cleanup.
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Await one task while repeatedly observing stream cancellation. */
  private static <O> O awaitCancellable(
      Future<O> future, List<? extends Future<?>> futures, BooleanSupplier cancelled) {
    while (true) {
      if (cancelled.getAsBoolean()) {
        cancelSubmittedTasks(futures);
        throw cancelled();
      }
      try {
        return future.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // A bounded wait lets the next loop observe cancellation.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelSubmittedTasks(futures);
        throw cancelled();
      } catch (ExecutionException e) {
        rethrowTaskFailure(e.getCause());
      }
    }
  }

  /** Interrupt every submitted task, including work already inside a downstream call. */
  private static void cancelSubmittedTasks(List<? extends Future<?>> futures) {
    for (Future<?> future : futures) {
      future.cancel(true);
    }
  }

  /** Rethrow a task failure without an execution-wrapper layer. */
  private static void rethrowTaskFailure(Throwable failure) {
    if (failure instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new CompletionException(failure);
  }

  /** Validate the concurrency bound before any tasks are submitted. */
  private static void validatePermits(int permits) {
    if (permits < 1) {
      throw new IllegalArgumentException("BoundedFanout permits must be >= 1, got " + permits);
    }
  }

  /** Build the canonical cancellation failure returned to a stopped stream driver. */
  private static CancellationException cancelled() {
    return new CancellationException("fan-out cancelled");
  }

  /**
   * Interruptible permit acquire: a shutdown/interrupt aborts the task instead of pinning the
   * thread on an uninterruptible wait.
   */
  private static void acquire(Semaphore gate) {
    try {
      gate.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted while awaiting fan-out permit");
    }
  }
}
