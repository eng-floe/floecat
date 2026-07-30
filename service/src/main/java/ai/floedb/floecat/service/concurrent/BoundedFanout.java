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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs independent, mostly-blocking tasks with a concurrency bound.
 *
 * <p>At most the configured number of tasks are submitted at a time. A completed task immediately
 * opens one submission slot, independent of input order; completed results are retained by index
 * and observed in input order after the bounded window has drained.
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
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    completeAll(items, permits, executor, task, outcomes, (index, outcome) -> {});
    return orderedResults(outcomes);
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
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    OrderedOutcomeConsumer<O> ordered = new OrderedOutcomeConsumer<>(outcomes, consumer);
    completeAll(items, permits, executor, task, outcomes, ordered::accept);
    ordered.throwIfFailed();
  }

  /**
   * As {@link #mapOrdered(List, int, Executor, Function)}, but {@code cancelled} is polled so a
   * cancelled stream interrupts every submitted task and returns without waiting for a slow task's
   * completion. Tasks must cooperate with interruption while blocked in downstream calls. The
   * submission window observes cancellation before it submits more work. A non-cancellation task
   * failure waits for every input task before surfacing, so callers can safely release task-owned
   * resources.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> items,
      int permits,
      ExecutorService executor,
      Function<I, O> task,
      BooleanSupplier cancelled) {
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    completeAllCancellable(
        items, permits, executor, task, cancelled, outcomes, (index, outcome) -> {});
    return orderedResults(outcomes);
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
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    OrderedOutcomeConsumer<O> ordered = new OrderedOutcomeConsumer<>(outcomes, consumer);
    completeAllCancellable(items, permits, executor, task, cancelled, outcomes, ordered::accept);
    ordered.throwIfFailed();
  }

  /** Re-throw the first input-ordered task failure after every bounded task has finished. */
  private static <O> List<O> orderedResults(List<TaskOutcome<O>> outcomes) {
    List<O> results = new ArrayList<>(outcomes.size());
    for (TaskOutcome<O> outcome : outcomes) {
      if (outcome.failure() != null) {
        rethrowTaskFailure(outcome.failure());
      }
      results.add(outcome.result());
    }
    return results;
  }

  /**
   * Submit no more than {@code permits} tasks at once, recording each outcome as it completes so a
   * fast later task immediately replenishes the window despite ordered result observation.
   */
  private static <I, O> void completeAll(
      List<I> items,
      int permits,
      Executor executor,
      Function<I, O> task,
      List<TaskOutcome<O>> outcomes,
      BiConsumer<Integer, TaskOutcome<O>> onCompletion) {
    validatePermits(permits);
    PropagatedContext context = PropagatedContext.capture();
    BlockingQueue<CompletionSlot<O>> completions = new LinkedBlockingQueue<>();
    List<CompletionSlot<O>> active = new ArrayList<>(permits);
    int next = 0;
    try {
      while (next < items.size() && active.size() < permits) {
        active.add(
            submitCompletionTask(next, items.get(next++), executor, context, task, completions));
      }
      while (!active.isEmpty()) {
        CompletionSlot<O> slot = takeCompletion(completions);
        active.remove(slot);
        TaskOutcome<O> outcome = completedOutcome(slot.completion);
        outcomes.set(slot.index, outcome);
        if (next < items.size()) {
          active.add(
              submitCompletionTask(next, items.get(next++), executor, context, task, completions));
        }
        onCompletion.accept(slot.index, outcome);
      }
    } catch (RuntimeException | Error submissionFailure) {
      awaitCompletedTasks(active);
      throw submissionFailure;
    }
  }

  /** Cancellable counterpart of the bounded completion scheduler. */
  private static <I, O> void completeAllCancellable(
      List<I> items,
      int permits,
      ExecutorService executor,
      Function<I, O> task,
      BooleanSupplier cancelled,
      List<TaskOutcome<O>> outcomes,
      BiConsumer<Integer, TaskOutcome<O>> onCompletion) {
    validatePermits(permits);
    PropagatedContext context = PropagatedContext.capture();
    BlockingQueue<CompletionSlot<O>> completions = new LinkedBlockingQueue<>();
    List<CompletionSlot<O>> active = new ArrayList<>(permits);
    int next = 0;
    try {
      while (next < items.size() && active.size() < permits) {
        checkCancelled(cancelled, active);
        active.add(
            submitCancellableTask(
                next, items.get(next++), executor, context, task, cancelled, completions));
      }
      while (!active.isEmpty()) {
        CompletionSlot<O> slot = takeCancellableCompletion(completions, active, cancelled);
        TaskOutcome<O> outcome = completedOutcome(slot.task, active, cancelled);
        outcomes.set(slot.index, outcome);
        active.remove(slot);
        if (next < items.size()) {
          checkCancelled(cancelled, active);
          active.add(
              submitCancellableTask(
                  next, items.get(next++), executor, context, task, cancelled, completions));
        }
        onCompletion.accept(slot.index, outcome);
      }
    } catch (CancellationException cancellationFailure) {
      cancelSubmittedTasks(active);
      throw cancellationFailure;
    } catch (RuntimeException | Error submissionFailure) {
      awaitSubmittedTasks(active, cancelled);
      throw submissionFailure;
    }
  }

  private static <I, O> CompletionSlot<O> submitCompletionTask(
      int index,
      I item,
      Executor executor,
      PropagatedContext context,
      Function<I, O> task,
      BlockingQueue<CompletionSlot<O>> completions) {
    CompletionSlot<O> slot = new CompletionSlot<>(index);
    slot.completion =
        CompletableFuture.supplyAsync(() -> context.supply(() -> task.apply(item)), executor);
    // Completion stages run only after the CompletableFuture has reached its terminal state, so
    // removing this slot cannot briefly exceed the submitted-task window.
    slot.completion.whenComplete((ignored, failure) -> completions.add(slot));
    return slot;
  }

  private static <I, O> CompletionSlot<O> submitCancellableTask(
      int index,
      I item,
      ExecutorService executor,
      PropagatedContext context,
      Function<I, O> task,
      BooleanSupplier cancelled,
      BlockingQueue<CompletionSlot<O>> completions) {
    CompletionSlot<O> slot = new CompletionSlot<>(index);
    FutureTask<O> submitted =
        new FutureTask<O>(
            () ->
                context.supply(
                    () -> {
                      if (cancelled.getAsBoolean()) {
                        throw cancelled();
                      }
                      return task.apply(item);
                    })) {
          @Override
          protected void done() {
            completions.add(slot);
          }
        };
    slot.task = submitted;
    executor.execute(submitted);
    return slot;
  }

  private static <O> CompletionSlot<O> takeCompletion(
      BlockingQueue<CompletionSlot<O>> completions) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return completions.take();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Await an active completion while polling cancellation instead of joining a slow input. */
  private static <O> CompletionSlot<O> takeCancellableCompletion(
      BlockingQueue<CompletionSlot<O>> completions,
      List<CompletionSlot<O>> active,
      BooleanSupplier cancelled) {
    while (true) {
      checkCancelled(cancelled, active);
      try {
        CompletionSlot<O> slot = completions.poll(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
        if (slot != null) {
          return slot;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelSubmittedTasks(active);
        throw cancelled();
      }
    }
  }

  private static <O> TaskOutcome<O> completedOutcome(CompletableFuture<O> future) {
    try {
      return TaskOutcome.success(Futures.join(future));
    } catch (RuntimeException | Error failure) {
      return TaskOutcome.failure(failure);
    }
  }

  private static <O> TaskOutcome<O> completedOutcome(
      Future<O> future, List<CompletionSlot<O>> active, BooleanSupplier cancelled) {
    try {
      return TaskOutcome.success(awaitCancellable(future, active, cancelled));
    } catch (RuntimeException | Error failure) {
      return TaskOutcome.failure(failure);
    }
  }

  /**
   * Await one just-completed task while still honoring a cancellation that races its completion.
   */
  private static <O> O awaitCancellable(
      Future<O> future, List<? extends CompletionSlot<?>> active, BooleanSupplier cancelled) {
    while (true) {
      checkCancelled(cancelled, active);
      try {
        return future.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // A bounded wait lets the next loop observe cancellation.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelSubmittedTasks(active);
        throw cancelled();
      } catch (ExecutionException e) {
        rethrowTaskFailure(e.getCause());
      }
    }
  }

  private static <O> List<TaskOutcome<O>> emptyOutcomes(int size) {
    return new ArrayList<>(java.util.Collections.nCopies(size, null));
  }

  /** Wait for every completion future after rejecting one bounded-window submission. */
  private static void awaitCompletedTasks(List<? extends CompletionSlot<?>> active) {
    for (CompletionSlot<?> slot : active) {
      try {
        Futures.join(slot.completion);
      } catch (RuntimeException | Error ignored) {
        // The submission failure is the caller-visible outcome; task failures only complete
        // task-owned cleanup.
      }
    }
  }

  /**
   * Wait for submitted tasks after a rejected submission, while still returning promptly when the
   * stream is cancelled. Without cancellation the original rejection remains caller-visible.
   */
  private static void awaitSubmittedTasks(
      List<? extends CompletionSlot<?>> active, BooleanSupplier cancelled) {
    for (CompletionSlot<?> slot : active) {
      while (true) {
        checkCancelled(cancelled, active);
        try {
          slot.task.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
          break;
        } catch (TimeoutException ignored) {
          // A bounded wait lets the next loop observe cancellation.
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          cancelSubmittedTasks(active);
          throw cancelled();
        } catch (ExecutionException | CancellationException ignored) {
          // The submission failure is the caller-visible outcome; task failures only complete
          // task-owned cleanup.
          break;
        }
      }
    }
  }

  private static void checkCancelled(
      BooleanSupplier cancelled, List<? extends CompletionSlot<?>> active) {
    if (cancelled.getAsBoolean()) {
      cancelSubmittedTasks(active);
      throw cancelled();
    }
  }

  /** Interrupt every submitted task, including work already inside a downstream call. */
  private static void cancelSubmittedTasks(List<? extends CompletionSlot<?>> active) {
    for (CompletionSlot<?> slot : active) {
      if (slot.task != null) {
        slot.task.cancel(true);
      }
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

  /** One submitted input and the future through which its completion is observed. */
  private static final class CompletionSlot<O> {
    private final int index;
    private CompletableFuture<O> completion;
    private Future<O> task;

    private CompletionSlot(int index) {
      this.index = index;
    }
  }

  /** The successful result or unwrapped failure captured for one input. */
  private record TaskOutcome<O>(O result, Throwable failure) {
    private static <O> TaskOutcome<O> success(O result) {
      return new TaskOutcome<>(result, null);
    }

    private static <O> TaskOutcome<O> failure(Throwable failure) {
      return new TaskOutcome<>(null, failure);
    }
  }

  /**
   * Delivers completed results to a caller in input order without stalling the submission window.
   */
  private static final class OrderedOutcomeConsumer<O> {
    private final List<TaskOutcome<O>> outcomes;
    private final Consumer<? super O> consumer;
    private int nextIndex;
    private RuntimeException firstRuntimeFailure;
    private Error firstErrorFailure;

    private OrderedOutcomeConsumer(List<TaskOutcome<O>> outcomes, Consumer<? super O> consumer) {
      this.outcomes = outcomes;
      this.consumer = consumer;
    }

    private void accept(int index, TaskOutcome<O> ignored) {
      while (nextIndex < outcomes.size() && outcomes.get(nextIndex) != null) {
        TaskOutcome<O> outcome = outcomes.get(nextIndex++);
        try {
          if (outcome.failure() != null) {
            rethrowTaskFailure(outcome.failure());
          }
          if (firstRuntimeFailure == null && firstErrorFailure == null) {
            consumer.accept(outcome.result());
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
    }

    private void throwIfFailed() {
      if (firstRuntimeFailure != null) {
        throw firstRuntimeFailure;
      }
      if (firstErrorFailure != null) {
        throw firstErrorFailure;
      }
    }
  }
}
