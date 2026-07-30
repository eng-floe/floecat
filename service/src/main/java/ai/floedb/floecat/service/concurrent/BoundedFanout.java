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
 * opens one submission slot, independent of input order. Completed results are retained by index;
 * mapping returns them after the bounded window drains, while ordered consumption delivers each
 * contiguous input-order prefix as soon as it is available.
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
   * an execution-wrapper exception. With no cancellation, every task future completes before the
   * first joined failure propagates, so callers can safely clean up task-owned resources.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> items, int permits, Executor executor, Function<I, O> task) {
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    completeAll(
        items,
        permits,
        outcomes,
        (index, outcome) -> {},
        new CompletionTaskRuntime<>(executor, PropagatedContext.capture(), task));
    return orderedResults(outcomes);
  }

  /**
   * Cancellation-aware ordered result consumption. A consumer failure has the same ordered
   * precedence as a task failure, while submitted work is cancelled promptly when requested. A
   * non-cancellation failure waits for already-submitted siblings before it surfaces, preserving
   * the task-resource completion guarantee at the cost of slowest-sibling failure latency. {@code
   * cancelled} may be read concurrently by the scheduler and workers, so it must be non-blocking
   * and thread-safe (typically {@link java.util.concurrent.atomic.AtomicBoolean#get}). Observed
   * cancellation throws {@link CancellationException}.
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
    completeAll(
        items,
        permits,
        outcomes,
        ordered::accept,
        new CancellableTaskRuntime<>(executor, PropagatedContext.capture(), task, cancelled));
  }

  /** Re-throw the first input-ordered task failure after every bounded task has finished. */
  private static <O> List<O> orderedResults(List<TaskOutcome<O>> outcomes) {
    List<O> results = new ArrayList<>(outcomes.size());
    for (TaskOutcome<O> outcome : outcomes) {
      if (outcome.failure() != null) {
        throw Futures.propagate(outcome.failure(), "unexpected checked exception from fan-out");
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
      List<TaskOutcome<O>> outcomes,
      BiConsumer<Integer, TaskOutcome<O>> onCompletion,
      TaskRuntime<I, O> runtime) {
    validatePermits(permits);
    List<CompletionSlot<O>> active = new ArrayList<>(permits);
    int next = 0;
    try {
      while (next < items.size() && active.size() < permits) {
        runtime.beforeSubmit(active);
        active.add(runtime.submit(next, items.get(next++)));
      }
      while (!active.isEmpty()) {
        CompletionSlot<O> slot = runtime.take(active);
        TaskOutcome<O> outcome = runtime.outcome(slot, active);
        outcomes.set(slot.index, outcome);
        active.remove(slot);
        if (next < items.size()) {
          runtime.beforeSubmit(active);
          active.add(runtime.submit(next, items.get(next++)));
        }
        onCompletion.accept(slot.index, outcome);
      }
    } catch (RuntimeException | Error processingFailure) {
      runtime.afterFailure(active, processingFailure);
      throw processingFailure;
    }
  }

  /** Task lifecycle strategy used by the shared bounded-window scheduler. */
  private interface TaskRuntime<I, O> {
    /** Validate scheduler state before submission without adding or removing active slots. */
    default void beforeSubmit(List<CompletionSlot<O>> active) {}

    /** Submit one indexed item and return the slot the scheduler owns until completion. */
    CompletionSlot<O> submit(int index, I item);

    /** Wait for and return one completed member of {@code active} without mutating that list. */
    CompletionSlot<O> take(List<CompletionSlot<O>> active);

    /** Capture a completed slot as data; only scheduler cancellation may escape this method. */
    TaskOutcome<O> outcome(CompletionSlot<O> slot, List<CompletionSlot<O>> active);

    /**
     * Finish or cancel active work after scheduler/consumer failure. Cleanup must preserve the
     * supplied failure as the caller-visible outcome and attach any raced cancellation to it.
     */
    void afterFailure(List<CompletionSlot<O>> active, Throwable processingFailure);
  }

  /** Non-cancellable CompletableFuture task lifecycle. */
  private static final class CompletionTaskRuntime<I, O> implements TaskRuntime<I, O> {
    private final Executor executor;
    private final PropagatedContext context;
    private final Function<I, O> task;
    private final BlockingQueue<CompletionSlot<O>> completions = new LinkedBlockingQueue<>();

    private CompletionTaskRuntime(
        Executor executor, PropagatedContext context, Function<I, O> task) {
      this.executor = executor;
      this.context = context;
      this.task = task;
    }

    @Override
    public CompletionSlot<O> submit(int index, I item) {
      CompletionSlot<O> slot = new CompletionSlot<>(index);
      slot.completion =
          CompletableFuture.supplyAsync(() -> context.supply(() -> task.apply(item)), executor);
      // A terminal stage publishes the slot only after the task is no longer active.
      slot.completion.whenComplete((ignored, failure) -> completions.add(slot));
      return slot;
    }

    @Override
    public CompletionSlot<O> take(List<CompletionSlot<O>> active) {
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

    @Override
    public TaskOutcome<O> outcome(CompletionSlot<O> slot, List<CompletionSlot<O>> active) {
      try {
        return TaskOutcome.success(Futures.join(slot.completion));
      } catch (RuntimeException | Error failure) {
        return TaskOutcome.failure(failure);
      }
    }

    @Override
    public void afterFailure(List<CompletionSlot<O>> active, Throwable processingFailure) {
      awaitCompletedTasks(active);
    }
  }

  /** Interruptible FutureTask lifecycle with cooperative request cancellation. */
  private static final class CancellableTaskRuntime<I, O> implements TaskRuntime<I, O> {
    private final ExecutorService executor;
    private final PropagatedContext context;
    private final Function<I, O> task;
    private final BooleanSupplier cancelled;
    private final BlockingQueue<CompletionSlot<O>> completions = new LinkedBlockingQueue<>();

    private CancellableTaskRuntime(
        ExecutorService executor,
        PropagatedContext context,
        Function<I, O> task,
        BooleanSupplier cancelled) {
      this.executor = executor;
      this.context = context;
      this.task = task;
      this.cancelled = cancelled;
    }

    @Override
    public void beforeSubmit(List<CompletionSlot<O>> active) {
      checkCancelled(cancelled, active);
    }

    @Override
    public CompletionSlot<O> submit(int index, I item) {
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

    @Override
    public CompletionSlot<O> take(List<CompletionSlot<O>> active) {
      while (true) {
        checkCancelled(cancelled, active);
        try {
          CompletionSlot<O> slot =
              completions.poll(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
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

    @Override
    public TaskOutcome<O> outcome(CompletionSlot<O> slot, List<CompletionSlot<O>> active) {
      try {
        return TaskOutcome.success(awaitCancellable(slot.task, active, cancelled));
      } catch (RuntimeException | Error failure) {
        return TaskOutcome.failure(failure);
      }
    }

    @Override
    public void afterFailure(List<CompletionSlot<O>> active, Throwable processingFailure) {
      if (processingFailure instanceof CancellationException) {
        cancelSubmittedTasks(active);
        return;
      }
      try {
        awaitSubmittedTasks(active, cancelled);
      } catch (CancellationException cancellationFailure) {
        processingFailure.addSuppressed(cancellationFailure);
      }
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
        throw Futures.propagate(e.getCause(), "unexpected checked exception from fan-out");
      }
    }
  }

  private static <O> List<TaskOutcome<O>> emptyOutcomes(int size) {
    return new ArrayList<>(java.util.Collections.nCopies(size, null));
  }

  /** Wait for every active completion after scheduling or ordered consumption fails. */
  private static void awaitCompletedTasks(List<? extends CompletionSlot<?>> active) {
    for (CompletionSlot<?> slot : active) {
      try {
        Futures.join(slot.completion);
      } catch (RuntimeException | Error ignored) {
        // The scheduler or consumer failure is caller-visible; task failures only complete
        // task-owned cleanup.
      }
    }
  }

  /**
   * Wait for submitted tasks after scheduling or ordered consumption fails, while still returning
   * promptly when the stream is cancelled. Without cancellation the original failure remains
   * caller-visible.
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
          // The scheduler or consumer failure is caller-visible; task failures only complete
          // task-owned cleanup.
          break;
        }
      }
    }
  }

  /** Cancel every active task and fail the caller when its cooperative signal is set. */
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

    private OrderedOutcomeConsumer(List<TaskOutcome<O>> outcomes, Consumer<? super O> consumer) {
      this.outcomes = outcomes;
      this.consumer = consumer;
    }

    /** Deliver every newly contiguous outcome, stopping at the first unfinished input index. */
    private void accept(int index, TaskOutcome<O> ignored) {
      while (nextIndex < outcomes.size() && outcomes.get(nextIndex) != null) {
        TaskOutcome<O> outcome = outcomes.get(nextIndex++);
        if (outcome.failure() != null) {
          throw Futures.propagate(outcome.failure(), "unexpected checked exception from fan-out");
        }
        consumer.accept(outcome.result());
      }
    }
  }
}
