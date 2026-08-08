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
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Runs one blocking call behind process-wide admission while preserving prompt caller cancellation.
 *
 * <p>{@link AdmittedTask} is the sole owner of a call's result, cancellation, queue-discard
 * cleanup, and permit release. A caller can stop waiting immediately, but the task retains its
 * permit until a downstream operation that ignores interruption has genuinely returned.
 */
final class CancellableCallRunner {

  private static final Logger LOG = Logger.getLogger(CancellableCallRunner.class);
  private static final int MAX_CAUSE_DEPTH = 32;
  private static final long SLOW_ADMISSION_WARN_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final long CLOSURE_POLL_MILLIS = 250;
  private static final long ADMITTED_CANCELLATION_POLL_MILLIS = 25;
  private static final BooleanSupplier NEVER_CANCELLED = () -> false;

  static final String RUNTIME_CLOSED = "metadata I/O executor closed while the call was in flight";

  private CancellableCallRunner() {}

  static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation) {
    AdmittedTask<T> task =
        new AdmittedTask<>(permits, operation, cancelled, closed, true, messages, executor);
    acquire(permits, cancelled, closed, messages, onSaturation, ADMITTED_CANCELLATION_POLL_MILLIS);
    submit(executor, task);
    return awaitOutcome(
        task,
        messages,
        () ->
            cancelled.getAsBoolean()
                ? messages.cancellation()
                : closed.getAsBoolean() ? RUNTIME_CLOSED : null,
        ADMITTED_CANCELLATION_POLL_MILLIS);
  }

  static <T> T callWithoutCancellation(
      Executor executor,
      Semaphore permits,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation) {
    AdmittedTask<T> task =
        new AdmittedTask<>(permits, operation, NEVER_CANCELLED, closed, false, messages, executor);
    acquire(permits, NEVER_CANCELLED, closed, messages, onSaturation, CLOSURE_POLL_MILLIS);
    submit(executor, task);
    return awaitOutcome(
        task, messages, () -> closed.getAsBoolean() ? RUNTIME_CLOSED : null, CLOSURE_POLL_MILLIS);
  }

  /** Caller-facing cancellation outcomes for a dispatched blocking call. */
  record FailureMessages(String label, String cancellation, String interruption) {
    public FailureMessages {
      java.util.Objects.requireNonNull(label, "label");
      java.util.Objects.requireNonNull(cancellation, "cancellation");
      java.util.Objects.requireNonNull(interruption, "interruption");
    }

    FailureMessages(String cancellation, String interruption) {
      this("metadata-io", cancellation, interruption);
    }
  }

  /** Return permits owned by calls removed from a shutdown executor's queue. */
  static void cancelDiscardedTasks(List<Runnable> discardedTasks) {
    for (Runnable task : discardedTasks) {
      if (task instanceof AdmittedTask<?> admitted) {
        admitted.cancel(false);
      }
    }
  }

  private static <T> void submit(Executor executor, AdmittedTask<T> task) {
    try {
      executor.execute(task);
    } catch (RuntimeException | Error failure) {
      task.releasePermit();
      throw failure;
    }
  }

  private static <T> T awaitOutcome(
      AdmittedTask<T> task,
      FailureMessages messages,
      Supplier<String> abortReason,
      long pollMillis) {
    try {
      while (true) {
        if (task.isDone()) {
          return drain(task, messages);
        }
        String reason = abortReason.get();
        if (reason != null) {
          // FutureTask is the arbitration point: a completed value or failure wins when cancel
          // loses the race, while a successful cancel makes the caller return immediately.
          if (task.cancel(true)) {
            throw new CancellationException(reason);
          }
          return drain(task, messages);
        }
        try {
          return task.get(pollMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
          // Re-check cancellation and closure.
        } catch (InterruptedException e) {
          task.cancel(true);
          Thread.currentThread().interrupt();
          throw new CancellationException(messages.interruption());
        } catch (ExecutionException e) {
          throw Futures.propagate(
              e.getCause(), "unexpected checked exception from cancellable call");
        }
      }
    } catch (CancellationException cancellation) {
      // A task cancelled by shutdown rather than this caller never reaches an operation result.
      if (!task.isDone()) {
        task.cancel(true);
      }
      if (cancellation.getMessage() == null) {
        throw new CancellationException(RUNTIME_CLOSED);
      }
      throw cancellation;
    }
  }

  private static <T> T drain(AdmittedTask<T> task, FailureMessages messages) {
    try {
      return task.get();
    } catch (InterruptedException e) {
      task.cancel(true);
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    } catch (CancellationException cancelled) {
      throw new CancellationException(RUNTIME_CLOSED);
    } catch (ExecutionException e) {
      throw Futures.propagate(e.getCause(), "unexpected checked exception from cancellable call");
    }
  }

  private static void rejectIfClosed(Semaphore permits, BooleanSupplier closed) {
    if (closed.getAsBoolean()) {
      permits.release();
      throw new RejectedExecutionException("metadata I/O executor is closed");
    }
  }

  /** Acquire admission in cancellable polling intervals without losing interrupt semantics. */
  private static void acquire(
      Semaphore permits,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      FailureMessages messages,
      Runnable onSaturation,
      long pollMillis) {
    try {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      boolean sawCeilingFull;
      if (permits.getQueueLength() == 0) {
        if (permits.tryAcquire()) {
          rejectIfClosed(permits, closed);
          return;
        }
        sawCeilingFull = true;
      } else {
        sawCeilingFull = permits.availablePermits() == 0;
      }
      if (sawCeilingFull) {
        try {
          onSaturation.run();
        } catch (RuntimeException sinkFailure) {
          LOG.warnf(sinkFailure, "metadata I/O saturation sink failed");
        }
      }
      long waitingSinceNanos = System.nanoTime();
      boolean reportedSlowWait = false;
      while (true) {
        if (cancelled.getAsBoolean()) {
          throw new CancellationException(messages.cancellation());
        }
        if (!reportedSlowWait
            && System.nanoTime() - waitingSinceNanos > SLOW_ADMISSION_WARN_NANOS) {
          reportedSlowWait = true;
          LOG.warnf(
              "%s has waited over %ds for metadata I/O admission (%d free, %d waiting)",
              messages.label(),
              TimeUnit.NANOSECONDS.toSeconds(SLOW_ADMISSION_WARN_NANOS),
              permits.availablePermits(),
              permits.getQueueLength());
        }
        if (closed.getAsBoolean()) {
          throw new RejectedExecutionException("metadata I/O executor is closed");
        }
        if (permits.tryAcquire(pollMillis, TimeUnit.MILLISECONDS)) {
          rejectIfClosed(permits, closed);
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    }
  }

  /** The one terminal-state owner for a dispatched admitted call. */
  private static final class AdmittedTask<T> extends FutureTask<T> {
    private final Semaphore permits;
    private final Supplier<T> operation;
    private final BooleanSupplier cancelled;
    private final BooleanSupplier closed;
    private final boolean rejectCancelledStart;
    private final FailureMessages messages;
    private final PropagatedContext context;
    private final ThreadPoolExecutor owningPool;
    private final Thread submissionThread;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean operationStarted = new AtomicBoolean();
    private final AtomicBoolean permitReleased = new AtomicBoolean();
    private volatile Thread runner;

    AdmittedTask(
        Semaphore permits,
        Supplier<T> operation,
        BooleanSupplier cancelled,
        BooleanSupplier closed,
        boolean rejectCancelledStart,
        FailureMessages messages,
        Executor executor) {
      super(() -> null);
      this.permits = permits;
      this.operation = operation;
      this.cancelled = cancelled;
      this.closed = closed;
      this.rejectCancelledStart = rejectCancelledStart;
      this.messages = messages;
      this.context = PropagatedContext.capture();
      this.owningPool = executor instanceof ThreadPoolExecutor pool ? pool : null;
      this.submissionThread = Thread.currentThread();
    }

    @Override
    public void run() {
      if (Thread.currentThread() == submissionThread) {
        setException(
            new IllegalArgumentException("blocking call executor must dispatch asynchronously"));
        releasePermit();
        return;
      }
      running.set(true);
      runner = Thread.currentThread();
      try {
        if (isCancelled()) {
          return;
        }
        operationStarted.set(true);
        if (isCancelled() || closed.getAsBoolean()) {
          throw new CancellationException(RUNTIME_CLOSED);
        }
        if (rejectCancelledStart && cancelled.getAsBoolean()) {
          throw new CancellationException(messages.cancellation());
        }
        set(context.supply(operation));
      } catch (Throwable failure) {
        if (failure instanceof Error) {
          LOG.errorf(failure, "JVM-level failure in a metadata I/O call");
        } else if (isCancelled() && !causedByInterruption(failure)) {
          LOG.warnf(failure, "metadata I/O call failed after its caller abandoned it");
        }
        setException(failure);
      } finally {
        runner = null;
        releasePermit();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(false);
      if (cancelled && mayInterruptIfRunning) {
        Thread currentRunner = runner;
        if (currentRunner != null) {
          currentRunner.interrupt();
        }
      }
      return cancelled;
    }

    @Override
    protected void done() {
      if (isCancelled() && !running.get()) {
        if (owningPool != null) {
          owningPool.remove(this);
        }
        releasePermit();
      }
    }

    void releasePermit() {
      if (permitReleased.compareAndSet(false, true)) {
        permits.release();
      }
    }
  }

  private static boolean causedByInterruption(Throwable failure) {
    int depth = 0;
    for (Throwable current = failure;
        current != null && current != current.getCause() && depth++ < MAX_CAUSE_DEPTH;
        current = current.getCause()) {
      if (current instanceof InterruptedException || current instanceof CancellationException) {
        return true;
      }
    }
    return false;
  }
}
