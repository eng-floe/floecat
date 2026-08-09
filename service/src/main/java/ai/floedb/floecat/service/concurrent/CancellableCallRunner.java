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
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final Executor NESTED_EXECUTOR =
      command -> Thread.ofVirtual().name("floecat-metadata-io-nested").start(command);
  private static final ThreadLocal<AdmissionLease> CURRENT_ADMISSION = new ThreadLocal<>();

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
    return call(
        executor,
        permits,
        cancelled,
        closed,
        operation,
        messages,
        onSaturation,
        PropagatedContext::capture);
  }

  static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation,
      Supplier<PropagatedContext> captureContext) {
    PropagatedContext context = captureContext.get();
    AdmissionLease admission =
        acquire(
            permits, cancelled, closed, messages, onSaturation, ADMITTED_CANCELLATION_POLL_MILLIS);
    AdmittedTask<T> task =
        newTask(admission, operation, cancelled, closed, true, messages, context, executor);
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
    return callWithoutCancellation(
        executor, permits, closed, operation, messages, onSaturation, PropagatedContext::capture);
  }

  static <T> T callWithoutCancellation(
      Executor executor,
      Semaphore permits,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation,
      Supplier<PropagatedContext> captureContext) {
    PropagatedContext context = captureContext.get();
    AdmissionLease admission =
        acquire(permits, NEVER_CANCELLED, closed, messages, onSaturation, CLOSURE_POLL_MILLIS);
    AdmittedTask<T> task =
        newTask(admission, operation, NEVER_CANCELLED, closed, false, messages, context, executor);
    submit(executor, task);
    return awaitOutcome(
        task, messages, () -> closed.getAsBoolean() ? RUNTIME_CLOSED : null, CLOSURE_POLL_MILLIS);
  }

  /**
   * Run nested work under a permit that its caller already owns.
   *
   * <p>The nested task gets another reference to the same admission lease rather than acquiring a
   * second permit. Its caller can therefore abandon the wait promptly while the lease remains held
   * until the downstream operation has actually stopped.
   */
  static <T> T callAlreadyAdmitted(
      AdmissionLease parentAdmission,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages) {
    PropagatedContext context = PropagatedContext.capture();
    AdmissionLease childAdmission = parentAdmission.fork();
    AdmittedTask<T> task =
        newTask(
            childAdmission, operation, cancelled, closed, true, messages, context, NESTED_EXECUTOR);
    submit(NESTED_EXECUTOR, task);
    try {
      return awaitOutcome(
          task,
          messages,
          () ->
              cancelled.getAsBoolean()
                  ? messages.cancellation()
                  : closed.getAsBoolean() ? RUNTIME_CLOSED : null,
          ADMITTED_CANCELLATION_POLL_MILLIS);
    } catch (CancellationException cancellation) {
      if (task.isCancelled() && !task.isAdmissionReleased()) {
        parentAdmission.invalidateLineage();
      }
      throw cancellation;
    }
  }

  static AdmissionLease currentAdmission() {
    return CURRENT_ADMISSION.get();
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
      // An executor is allowed to throw after handing the task to a worker. Cancellation prevents
      // a not-yet-started task from running; a task that already passed its cancellation gate
      // retains its admission until its own finally releases it.
      task.cancel(false);
      throw failure;
    }
  }

  private static <T> AdmittedTask<T> newTask(
      AdmissionLease admission,
      Supplier<T> operation,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      boolean rejectCancelledStart,
      FailureMessages messages,
      PropagatedContext context,
      Executor executor) {
    try {
      return new AdmittedTask<>(
          admission,
          operation,
          cancelled,
          closed,
          rejectCancelledStart,
          messages,
          context,
          executor);
    } catch (RuntimeException | Error failure) {
      admission.release();
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
        } catch (CancellationException taskCancelled) {
          throw new CancellationException(RUNTIME_CLOSED);
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

  /** Acquire admission in cancellable polling intervals without losing interrupt semantics. */
  private static AdmissionLease acquire(
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
          return takeAcquiredPermit(permits, closed);
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
          return takeAcquiredPermit(permits, closed);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    }
  }

  /** Turn a raw semaphore permit into a lease before any fallible post-acquisition work. */
  private static AdmissionLease takeAcquiredPermit(Semaphore permits, BooleanSupplier closed) {
    AdmissionLease admission = null;
    try {
      admission = new Admission(permits).root();
      if (closed.getAsBoolean()) {
        throw new RejectedExecutionException("metadata I/O executor is closed");
      }
      return admission;
    } catch (RuntimeException | Error failure) {
      if (admission == null) {
        permits.release();
      } else {
        admission.release();
      }
      throw failure;
    }
  }

  /** A permit that remains held until every nested operation using it has genuinely returned. */
  static final class Admission {
    private final Semaphore permits;
    private final AtomicInteger references = new AtomicInteger(1);

    Admission(Semaphore permits) {
      this.permits = permits;
    }

    private void retain() {
      int current;
      do {
        current = references.get();
        if (current == 0) {
          throw new IllegalStateException("metadata I/O admission was already released");
        }
      } while (!references.compareAndSet(current, current + 1));
    }

    private void release() {
      if (references.decrementAndGet() == 0) {
        permits.release();
      }
    }

    AdmissionLease root() {
      return new AdmissionLease(this, null);
    }
  }

  /** One logical execution branch sharing an admission with its nested descendants. */
  static final class AdmissionLease {
    private final Admission admission;
    private final AdmissionLease parent;
    private final AtomicBoolean reusable = new AtomicBoolean(true);
    private final AtomicBoolean released = new AtomicBoolean();

    private AdmissionLease(Admission admission, AdmissionLease parent) {
      this.admission = admission;
      this.parent = parent;
    }

    AdmissionLease fork() {
      AdmissionLease child = new AdmissionLease(admission, this);
      admission.retain();
      return child;
    }

    boolean isReusable() {
      return reusable.get();
    }

    void invalidateLineage() {
      for (AdmissionLease current = this; current != null; current = current.parent) {
        current.reusable.set(false);
      }
    }

    void release() {
      if (released.compareAndSet(false, true)) {
        admission.release();
      }
    }
  }

  /** The one terminal-state owner for a dispatched admitted call. */
  private static final class AdmittedTask<T> extends FutureTask<T> {
    private final AdmissionLease admission;
    private final Supplier<T> operation;
    private final BooleanSupplier cancelled;
    private final BooleanSupplier closed;
    private final boolean rejectCancelledStart;
    private final FailureMessages messages;
    private final PropagatedContext context;
    private final ThreadPoolExecutor owningPool;
    private final Thread submissionThread;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean permitReleased = new AtomicBoolean();
    private volatile Thread runner;

    AdmittedTask(
        AdmissionLease admission,
        Supplier<T> operation,
        BooleanSupplier cancelled,
        BooleanSupplier closed,
        boolean rejectCancelledStart,
        FailureMessages messages,
        PropagatedContext context,
        Executor executor) {
      super(() -> null);
      this.admission = admission;
      this.operation = operation;
      this.cancelled = cancelled;
      this.closed = closed;
      this.rejectCancelledStart = rejectCancelledStart;
      this.messages = messages;
      this.context = context;
      this.owningPool = executor instanceof ThreadPoolExecutor pool ? pool : null;
      this.submissionThread = Thread.currentThread();
    }

    @Override
    public void run() {
      Thread worker = Thread.currentThread();
      if (worker == submissionThread) {
        setException(
            new IllegalArgumentException("blocking call executor must dispatch asynchronously"));
        releasePermit();
        return;
      }
      running.set(true);
      runner = worker;
      try {
        if (isCancelled()) {
          return;
        }
        if (isCancelled() || closed.getAsBoolean()) {
          throw new CancellationException(RUNTIME_CLOSED);
        }
        if (rejectCancelledStart && cancelled.getAsBoolean()) {
          throw new CancellationException(messages.cancellation());
        }
        AdmissionLease previousAdmission = CURRENT_ADMISSION.get();
        CURRENT_ADMISSION.set(admission);
        try {
          set(context.supply(operation));
        } finally {
          if (previousAdmission == null) {
            CURRENT_ADMISSION.remove();
          } else {
            CURRENT_ADMISSION.set(previousAdmission);
          }
        }
      } catch (Throwable failure) {
        if (failure instanceof Error) {
          LOG.errorf(failure, "JVM-level failure in a metadata I/O call");
        } else if (isCancelled() && !causedByInterruption(failure)) {
          LOG.warnf(failure, "metadata I/O call failed after its caller abandoned it");
        }
        setException(failure);
      } finally {
        runner = null;
        try {
          worker.setContextClassLoader(ClassLoader.getPlatformClassLoader());
        } finally {
          releasePermit();
        }
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
        admission.release();
      }
    }

    boolean isAdmissionReleased() {
      return permitReleased.get();
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
