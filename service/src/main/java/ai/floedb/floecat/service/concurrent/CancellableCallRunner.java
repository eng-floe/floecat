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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Runs one blocking call behind process-wide admission while preserving prompt caller cancellation.
 *
 * <p>Admission transfers to the dispatched task and remains held until its callable returns. A
 * caller that cancels interrupts the worker when possible and returns immediately; an
 * interruption-insensitive downstream call still owns its admission slot, bounding retained work.
 */
public final class CancellableCallRunner {

  private static final long CANCELLATION_POLL_MILLIS = 10;

  private CancellableCallRunner() {}

  /**
   * Run {@code operation} under {@code permits}, polling {@code cancelled} while awaiting both
   * admission and completion. The operation receives the caller's propagated request context.
   *
   * <p>Returns the operation result, propagates operation and executor failures unchanged, and
   * throws {@link CancellationException} when cancellation or interruption wins. Admission remains
   * held until the operation exits, including when the caller has already returned.
   */
  public static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      String cancellationMessage,
      String interruptionMessage) {
    acquire(permits, cancelled, cancellationMessage, interruptionMessage);
    SubmittedCallHandle<T> submitted =
        submit(
            executor,
            permits,
            operation,
            cancelled,
            true,
            cancellationMessage,
            TimeoutListener.NOOP);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    try {
      while (true) {
        if (cancelled.getAsBoolean()) {
          lifecycle.cancel();
          throw new CancellationException(cancellationMessage);
        }
        try {
          return result.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
          // A bounded wait lets the caller interrupt a stalled operation on cancellation.
        } catch (InterruptedException e) {
          lifecycle.cancel();
          Thread.currentThread().interrupt();
          throw new CancellationException(interruptionMessage);
        } catch (ExecutionException e) {
          rethrow(e.getCause());
        }
      }
    } catch (CancellationException e) {
      lifecycle.cancel();
      throw e;
    }
  }

  /**
   * Run a call with no caller cancellation signal on {@code executor}, blocking for fair admission
   * and its result until {@code timeout}.
   *
   * <p>This keeps potentially carrier-pinning store calls off a virtual planning thread while
   * preserving legacy synchronous completion semantics. On timeout the caller is released and the
   * task is interrupted, but admission remains held until an interruption-insensitive operation
   * truly exits. Returns the operation result and propagates operation and executor failures
   * unchanged; throws {@link CallTimeoutException} when admission or completion outlives the
   * supplied timeout, and {@link CancellationException} if the waiting thread is interrupted.
   */
  public static <T> T callUncancellable(
      Executor executor,
      Semaphore permits,
      Supplier<T> operation,
      long timeout,
      TimeUnit timeoutUnit,
      String cancellationMessage,
      String interruptionMessage,
      String timeoutMessage) {
    return callUncancellable(
        executor,
        permits,
        operation,
        timeout,
        timeoutUnit,
        cancellationMessage,
        interruptionMessage,
        timeoutMessage,
        TimeoutListener.NOOP);
  }

  /**
   * Variant of {@link #callUncancellable(Executor, Semaphore, Supplier, long, TimeUnit, String,
   * String, String)} that reports calls which outlive their caller's timeout.
   *
   * <p>The listener is notified once when the caller times out and once when the underlying
   * callable finally exits (or is discarded before it starts). It lets an owner expose retained
   * admission as an operational gauge without ever releasing capacity while I/O is still live.
   */
  public static <T> T callUncancellable(
      Executor executor,
      Semaphore permits,
      Supplier<T> operation,
      long timeout,
      TimeUnit timeoutUnit,
      String cancellationMessage,
      String interruptionMessage,
      String timeoutMessage,
      TimeoutListener timeoutListener) {
    long timeoutNanos = timeoutUnit.toNanos(timeout);
    long startedNanos = System.nanoTime();
    try {
      if (!permits.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS)) {
        throw new CallTimeoutException(timeoutMessage);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(interruptionMessage);
    }

    SubmittedCallHandle<T> submitted =
        submit(
            executor, permits, operation, () -> false, false, cancellationMessage, timeoutListener);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    try {
      long remainingNanos = timeoutNanos - (System.nanoTime() - startedNanos);
      if (remainingNanos <= 0) {
        lifecycle.timedOut();
        lifecycle.cancel();
        throw new CallTimeoutException(timeoutMessage);
      }
      return result.get(remainingNanos, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      lifecycle.cancel();
      Thread.currentThread().interrupt();
      throw new CancellationException(interruptionMessage);
    } catch (TimeoutException e) {
      // The callable may ignore interruption. Keep its permit until it really returns so timed-out
      // callers cannot cause unbounded live store I/O; this only releases a caller thread.
      lifecycle.timedOut();
      lifecycle.cancel();
      throw new CallTimeoutException(timeoutMessage, e);
    } catch (ExecutionException e) {
      rethrow(e.getCause());
      throw new AssertionError("rethrow must not return");
    }
  }

  /** A caller-visible timeout for bounded admission or completion of a metadata operation. */
  public static final class CallTimeoutException extends RuntimeException {
    CallTimeoutException(String message) {
      super(message);
    }

    CallTimeoutException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Receives lifecycle notifications for a call which outlives its caller timeout. */
  public interface TimeoutListener {
    TimeoutListener NOOP =
        new TimeoutListener() {
          @Override
          public void timedOut() {}

          @Override
          public void finished() {}
        };

    /** The caller timed out while the callable may still own an admission permit. */
    void timedOut();

    /** A callable previously reported as timed out has finally exited or been discarded. */
    void finished();
  }

  /**
   * Complete and release calls returned from {@link
   * java.util.concurrent.ExecutorService#shutdownNow}.
   *
   * <p>A queued call has already acquired its admission permit, but its callable (and therefore its
   * normal {@code finally}) will never run. Cancelling it here returns that permit and unblocks its
   * caller instead of leaving either stranded during application shutdown.
   */
  public static void cancelDiscardedTasks(List<Runnable> discardedTasks) {
    for (Runnable task : discardedTasks) {
      if (task instanceof SubmittedCall submitted) {
        submitted.cancel(false);
      }
    }
  }

  private static void acquire(
      Semaphore permits,
      BooleanSupplier cancelled,
      String cancellationMessage,
      String interruptionMessage) {
    try {
      while (true) {
        throwIfCancelled(cancelled, cancellationMessage);
        if (permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(interruptionMessage);
    }
  }

  private static void throwIfCancelled(BooleanSupplier cancelled, String message) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException(message);
    }
  }

  /**
   * Capture context and submit an admitted call, retaining its permit until the callable exits or
   * the executor discards it. A cancellable caller asks the worker to reject a task that starts
   * after cancellation; an uncancellable caller executes even if its waiting thread later times
   * out.
   */
  private static <T> SubmittedCallHandle<T> submit(
      Executor executor,
      Semaphore permits,
      Supplier<T> operation,
      BooleanSupplier cancelled,
      boolean rejectCancelledStart,
      String cancellationMessage,
      TimeoutListener timeoutListener) {
    PropagatedContext context = PropagatedContext.capture();
    CompletableFuture<T> result = new CompletableFuture<>();
    CallLifecycle lifecycle = new CallLifecycle(timeoutListener);
    PermitLease permitLease = new PermitLease(permits);
    SubmittedCall task =
        new SubmittedCall(
            () -> {
              lifecycle.operationStarted.set(true);
              try {
                if (rejectCancelledStart) {
                  if (lifecycle.cancellationRequested.get()) {
                    throw new CancellationException(cancellationMessage);
                  }
                  throwIfCancelled(cancelled, cancellationMessage);
                }
                result.complete(context.supply(operation));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              } finally {
                lifecycle.operationFinished();
                permitLease.release();
              }
            },
            result,
            lifecycle,
            permitLease,
            cancellationMessage);
    lifecycle.task = task;
    try {
      // An explicit FutureTask prevents ForkJoinPool from help-running a submitted lambda inline
      // on the caller, which would bypass this method's cancellation polling loop.
      executor.execute(task);
    } catch (RuntimeException | Error submissionFailure) {
      permitLease.release();
      throw submissionFailure;
    }
    return new SubmittedCallHandle<>(result, lifecycle);
  }

  /** Result and cancellation ownership for one submitted callable. */
  private static final class SubmittedCallHandle<T> {
    private final CompletableFuture<T> result;
    private final CallLifecycle lifecycle;

    private SubmittedCallHandle(CompletableFuture<T> result, CallLifecycle lifecycle) {
      this.result = result;
      this.lifecycle = lifecycle;
    }
  }

  /** Owns cancellation state for one submitted callable. */
  private static final class CallLifecycle {
    private final AtomicBoolean operationStarted = new AtomicBoolean();
    private final AtomicBoolean runnerInstalled = new AtomicBoolean();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicBoolean timedOut = new AtomicBoolean();
    private final AtomicBoolean operationFinished = new AtomicBoolean();
    private final AtomicBoolean finishedAfterTimeout = new AtomicBoolean();
    private final TimeoutListener timeoutListener;
    private volatile FutureTask<?> task;

    CallLifecycle() {
      this(TimeoutListener.NOOP);
    }

    CallLifecycle(TimeoutListener timeoutListener) {
      this.timeoutListener = timeoutListener;
    }

    void timedOut() {
      if (timedOut.compareAndSet(false, true)) {
        timeoutListener.timedOut();
        reportFinishedAfterTimeout();
      }
    }

    void operationFinished() {
      operationFinished.set(true);
      reportFinishedAfterTimeout();
    }

    private void reportFinishedAfterTimeout() {
      if (timedOut.get()
          && operationFinished.get()
          && finishedAfterTimeout.compareAndSet(false, true)) {
        timeoutListener.finished();
      }
    }

    void cancel() {
      cancellationRequested.set(true);
      // FutureTask atomically owns its runner while it is executing. Its cancellation path cannot
      // interrupt a platform thread after that task has completed and been returned to the pool.
      task.cancel(true);
    }
  }

  /** A permit lease that remains held until the dispatched callable truly exits. */
  private static final class PermitLease {
    private final Semaphore permits;
    private final AtomicBoolean released = new AtomicBoolean();

    PermitLease(Semaphore permits) {
      this.permits = permits;
    }

    void release() {
      if (released.compareAndSet(false, true)) {
        permits.release();
      }
    }
  }

  /** FutureTask variant that cleans up admission when an executor discards it before execution. */
  private static final class SubmittedCall extends FutureTask<Void> {
    private final CompletableFuture<?> result;
    private final CallLifecycle lifecycle;
    private final PermitLease permitLease;
    private final String cancellationMessage;

    SubmittedCall(
        Runnable runnable,
        CompletableFuture<?> result,
        CallLifecycle lifecycle,
        PermitLease permitLease,
        String cancellationMessage) {
      super(runnable, null);
      this.result = result;
      this.lifecycle = lifecycle;
      this.permitLease = permitLease;
      this.cancellationMessage = cancellationMessage;
    }

    @Override
    public void run() {
      // Mark this before FutureTask installs its runner. Cancellation can otherwise observe a
      // cancelled task between FutureTask's runner CAS and the submitted Runnable's first line,
      // incorrectly classify live work as executor-discarded, and release its admission early.
      lifecycle.runnerInstalled.set(true);
      super.run();
      // A cancellation before FutureTask invokes the submitted Runnable leaves no runnable
      // finally block to release the admission. At this point run() has returned, so the task did
      // not enter the downstream operation and is safe to discard.
      if (!lifecycle.operationStarted.get()) {
        if (isCancelled()) {
          result.completeExceptionally(new CancellationException(cancellationMessage));
        }
        lifecycle.operationFinished();
        permitLease.release();
      }
    }

    @Override
    protected void done() {
      if (isCancelled() && !lifecycle.runnerInstalled.get()) {
        result.completeExceptionally(new CancellationException(cancellationMessage));
        lifecycle.operationFinished();
        permitLease.release();
      }
    }
  }

  private static void rethrow(Throwable failure) {
    if (failure instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new IllegalStateException("unexpected checked exception from cancellable call", failure);
  }
}
