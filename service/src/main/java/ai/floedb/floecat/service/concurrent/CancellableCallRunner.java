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
import java.util.concurrent.ThreadPoolExecutor;
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
   * held until the operation exits, including when the caller has already returned. The executor
   * must dispatch work asynchronously; an executor that invokes tasks on the submitting thread is
   * rejected before the operation starts so it cannot bypass cancellation polling. {@code
   * cancelled} is read from the caller and worker threads and must be non-blocking and thread-safe
   * (typically {@link java.util.concurrent.atomic.AtomicBoolean#get}).
   */
  public static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      FailureMessages messages) {
    acquire(permits, cancelled, messages);
    SubmittedCallHandle<T> submitted =
        submit(executor, permits, operation, cancelled, true, messages);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    try {
      while (true) {
        if (cancelled.getAsBoolean()) {
          lifecycle.cancel();
          throw new CancellationException(messages.cancellation());
        }
        try {
          return result.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
          // A bounded wait lets the caller interrupt a stalled operation on cancellation.
        } catch (InterruptedException e) {
          lifecycle.cancel();
          Thread.currentThread().interrupt();
          throw new CancellationException(messages.interruption());
        } catch (ExecutionException e) {
          throw Futures.propagate(
              e.getCause(), "unexpected checked exception from cancellable call");
        }
      }
    } catch (CancellationException e) {
      lifecycle.cancel();
      throw e;
    }
  }

  /**
   * Run {@code operation} off-thread under the supplied admission semaphore when the caller has no
   * cancellation signal. Admission and completion have no deadline, keeping blocking store work off
   * virtual planning threads without changing synchronous completion semantics. Interruption
   * cancels the submitted task and returns {@link CancellationException}; a downstream call that
   * ignores interruption retains its permit until it truly exits.
   */
  public static <T> T callWithoutCancellation(
      Executor executor, Semaphore permits, Supplier<T> operation, FailureMessages messages) {
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    }

    SubmittedCallHandle<T> submitted =
        submit(executor, permits, operation, () -> false, false, messages);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    try {
      return result.get();
    } catch (InterruptedException e) {
      lifecycle.cancel();
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    } catch (ExecutionException e) {
      throw Futures.propagate(e.getCause(), "unexpected checked exception from cancellable call");
    }
  }

  /** Caller-facing cancellation outcomes for a dispatched blocking call. */
  public record FailureMessages(String cancellation, String interruption) {
    public FailureMessages {
      java.util.Objects.requireNonNull(cancellation, "cancellation");
      java.util.Objects.requireNonNull(interruption, "interruption");
    }
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

  /** Acquire admission in cancellable polling intervals without losing interrupt semantics. */
  private static void acquire(
      Semaphore permits, BooleanSupplier cancelled, FailureMessages messages) {
    try {
      while (true) {
        throwIfCancelled(cancelled, messages.cancellation());
        if (permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
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
   * after cancellation; an uncancellable caller executes unless its waiting thread is interrupted.
   */
  private static <T> SubmittedCallHandle<T> submit(
      Executor executor,
      Semaphore permits,
      Supplier<T> operation,
      BooleanSupplier cancelled,
      boolean rejectCancelledStart,
      FailureMessages messages) {
    PropagatedContext context = PropagatedContext.capture();
    CompletableFuture<T> result = new CompletableFuture<>();
    CallLifecycle lifecycle = new CallLifecycle();
    PermitLease permitLease = new PermitLease(permits);
    SubmittedCall task =
        new SubmittedCall(
            () -> {
              lifecycle.operationStarted.set(true);
              try {
                if (rejectCancelledStart) {
                  if (lifecycle.cancellationRequested.get()) {
                    throw new CancellationException(messages.cancellation());
                  }
                  throwIfCancelled(cancelled, messages.cancellation());
                }
                result.complete(context.supply(operation));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              } finally {
                permitLease.release();
              }
            },
            result,
            lifecycle,
            permitLease,
            messages,
            executor instanceof ThreadPoolExecutor pool ? pool : null,
            Thread.currentThread());
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
    private volatile FutureTask<?> task;

    /** Atomically bind interruption to this task's current runner, never a reused pool worker. */
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
    private final FailureMessages messages;
    private final ThreadPoolExecutor owningPool;
    private final Thread submissionThread;

    SubmittedCall(
        Runnable runnable,
        CompletableFuture<?> result,
        CallLifecycle lifecycle,
        PermitLease permitLease,
        FailureMessages messages,
        ThreadPoolExecutor owningPool,
        Thread submissionThread) {
      super(runnable, null);
      this.result = result;
      this.lifecycle = lifecycle;
      this.permitLease = permitLease;
      this.messages = messages;
      this.owningPool = owningPool;
      this.submissionThread = submissionThread;
    }

    /** Install runner ownership before FutureTask exposes its cancellation transition. */
    @Override
    public void run() {
      if (Thread.currentThread() == submissionThread) {
        result.completeExceptionally(
            new IllegalArgumentException("blocking call executor must dispatch asynchronously"));
        permitLease.release();
        return;
      }
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
          result.completeExceptionally(new CancellationException(messages.cancellation()));
        }
        permitLease.release();
      }
    }

    /** Release admission when cancellation discards a task before any runner installs. */
    @Override
    protected void done() {
      if (isCancelled() && !lifecycle.runnerInstalled.get()) {
        // ThreadPoolExecutor retains cancelled FutureTasks in its work queue. Remove this
        // tombstone before recycling admission, otherwise cancellation bursts can fill the
        // bounded queue and make a later admitted call fail with RejectedExecutionException.
        if (owningPool != null) {
          owningPool.remove(this);
        }
        result.completeExceptionally(new CancellationException(messages.cancellation()));
        permitLease.release();
      }
    }
  }
}
