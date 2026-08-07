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
 * <p>Admission transfers to the dispatched task and remains held until its callable returns. A
 * caller that cancels interrupts the worker when possible and returns immediately; an
 * interruption-insensitive downstream call still owns its admission slot, bounding retained work.
 */
final class CancellableCallRunner {

  private static final Logger LOG = Logger.getLogger(CancellableCallRunner.class);

  /** How far to walk a cause chain before giving up; guards against a legal cause cycle. */
  private static final int MAX_CAUSE_DEPTH = 32;

  /** How long a single caller may wait for admission before it is worth naming in the log. */
  private static final long SLOW_ADMISSION_WARN_NANOS = TimeUnit.SECONDS.toNanos(30);

  /**
   * How often a non-cancellable caller re-checks for runtime closure. Shutdown-scale, not
   * request-scale: this wait has no cancellation to observe.
   */
  private static final long CLOSURE_POLL_MILLIS = 250;

  /**
   * How often an admitted call re-reads its cancellation signal. Deliberately coarser than the
   * fan-out's per-batch budget: that one is paid once by a scheduler thread, this is paid by every
   * in-flight call and every thread parked on admission, so borrowing it multiplies wakeups by the
   * in-flight count. A store round trip is milliseconds at best, so this is not observable.
   */
  private static final long ADMITTED_CANCELLATION_POLL_MILLIS = 25;

  // Runtime closure reaches the caller as two types by design: RejectedExecutionException when it
  // is observed before admission is granted (nothing was submitted), CancellationException with
  // RUNTIME_CLOSED once the call is in flight. A caller mapping these to RPC statuses must handle
  // both, or a shutdown produces spurious INTERNAL alongside the clean cancellations.
  /**
   * A signal that never reports cancellation. Local rather than {@code BoundedFanout}'s: this class
   * needs only the behaviour, and borrowing that constant closes a dependency cycle back into the
   * fan-out. Do not pass this instance to {@code BoundedFanout} — its scheduler recognises its own
   * constant by identity to take a non-polling wait, and would not recognise this one.
   */
  private static final BooleanSupplier NEVER_CANCELLED = () -> false;

  static final String RUNTIME_CLOSED = "metadata I/O executor closed while the call was in flight";

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
  static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation) {
    // Allocate the lease BEFORE acquiring: after acquire() a permit exists that only the lease can
    // return, so an allocation Error between the two would consume it for the life of the process.
    PermitLease permitLease = new PermitLease(permits);
    acquire(permits, cancelled, closed, messages, onSaturation, ADMITTED_CANCELLATION_POLL_MILLIS);
    SubmittedCallHandle<T> submitted =
        submit(executor, permitLease, operation, cancelled, closed, true, messages);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    return awaitOutcome(
        result,
        lifecycle,
        messages,
        () ->
            cancelled.getAsBoolean()
                ? messages.cancellation()
                : closed.getAsBoolean() ? RUNTIME_CLOSED : null,
        ADMITTED_CANCELLATION_POLL_MILLIS);
  }

  /**
   * Run {@code operation} off-thread under the supplied admission semaphore when the caller has no
   * cancellation signal. Admission and completion have no deadline, keeping blocking store work off
   * virtual planning threads without changing synchronous completion semantics. Interruption
   * cancels the submitted task and returns {@link CancellationException}; a downstream call that
   * ignores interruption retains its permit until it truly exits.
   */
  static <T> T callWithoutCancellation(
      Executor executor,
      Semaphore permits,
      BooleanSupplier closed,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation) {
    // Allocated before acquiring, for the reason given in call(...).
    PermitLease permitLease = new PermitLease(permits);
    acquire(permits, NEVER_CANCELLED, closed, messages, onSaturation, CLOSURE_POLL_MILLIS);

    SubmittedCallHandle<T> submitted =
        submit(executor, permitLease, operation, NEVER_CANCELLED, closed, false, messages);
    CompletableFuture<T> result = submitted.result;
    CallLifecycle lifecycle = submitted.lifecycle;
    return awaitOutcome(
        result,
        lifecycle,
        messages,
        () -> closed.getAsBoolean() ? RUNTIME_CLOSED : null,
        CLOSURE_POLL_MILLIS);
  }

  /** Caller-facing cancellation outcomes for a dispatched blocking call. */
  record FailureMessages(String label, String cancellation, String interruption) {
    public FailureMessages {
      java.util.Objects.requireNonNull(label, "label");
      java.util.Objects.requireNonNull(cancellation, "cancellation");
      java.util.Objects.requireNonNull(interruption, "interruption");
    }

    /** Outcome sentences with no distinct operation label. */
    FailureMessages(String cancellation, String interruption) {
      this("metadata-io", cancellation, interruption);
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
  static void cancelDiscardedTasks(List<Runnable> discardedTasks) {
    for (Runnable task : discardedTasks) {
      if (task instanceof SubmittedCall submitted) {
        submitted.cancel(false);
      }
    }
  }

  /**
   * Resolve an abort against a possibly-finishing call. {@code result} is the arbiter: completing
   * it exceptionally succeeds only if the operation had not already completed, so a value or store
   * failure that lands in the same instant wins and is drained instead. Checking {@code isDone}
   * first cannot do this — the operation can complete between that read and the cancel.
   */
  private static <T> T abort(
      CompletableFuture<T> result,
      CallLifecycle lifecycle,
      FailureMessages messages,
      String reason) {
    if (result.completeExceptionally(new CancellationException(reason))) {
      lifecycle.cancel();
      throw new CancellationException(reason);
    }
    return drain(result, lifecycle, messages);
  }

  /** Whether a cancelled call's failure is just its own interruption surfacing. */
  private static boolean causedByInterruption(Throwable failure, boolean cancelledByCaller) {
    if (!cancelledByCaller) {
      return false;
    }
    // Bounded rather than cycle-detecting: getCause() never returns the throwable itself (that
    // value is the "not initialized" sentinel and reads back as null), but a multi-hop cycle is
    // legal and would spin a pool thread forever. Real chains are far shorter than this.
    Throwable t = failure;
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
      if (t instanceof InterruptedException || t instanceof CancellationException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Wait for a call to finish, abandoning it when {@code abortReason} yields one.
   *
   * <p>One loop for both entry points: they differ only in what counts as an abort and how often to
   * look, and two copies drift.
   *
   * <p>Completion always wins: {@code isDone} is checked first, and {@link #abort} arbitrates on
   * {@code completeExceptionally} so a value or store failure landing in the same instant is
   * drained rather than replaced.
   */
  private static <T> T awaitOutcome(
      CompletableFuture<T> result,
      CallLifecycle lifecycle,
      FailureMessages messages,
      Supplier<String> abortReason,
      long pollMillis) {
    try {
      while (true) {
        if (result.isDone()) {
          return drain(result, lifecycle, messages);
        }
        String reason = abortReason.get();
        if (reason != null) {
          return abort(result, lifecycle, messages, reason);
        }
        try {
          return result.get(pollMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException stillRunning) {
          // Look again.
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
   * Hand back a permit taken after closure latched, and reject. Shared by both acquisition points.
   */
  private static void rejectIfClosed(Semaphore permits, BooleanSupplier closed) {
    if (closed.getAsBoolean()) {
      permits.release();
      throw new RejectedExecutionException("metadata I/O executor is closed");
    }
  }

  /** Take the outcome of an already-completed call. */
  private static <T> T drain(
      CompletableFuture<T> result, CallLifecycle lifecycle, FailureMessages messages) {
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

  /** Acquire admission in cancellable polling intervals without losing interrupt semantics. */
  private static void acquire(
      Semaphore permits,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      FailureMessages messages,
      Runnable onSaturation,
      long pollMillis) {
    try {
      // An already-interrupted caller must not start metadata I/O: the fast path below can
      // succeed outright, and reading a quickly-completed future never blocks, so the call would
      // run and hold a permit for a thread whose owner has already been unwound.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      // Skip the fast path when anyone is queued, so an arrival does not take a free permit
      // without ever joining the queue. This narrows barging; it does not remove it. The
      // semaphore is non-fair and the timed tryAcquire below barges too —
      // tryAcquireSharedNanos attempts the acquire before enqueuing — so under sustained
      // saturation a waiter still has no bounded tail. Accepted: fairness would forbid barging
      // outright and cost throughput on the one chokepoint for all metadata I/O, and every
      // waiter here re-enqueues on each poll regardless.
      boolean sawCeilingFull;
      if (permits.getQueueLength() == 0) {
        if (permits.tryAcquire()) {
          // Hand the permit back rather than retire it: the task's own closure check stops the
          // body running, but nothing else returns a permit taken after the latch.
          rejectIfClosed(permits, closed);
          return;
        }
        // The fast path was attempted and failed, so the ceiling was full at that instant. Reading
        // availablePermits() again here would miss it whenever a holder releases in between.
        sawCeilingFull = true;
      } else {
        // Skipped for queued waiters rather than a full ceiling, so ask directly.
        sawCeilingFull = permits.availablePermits() == 0;
      }
      // Deliberately no cancellation check here, unlike the polling loop. The dispatched task
      // re-checks under rejectCancelledStart, so an already-cancelled caller runs nothing either
      // way; adding one would make the caller fail before the operation is dispatched at all, which
      // removes the window in which a completed store failure can still beat a racing cancellation.
      // Deliberate but unpinned: no test forbids adding the check. The cost of the asymmetry is one
      // wasted permit and worker hop under a cancellation burst.
      // A non-empty queue is not the same as no permits: waiters re-enqueue on every timed poll,
      // so there is a recurring window where the queue is non-empty while a permit is free. Only
      // count an arrival that genuinely found the ceiling full, or the metric drifts on non-events.
      // Past this point the ceiling, not the store, is what the caller is waiting on. Reported once
      // per call; the pool owner decides what to do with it.
      try {
        if (sawCeilingFull) {
          onSaturation.run();
        }
      } catch (RuntimeException sinkFailure) {
        // Accounting must never fail the call it is accounting for — least of all here, where it
        // only runs under saturation.
        LOG.warnf(sinkFailure, "metadata I/O saturation sink failed");
      }
      long waitingSinceNanos = System.nanoTime();
      boolean reportedSlowWait = false;
      while (true) {
        throwIfCancelled(cancelled, messages.cancellation());
        if (!reportedSlowWait
            && System.nanoTime() - waitingSinceNanos > SLOW_ADMISSION_WARN_NANOS) {
          // No deadline is imposed — any ceiling low enough to catch a wedged store would also
          // abort legitimately slow reads. But a wait this long means the ceiling, not the store,
          // is what the caller is stuck behind, and the gauges alone cannot say which call. Say it
          // once per call.
          reportedSlowWait = true;
          LOG.warnf(
              "%s has waited over %ds for metadata I/O admission (%d free, %d waiting)",
              messages.label(),
              TimeUnit.NANOSECONDS.toSeconds(SLOW_ADMISSION_WARN_NANOS),
              permits.availablePermits(),
              permits.getQueueLength());
        }
        // A closed runtime releases no further permits, so waiting on one never ends. Nothing was
        // admitted here, so this is a rejected submission rather than a cancelled call.
        if (closed.getAsBoolean()) {
          throw new RejectedExecutionException("metadata I/O executor is closed");
        }
        if (permits.tryAcquire(pollMillis, TimeUnit.MILLISECONDS)) {
          // Same re-check as the fast path: closure can latch inside the poll window, and a permit
          // taken here would otherwise dispatch onto an already shut-down pool, turning an admitted
          // call into an AbortPolicy rejection.
          rejectIfClosed(permits, closed);
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
      PermitLease permitLease,
      Supplier<T> operation,
      BooleanSupplier cancelled,
      BooleanSupplier closed,
      boolean rejectCancelledStart,
      FailureMessages messages) {
    // The caller holds the permit and hands in the lease that owns it (minted before the acquire,
    // so no window exists where a permit is held by no lease). Any failure here —
    // PropagatedContext.capture(), an allocation Error under the same memory pressure that makes
    // fan-out likely, or executor rejection — releases it instead of permanently shrinking the
    // semaphore. The lease is idempotent, so the task's own finally still releases exactly once on
    // the success path.
    try {
      PropagatedContext context = PropagatedContext.capture();
      CompletableFuture<T> result = new CompletableFuture<>();
      CallLifecycle lifecycle = new CallLifecycle();
      SubmittedCall task =
          new SubmittedCall(
              () -> {
                lifecycle.operationStarted.set(true);
                // A hung store client pins its permit with no way to identify it: every worker is
                // named floecat-metadata-io-<n>. Carry the operation label on the thread name so a
                // thread dump names the call that wedged the ceiling.
                Thread worker = Thread.currentThread();
                String idleName = worker.getName();
                boolean renamed = false;
                try {
                  // Inside the try: admission has already transferred to this thread, so anything
                  // that throws from here on — including an OutOfMemoryError building the name —
                  // must still reach the finally that releases the permit. Renaming outside it
                  // leaked a permit permanently and shrank the process-wide ceiling.
                  // setName is a native call on a live platform thread, so this would be two per
                  // round trip on a hot path, churning a name profilers and log aggregation treat
                  // as stable.
                  //
                  // Skipped whenever the label adds nothing to the worker's own name — which is
                  // every call today, since the only label in existence is the default and the
                  // workers are already named for it. It starts firing when a caller supplies a
                  // distinguishing label through the three-arg FailureMessages.
                  renamed = !idleName.contains(messages.label());
                  if (renamed) {
                    worker.setName(idleName + " " + messages.label());
                  }
                  // Closure is checked for both entry points: a task dispatched in the window
                  // between admission and shutdown must not start a fresh round trip.
                  if (closed.getAsBoolean()) {
                    throw new CancellationException(RUNTIME_CLOSED);
                  }
                  if (rejectCancelledStart) {
                    if (lifecycle.cancellationRequested.get()) {
                      throw new CancellationException(messages.cancellation());
                    }
                    throwIfCancelled(cancelled, messages.cancellation());
                  }
                  result.complete(context.supply(operation));
                } catch (Throwable failure) {
                  // Two ways nobody is left to see this: the result was already completed (the
                  // caller took a cancellation and moved on), or the caller cancelled while the
                  // operation ran and never waited for the result. Completing it succeeds in the
                  // second case, so the return value alone does not detect abandonment.
                  boolean cancelledByCaller = lifecycle.cancellationRequested.get();
                  boolean unobserved = !result.completeExceptionally(failure) || cancelledByCaller;
                  if (failure instanceof Error) {
                    // FutureTask.run routes a Throwable to setException, and nothing reads this
                    // task's own outcome, so no UncaughtExceptionHandler will ever see a VM error
                    // raised in here. This log is the only record.
                    LOG.errorf(failure, "JVM-level failure in a metadata I/O call");
                  } else if (unobserved && !causedByInterruption(failure, cancelledByCaller)) {
                    // A cancelled call's operation almost always throws because the cancel
                    // interrupted it. Logging that would put a WARN on every cancelled request and
                    // bury the case this exists for: a genuine store failure nobody is left to see.
                    LOG.warnf(failure, "metadata I/O call failed after its caller abandoned it");
                  }
                } finally {
                  // Nested so the release is unconditional: a throw from setName would otherwise
                  // skip it and retire a permit for the life of the process.
                  try {
                    if (renamed) {
                      worker.setName(idleName);
                    }
                  } finally {
                    permitLease.release();
                  }
                }
              },
              result,
              lifecycle,
              permitLease,
              messages,
              // Tombstone removal needs the raw pool. A lambda, a ManagedExecutor, or one of
              // the JDK's delegating wrappers (what Executors.newSingleThreadExecutor returns) is
              // not statically a ThreadPoolExecutor, so done() cannot evict a
              // cancelled-while-queued
              // task: bursts then fill the bounded queue and a later admitted call fails with
              // RejectedExecutionException. Production passes newBoundedDaemonPool's raw
              // ThreadPoolExecutor, and these entry points are package-private so that stays
              // enforceable.
              executor instanceof ThreadPoolExecutor pool ? pool : null,
              Thread.currentThread());
      lifecycle.task = task;
      // Allocate the handle BEFORE handing the task to the executor: once execute() returns, the
      // task may already be running and owns the lease. An allocation Error after that point would
      // reach the catch below and release admission out from under live store I/O — letting another
      // call in past the ceiling, under exactly the memory pressure this path exists to survive.
      // Nothing fallible may sit between execute() and the return.
      SubmittedCallHandle<T> handle = new SubmittedCallHandle<>(result, lifecycle);
      // Async dispatch is enforced by the submissionThread guard at the top of
      // SubmittedCall.run(); the FutureTask provides cancellation ownership and the
      // executor-discard cleanup in done().
      executor.execute(task);
      return handle;
    } catch (RuntimeException | Error failure) {
      permitLease.release();
      throw failure;
    }
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
        // cancellationRequested marks a cancel issued by CallLifecycle.cancel, i.e. the caller's
        // own request. Anything else discarded this task — shutdownNow draining the queue — and
        // reporting that as a request cancellation misattributes the cause.
        String reason =
            lifecycle.cancellationRequested.get() ? messages.cancellation() : RUNTIME_CLOSED;
        result.completeExceptionally(new CancellationException(reason));
        permitLease.release();
      }
    }
  }
}
