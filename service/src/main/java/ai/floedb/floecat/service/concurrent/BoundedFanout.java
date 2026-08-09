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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import org.jboss.logging.Logger;

/**
 * Runs independent, mostly-blocking tasks with a concurrency bound.
 *
 * <p>At most the configured number of tasks are submitted at a time. A completed task immediately
 * opens one submission slot, independent of input order. Completed results are retained by index;
 * mapping and ordered consumption deliver each contiguous input-order prefix as soon as it is
 * available. Once that prefix exposes a failure, active siblings are cancelled or abandoned and the
 * original failure returns without waiting for unrelated work.
 *
 * <p>This is orchestration only — it applies no I/O admission of its own. Its fan-out methods are
 * package-private on purpose: an I/O-admission tier is meant to wrap them, so widening them to
 * {@code public} would let a caller fan out per-unit store calls that bypass admission entirely.
 * Keep them package-private.
 *
 * <p>{@code permits} bounds concurrency, not buffered results. The window refills past a
 * still-running input-order head, so a completed later result is retained until the head lets the
 * contiguous prefix advance. Undelivered results are therefore bounded by {@code items.size()}, not
 * by {@code permits}: one slow head can hold every downstream result in heap at once. That is the
 * deliberate trade for not stalling a fast later task behind a slow earlier one — size the batch
 * (or the per-element footprint) accordingly rather than assuming a {@code permits}-sized buffer.
 *
 * <p>Return does not imply completion. Cancellation and failure <em>abandon</em> active siblings —
 * {@code Future.cancel(true)} interrupts them but does not wait — so this class can return with
 * tasks still running inside a non-interruptible store call (an S3/JDBC round trip that ignores the
 * interrupt). This class holds no admission permit of its own.
 *
 * <p><b>Executor requirement:</b> {@code executor} must be able to run submitted tasks
 * independently of the calling thread. The scheduler blocks the caller awaiting completions, so a
 * bounded executor that the caller thread <em>itself</em> occupies deadlocks — the caller holds the
 * only worker while its own submissions queue behind it, with no timeout. An unbounded or
 * virtual-thread executor is always safe, as is any pool separate from the caller's. Use one of
 * those. This cannot be detected here (the class does not know the caller's pool), so it is the
 * caller's contract.
 *
 * <p>A same-pool {@link ForkJoinPool} is <b>no exemption at all</b>, despite {@link
 * ForkJoinPool#managedBlock}. Compensation covers the <em>scheduler's</em> wait — one credit per
 * managed block, and {@code tryCompensate} often satisfies it by lowering the active count rather
 * than adding a worker. Nothing compensates for a submitted task that blocks, because that park is
 * unmanaged, and mostly-blocking tasks are this class's whole premise. So the pool stops growing
 * after a small constant and every later submission starves. Measured on a single-worker pool, with
 * the pool capping at three threads: a cancellable caller stalls permanently with two blocking
 * tasks, and {@link #NEVER_CANCELLED} stalls from three onward. Neither path is safe — the sentinel
 * merely fails one task later. Do not dispatch to the pool the caller is running on.
 *
 * <p>{@code managedBlock} is still used, and still earns its keep for the supported topology: a
 * scheduler running on an FJP worker that dispatches tasks <em>elsewhere</em> lends the pool a
 * worker for the duration of its wait. Off an FJP it degrades to an ordinary block.
 *
 * <p>{@code executor} must also <b>run or reject</b> every task it accepts, never silently drop
 * one. The scheduler's only exits from its wait are a completion notification, an observed
 * cancellation, or an interrupt: a task accepted and then discarded never notifies, so {@code
 * active} never drains and the calling thread parks. A rejection is fine (it surfaces through the
 * ordered failure path); a {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy} or {@code
 * DiscardOldestPolicy} is not, and neither is calling {@code shutdownNow()} on the executor while a
 * fan-out is in flight — that drains queued-but-unstarted tasks without running them. There is
 * deliberately no wait deadline: any ceiling low enough to catch a wedged executor would also abort
 * legitimately slow metadata reads, so this is a wiring contract rather than a timeout.
 */
final class BoundedFanout {

  private static final Logger LOG = Logger.getLogger(BoundedFanout.class);

  /**
   * The canonical "never cancels" signal, shared so callers do not each define their own — and so
   * they get the fast path: this scheduler recognizes <em>this exact instance</em> by identity and
   * blocks for the next completion instead of waking every {@value #CANCELLATION_POLL_MILLIS} ms to
   * re-read a signal that can never change. Passing an equivalent but distinct {@code () -> false}
   * takes the polling path instead: correct on a conforming executor, but it pays that wakeup cost
   * and does not share this path's incidental tolerance of a same-pool {@link ForkJoinPool} (see
   * the class javadoc — that shape is unsupported either way). Pass this constant through rather
   * than redeclaring it. Scheduling is otherwise identical: {@link #forEachOrdered} always installs
   * the cancellation-aware runtime.
   */
  static final BooleanSupplier NEVER_CANCELLED = () -> false;

  /**
   * The shared cancellation-responsiveness budget (millis): this fan-out polls cancellation on this
   * interval. Package-private so same-package callers share the one knob rather than each declaring
   * their own.
   */
  static final long CANCELLATION_POLL_MILLIS = 10;

  /**
   * Shared no-op completion barrier used by production. A completion barrier is a test seam invoked
   * with a slot's input index after its future is terminal but before the slot is enqueued, so a
   * test can hold that notification and exercise the terminal-but-unnotified window. It runs on
   * whichever thread completed the future ({@link FutureTask#done()}) — the task's worker, or the
   * scheduler thread itself when that thread calls {@code cancel(true)} — so a test must only hold
   * notifications for indices it is not itself waiting on. Tests inject their own barrier through
   * the package-private {@code forEachOrdered} overload rather than mutating process-wide state.
   */
  private static final IntConsumer NO_COMPLETION_BARRIER = index -> {};

  private BoundedFanout() {}

  /**
   * Apply {@code task} to each item on {@code executor}, at most {@code permits} running at once,
   * and return the results in input order. Each task runs under the caller's request context
   * (OpenTelemetry, engine/principal/correlation, MDC) re-established via {@link
   * PropagatedContext}, so ambient reads behave off-thread as they do on the caller's thread. A
   * task failure surfaces unwrapped — its original {@link RuntimeException} or {@link Error}, never
   * an execution-wrapper exception. The first failure reachable in input order propagates
   * immediately; active siblings are cancelled or abandoned rather than drained. {@code cancelled}
   * must be non-blocking and thread-safe; observed cancellation throws {@link
   * CancellationException} and abandons in-flight work rather than running every task to
   * completion.
   */
  static <I, O> List<O> mapOrdered(
      List<I> items,
      int permits,
      Executor executor,
      Function<I, O> task,
      BooleanSupplier cancelled) {
    List<O> results = new ArrayList<>(items.size());
    forEachOrdered(items, permits, executor, task, results::add, cancelled);
    return results;
  }

  /**
   * Cancellation-aware ordered result consumption. A consumer failure has the same ordered
   * precedence as a task failure, while submitted work is cancelled promptly on failure or when
   * cancellation is requested. {@code cancelled} may be read concurrently by the scheduler and
   * workers, so it must be non-blocking and thread-safe (typically {@link
   * java.util.concurrent.atomic.AtomicBoolean#get}). Observed cancellation throws {@link
   * CancellationException}.
   *
   * <p>{@code consumer} runs synchronously on the scheduler thread between window refills, and the
   * scheduler cannot observe cancellation while it runs — so the consumer <b>must not block</b>
   * (e.g. on downstream backpressure). A blocked consumer stalls both cancellation polling and
   * window refill until it returns; route streaming/backpressure outside the fan-out (collect here,
   * emit after) rather than inside {@code consumer}.
   */
  static <I, O> void forEachOrdered(
      List<I> items,
      int permits,
      Executor executor,
      Function<I, O> task,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled) {
    forEachOrdered(items, permits, executor, task, consumer, cancelled, NO_COMPLETION_BARRIER);
  }

  /**
   * {@link #forEachOrdered} with a per-invocation completion barrier — a test seam (see {@link
   * #NO_COMPLETION_BARRIER}) injected without touching process-wide state. Package-private: every
   * production path uses the six-arg overload’s no-op barrier.
   */
  static <I, O> void forEachOrdered(
      List<I> items,
      int permits,
      Executor executor,
      Function<I, O> task,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled,
      IntConsumer completionBarrier) {
    // Before capture() or any allocation: an invalid bound should cost nothing and name itself.
    validatePermits(permits);
    // Check even an empty batch so invalid wiring fails deterministically rather than only when
    // production data happens to contain work.
    MetadataIoRunner.rejectFanOutFromAdmittedOperation("BoundedFanout.forEachOrdered");
    List<TaskOutcome<O>> outcomes = emptyOutcomes(items.size());
    OrderedOutcomeConsumer<O> ordered = new OrderedOutcomeConsumer<>(outcomes, consumer, cancelled);
    completeAll(
        items,
        permits,
        outcomes,
        ordered,
        new CancellableTaskRuntime<>(
            executor, PropagatedContext.capture(), task, cancelled, completionBarrier));
  }

  /**
   * Submit no more than {@code permits} tasks at once, recording each outcome as it completes so a
   * fast later task immediately replenishes the window despite ordered result observation.
   */
  private static <I, O> void completeAll(
      List<I> items,
      int permits,
      List<TaskOutcome<O>> outcomes,
      OrderedOutcomeConsumer<O> ordered,
      TaskRuntime<I, O> runtime) {
    // Never more than items.size() slots are active at once, so do not let a large permits value
    // pre-allocate memory independent of the actual workload.
    List<CompletionSlot<O>> active = new ArrayList<>(Math.min(permits, items.size()));
    int next = 0;
    try {
      next = fillWindow(items, permits, next, active, outcomes, ordered, runtime);
      // Surface a failure a synchronous initial fill already made reachable before blocking on
      // take(). Reconcile cancellation first, exactly as the main loop does: the fill's last task
      // can set cancelled before returning, and an earlier recorded success must not be published
      // to a stopped stream — a reachable failure still keeps precedence over the cancellation.
      if (runtime.cancellationObserved()) {
        surfaceSubmissionFailure(cancelled(), runtime, active, outcomes, ordered);
      }
      ordered.deliverReady();
      while (!active.isEmpty()) {
        CompletionSlot<O> slot;
        try {
          slot = runtime.take(active);
        } catch (CancellationException cancellation) {
          // take observed cancellation with nothing ready in its queue. A sibling may have reached
          // a terminal failure whose notification is still pending (on the tail, fillWindow's loop
          // body did not run, so nothing reconciled it). Reconcile through the same path as every
          // other cancellation point so that reachable failure keeps precedence over the
          // cancellation instead of being masked by it.
          surfaceSubmissionFailure(cancellation, runtime, active, outcomes, ordered);
          throw cancellation; // unreachable: surfaceSubmissionFailure always throws
        }
        outcomes.set(slot.index, runtime.outcome(slot));
        active.remove(slot);
        // Refill before delivering, so a burst of completions does not shrink the live window.
        next = fillWindow(items, permits, next, active, outcomes, ordered, runtime);
        // fillWindow's beforeSubmit does this recheck whenever work remains; the final (or sole)
        // completion has no submit left to trip it, so repeat it here every iteration.
        if (runtime.cancellationObserved()) {
          surfaceSubmissionFailure(cancelled(), runtime, active, outcomes, ordered);
        }
        ordered.deliverReady();
      }
    } catch (RuntimeException | Error processingFailure) {
      runtime.afterFailure(active, processingFailure);
      throw processingFailure;
    }
  }

  /**
   * Submit inputs until the window holds {@code permits} tasks or inputs are exhausted, reconciling
   * terminal completions and stopping before the next submission once a failure is reachable in
   * input order. Reconciling between submissions keeps a synchronous executor safe — a task that
   * failed during the preceding submit is surfaced before another is started, so later work never
   * runs on the caller thread past a completed failure — and refills every slot a burst of
   * completions reopened, so the configured parallelism is not silently collapsed to one. Returns
   * the next unsubmitted index.
   */
  private static <I, O> int fillWindow(
      List<I> items,
      int permits,
      int next,
      List<CompletionSlot<O>> active,
      List<TaskOutcome<O>> outcomes,
      OrderedOutcomeConsumer<O> ordered,
      TaskRuntime<I, O> runtime) {
    // Cap this call at one window's worth of submissions. With a synchronous executor the just-
    // submitted task completes inline, so recordTerminalCompletions empties active every iteration
    // and active.size() alone would never reach permits — one call would submit the whole batch,
    // suppress incremental delivery, and rescan a growing contiguous prefix (O(N^2)). The main loop
    // reconciles and refills between deliveries, so bounding submissions per call restores both.
    int submitted = 0;
    while (submitted < permits && active.size() < permits && next < items.size()) {
      // A sibling's terminal transition happens-before its queue notification, so record terminal
      // slots directly before each submit: a failure that has completed but is not yet queued would
      // otherwise stay unreachable here and let the fill proceed past it.
      runtime.recordTerminalCompletions(active, outcomes);
      if (ordered.earliestReachableFailure() != null) {
        break;
      }
      try {
        runtime.beforeSubmit(active);
        active.add(runtime.submit(next, items.get(next)));
        next++;
        submitted++;
      } catch (RuntimeException | Error submissionFailure) {
        // A fill failure (cancellation or executor rejection) is reconciled against every terminal
        // completion, so a store failure an earlier input already produced keeps first-reachable
        // precedence over it.
        surfaceSubmissionFailure(submissionFailure, runtime, active, outcomes, ordered);
      }
    }
    return next;
  }

  /**
   * Reconcile a submission failure — from {@code beforeSubmit}/{@code submit}, during initial fill
   * or refill — against work already completed, then surface it (this method always throws). Every
   * already-terminal completion is recorded into {@code outcomes} first — consulting each future
   * directly, so a sibling that is terminal but whose queue notification has not yet landed still
   * counts — so an earlier input's completion cannot leave a later input's failure unreachable in
   * input order. A failure reachable in the contiguous prefix keeps first-failure precedence: on
   * cancellation it is thrown in place of the cancellation (no success is published after
   * cancellation); otherwise the contiguous prefix is delivered — a reachable failure throws here —
   * before the submission failure itself is surfaced.
   */
  private static <O> void surfaceSubmissionFailure(
      Throwable submissionFailure,
      TaskRuntime<?, O> runtime,
      List<CompletionSlot<O>> active,
      List<TaskOutcome<O>> outcomes,
      OrderedOutcomeConsumer<O> ordered) {
    runtime.recordTerminalCompletions(active, outcomes);
    if (submissionFailure instanceof CancellationException cancellation) {
      Throwable reachable = ordered.earliestReachableFailure();
      if (reachable instanceof RuntimeException reachableRuntime) {
        throw reachableRuntime;
      }
      if (reachable instanceof Error reachableError) {
        throw reachableError;
      }
      throw cancellation;
    }
    ordered.deliverReady();
    if (submissionFailure instanceof RuntimeException runtimeFailure) {
      throw runtimeFailure;
    }
    if (submissionFailure instanceof Error errorFailure) {
      throw errorFailure;
    }
    // Structurally enforce the always-throws contract two callers rely on (completeAll's
    // unreachable
    // rethrow, fillWindow's catch that would otherwise retry the same index). Submission failures
    // only ever arrive through catch (RuntimeException | Error), so a checked Throwable here is a
    // programming error rather than a swallow-and-retry loop.
    throw new AssertionError(
        "unexpected throwable type from a submission failure", submissionFailure);
  }

  /**
   * The task lifecycle protocol the bounded-window scheduler drives. One implementation today
   * ({@link CancellableTaskRuntime}); the interface documents the terminal-vs-notified protocol as
   * a contract rather than a two-implementation hierarchy.
   */
  private interface TaskRuntime<I, O> {
    /** Validate scheduler state before submission without adding or removing active slots. */
    void beforeSubmit(List<CompletionSlot<O>> active);

    /**
     * Whether cancellation has been observed. Checked after each completion is recorded and before
     * it is delivered, so a cancellation that flips during a task's post-I/O work is reconciled
     * even for the final/sole completion — which has no refill to trip {@link #beforeSubmit}. Must
     * not throw or mutate state (unlike {@code beforeSubmit}).
     */
    boolean cancellationObserved();

    /** Submit one indexed item and return the slot the scheduler owns until completion. */
    CompletionSlot<O> submit(int index, I item);

    /** Wait for and return one completed member of {@code active} without mutating that list. */
    CompletionSlot<O> take(List<CompletionSlot<O>> active);

    /**
     * Whether this slot's future has reached its terminal state. This transition happens-before the
     * slot's queue notification — a {@link FutureTask} publishes its state before running {@code
     * done()} — so a reconciliation point must consult it directly rather than the notification
     * queue, which lags behind it.
     */
    boolean isTerminal(CompletionSlot<O> slot);

    /**
     * Whether this slot's future was cancelled — i.e. abandoned by the scheduler's own cancellation
     * rather than reaching its own terminal state. Three paths call {@code cancel(true)}: {@code
     * checkCancelled}, {@code take}'s {@code InterruptedException} catch, and {@code afterFailure}.
     * Such a slot carries no genuine outcome; recording its interrupt as a failure would mask the
     * cancellation that caused it.
     */
    boolean isCancelled(CompletionSlot<O> slot);

    /**
     * Record every active slot whose future is already terminal into {@code outcomes}, removing it
     * from {@code active}. Consults each future directly (see {@link #isTerminal}), so a sibling
     * that completed but whose queue notification is still pending cannot leave its outcome — a
     * failure in particular — unreachable in input order at a refill or submission-failure
     * decision. Each such slot is marked consumed so its later queue notification is skipped by
     * {@link #take}. A slot the scheduler cancelled contributes no outcome (see {@link
     * #isCancelled}), so its interrupt cannot masquerade as a reachable failure and mask the
     * cancellation.
     *
     * <p>Dropping a cancelled slot's outcome leaves a permanent {@code null} in {@code outcomes}
     * while still removing it from {@code active}. That is safe only because every path that
     * cancels slots then throws unconditionally — {@code checkCancelled}, {@code take}'s {@code
     * InterruptedException} catch, and {@code afterFailure} from {@code completeAll}'s catch. If
     * any of them were ever made to continue instead, {@code deliverReady} would stop at that null,
     * {@code active} would drain, and {@code forEachOrdered} would return NORMALLY having silently
     * delivered a truncated prefix. Keep the throw — and never leave an index null on a path that
     * continues, which is why the racing-cancellation catch below records a failure for the slot
     * instead of dropping it.
     */
    default void recordTerminalCompletions(
        List<CompletionSlot<O>> active, List<TaskOutcome<O>> outcomes) {
      for (Iterator<CompletionSlot<O>> it = active.iterator(); it.hasNext(); ) {
        CompletionSlot<O> slot = it.next();
        if (slot.consumed || !isTerminal(slot)) {
          continue;
        }
        slot.consumed = true;
        it.remove();
        if (!isCancelled(slot)) {
          try {
            outcomes.set(slot.index, outcome(slot));
          } catch (CancellationException racedCancellation) {
            // Guard, not a known-live path: outcome() escapes only for a scheduler-cancelled
            // future, and every cancel(true) call site runs on this same scheduler thread, so the
            // interleaving should be impossible — but a raw CancellationException was observed
            // escaping forEachOrdered through here in a contended run and never reproduced.
            //
            // Record it as this index's failure rather than dropping it. Dropping leaves a
            // permanent null here while the slot is already out of active — exactly the truncation
            // this method's javadoc warns about: deliverReady stops at the null, active drains, and
            // forEachOrdered returns NORMALLY having delivered a short prefix. Silent data loss is
            // worse than a surfaced cancellation. As a failure at its own index it obeys
            // input-order precedence like any other, so an earlier store failure still wins.
            CancellationException surfaced =
                new CancellationException(
                    "fan-out task " + slot.index + " was cancelled while its outcome was read");
            surfaced.initCause(racedCancellation);
            LOG.warn(surfaced.getMessage(), racedCancellation);
            outcomes.set(slot.index, TaskOutcome.failure(surfaced));
          }
        }
      }
    }

    /** Capture a completed slot as data; only scheduler cancellation may escape this method. */
    TaskOutcome<O> outcome(CompletionSlot<O> slot);

    /**
     * Cancel or abandon active work after scheduler/consumer failure without waiting for it. The
     * supplied failure remains the caller-visible outcome.
     */
    void afterFailure(List<CompletionSlot<O>> active, Throwable processingFailure);
  }

  /** Interruptible FutureTask lifecycle with cooperative request cancellation. */
  private static final class CancellableTaskRuntime<I, O> implements TaskRuntime<I, O> {
    private final Executor executor;
    private final PropagatedContext context;
    private final Function<I, O> task;
    private final BooleanSupplier cancelled;
    private final IntConsumer completionBarrier;
    private final BlockingQueue<CompletionSlot<O>> completions = new LinkedBlockingQueue<>();

    private CancellableTaskRuntime(
        Executor executor,
        PropagatedContext context,
        Function<I, O> task,
        BooleanSupplier cancelled,
        IntConsumer completionBarrier) {
      this.executor = executor;
      this.context = context;
      this.task = task;
      this.cancelled = cancelled;
      this.completionBarrier = completionBarrier;
    }

    @Override
    public void beforeSubmit(List<CompletionSlot<O>> active) {
      checkCancelled(cancelled, active);
    }

    @Override
    public boolean cancellationObserved() {
      return cancelled.getAsBoolean();
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
              // FutureTask publishes its terminal state before running done(); the barrier (a no-op
              // in production) lets a test hold this notification while the slot is already
              // terminal. Enqueue in a finally: a barrier that throws or is interrupted must not
              // orphan the slot, which with a sole active slot would block take() forever on a
              // future that is already terminal.
              try {
                completionBarrier.accept(slot.index);
              } finally {
                completions.add(slot);
              }
            }
          };
      slot.task = submitted;
      executor.execute(submitted);
      return slot;
    }

    @Override
    public boolean isTerminal(CompletionSlot<O> slot) {
      return slot.task.isDone();
    }

    @Override
    public boolean isCancelled(CompletionSlot<O> slot) {
      return slot.task.isCancelled();
    }

    @Override
    public CompletionSlot<O> take(List<CompletionSlot<O>> active) {
      // A caller that cannot cancel (NEVER_CANCELLED, by identity) blocks for the next completion
      // instead of waking every CANCELLATION_POLL_MILLIS to poll a hard-false signal. That polling
      // scales with request concurrency rather than work: ~100 wakeups/second per in-flight
      // fan-out, which the sentinel avoids entirely. A wakeup cost on a conforming executor,
      // nothing more — the two paths differ in liveness only on a same-pool ForkJoinPool, where
      // both stall anyway (see the class javadoc), so neither is a way to make that shape work.
      boolean cancellable = cancelled != NEVER_CANCELLED;
      while (true) {
        // Drain a completion that is already ready before observing cancellation, so a task that
        // has finished (a failure at the next reachable index in particular) is recorded and keeps
        // its first-failure precedence rather than being masked by a racing cancellation.
        // Cancellation is observed only when nothing is ready — active work is still cancelled
        // promptly then. A slot already recorded through recordTerminalCompletions is skipped: its
        // outcome is set and it is no longer active, so this stale notification must not be
        // redelivered. Under the current invariants a redelivery is value-identical — outcome()
        // re-reads the same terminal future — so this guard is defensive rather than load-bearing,
        // and is not pinned by a test. (Not because it lands below nextIndex: a later index
        // recorded out of order while the head still runs is undelivered, so a redelivery can land
        // at or above it.)
        CompletionSlot<O> ready = completions.poll();
        if (ready != null) {
          if (!ready.consumed) {
            return ready;
          }
          continue;
        }
        try {
          CompletionSlot<O> slot;
          if (cancellable) {
            checkCancelled(cancelled, active);
            slot = managedPoll(completions, CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
          } else {
            slot = managedTake(completions);
          }
          if (slot != null && !slot.consumed) {
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
    public TaskOutcome<O> outcome(CompletionSlot<O> slot) {
      try {
        return TaskOutcome.success(terminalOutcome(slot.task));
      } catch (CancellationException cancellation) {
        // Escape ONLY for a future the scheduler itself abandoned (cancel(true)); that slot carries
        // no outcome. A CancellationException the task BODY threw is recorded at its own index
        // instead, so it cannot escape past still-unfinished predecessors and break input-order
        // precedence. Reachable from recordTerminalCompletions when cancel(true) lands between its
        // isCancelled pre-check and this read; that caller converts it back to "no outcome" rather
        // than letting it escape.
        if (slot.task.isCancelled()) {
          throw cancellation;
        }
        return TaskOutcome.failure(cancellation);
      } catch (RuntimeException | Error failure) {
        return TaskOutcome.failure(failure);
      }
    }

    @Override
    public void afterFailure(List<CompletionSlot<O>> active, Throwable processingFailure) {
      cancelSubmittedTasks(active);
    }
  }

  /**
   * Capture the outcome of an already-terminal future: its result, or its failure propagated
   * unwrapped. Every caller reaches this with a terminal future — a slot dequeued from {@code
   * completions} (enqueued only after {@code done()} fires) or one {@link TaskRuntime#isTerminal}
   * confirmed — so {@code get()} does not wait on the task. The {@code InterruptedException} catch
   * is unreachable rather than live interrupt handling: {@code FutureTask.awaitDone} yields while
   * the state is {@code COMPLETING} and only tests {@code Thread.interrupted()} when the state is
   * still {@code NEW}, which {@code isDone()} has already ruled out. It is kept so the contract
   * does not depend on that internal detail.
   */
  private static <O> O terminalOutcome(Future<O> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw cancelled();
    } catch (ExecutionException e) {
      throw Futures.propagate(e.getCause(), "unexpected checked exception from fan-out");
    }
  }

  private static <O> List<TaskOutcome<O>> emptyOutcomes(int size) {
    return new ArrayList<>(Collections.nCopies(size, null));
  }

  /**
   * Block indefinitely for the next completion without consuming an uncompensated ForkJoinPool
   * worker when the scheduler and its tasks share that pool. Used only for non-cancellable callers,
   * which have no signal to poll for.
   */
  // Not pinned by a test, deliberately. Its effect is FJP throughput for the supported topology (a
  // scheduler on an FJP worker dispatching tasks elsewhere): replacing it with a bare queue.take()
  // leaves the whole suite green, because FJP grows a worker for an external submission regardless
  // of compensation, so no unit test can isolate it. Attempts to pin it via a same-pool pool only
  // asserted an unsupported topology — see the class javadoc. Keep it: it is the documented,
  // correct way for a ForkJoinWorkerThread to block, and costs nothing off an FJP.
  private static <T> T managedTake(BlockingQueue<T> queue) throws InterruptedException {
    QueueTakeBlocker<T> blocker = new QueueTakeBlocker<>(queue);
    ForkJoinPool.managedBlock(blocker);
    return blocker.result;
  }

  /**
   * Block for a completion without consuming an uncompensated ForkJoinPool worker when the fan-out
   * scheduler and its tasks share that pool.
   */
  private static <T> T managedPoll(BlockingQueue<T> queue, long timeout, TimeUnit unit)
      throws InterruptedException {
    QueuePollBlocker<T> blocker = new QueuePollBlocker<>(queue, unit.toNanos(timeout));
    ForkJoinPool.managedBlock(blocker);
    return blocker.result;
  }

  /** Managed blocker for an unbounded completion-queue wait. */
  private static final class QueueTakeBlocker<T> implements ForkJoinPool.ManagedBlocker {
    private final BlockingQueue<T> queue;
    private T result;

    private QueueTakeBlocker(BlockingQueue<T> queue) {
      this.queue = queue;
    }

    @Override
    public boolean block() throws InterruptedException {
      if (result == null) {
        result = queue.take();
      }
      return true;
    }

    @Override
    public boolean isReleasable() {
      if (result == null) {
        result = queue.poll();
      }
      return result != null;
    }
  }

  /** Managed blocker for one bounded completion-queue poll. */
  private static final class QueuePollBlocker<T> implements ForkJoinPool.ManagedBlocker {
    private final BlockingQueue<T> queue;
    private final long deadlineNanos;
    private T result;
    private boolean done;

    private QueuePollBlocker(BlockingQueue<T> queue, long timeoutNanos) {
      this.queue = queue;
      this.deadlineNanos = System.nanoTime() + timeoutNanos;
    }

    @Override
    public boolean block() throws InterruptedException {
      if (!done) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos > 0) {
          result = queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
        }
        done = true;
      }
      return true;
    }

    @Override
    public boolean isReleasable() {
      if (!done) {
        result = queue.poll();
        done = result != null || System.nanoTime() >= deadlineNanos;
      }
      return done;
    }
  }

  /**
   * Cancel every active task and fail the caller when its cooperative signal is set, or when the
   * scheduler thread already carries an interrupt — the mechanism a container uses to unwind a
   * worker, which must abandon pending work rather than let the batch run to completion.
   */
  private static void checkCancelled(
      BooleanSupplier cancelled, List<? extends CompletionSlot<?>> active) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      cancelSubmittedTasks(active);
      throw cancelled();
    }
  }

  /** Interrupt every submitted task, including work already inside a downstream call. */
  private static void cancelSubmittedTasks(List<? extends CompletionSlot<?>> active) {
    // cancel(true) runs FutureTask.finishCompletion -> done() -> the completion barrier, ON THIS
    // thread. A throw from any one slot must not (a) leave the remaining siblings running, which is
    // the opposite of this method's purpose, or (b) escape to replace the store failure the caller
    // is being given — afterFailure runs inside completeAll's catch, just before it rethrows the
    // original. Cancel every slot, and log any casualty rather than attaching it.
    RuntimeException cancellationFault = null;
    for (CompletionSlot<?> slot : active) {
      if (slot.task == null) {
        continue;
      }
      try {
        slot.task.cancel(true);
      } catch (RuntimeException | Error failure) {
        if (cancellationFault == null) {
          cancellationFault = new IllegalStateException("failed to cancel a fan-out task");
        }
        cancellationFault.addSuppressed(failure);
      }
    }
    if (cancellationFault != null) {
      // Every slot has been attempted by now. Log rather than throw: the caller's own failure (or
      // cancellation) is the diagnosis they need, and this cannot be attached to it from here.
      LOG.warnf(cancellationFault, "one or more fan-out tasks could not be cancelled");
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
    private Future<O> task;
    // Set once this slot's outcome has been recorded through recordTerminalCompletions, so a queue
    // notification landing after the future became terminal is skipped rather than recording and
    // redelivering the same slot twice. Touched only by the scheduler thread.
    private boolean consumed;

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
    private final BooleanSupplier cancelled;
    private int nextIndex;

    private OrderedOutcomeConsumer(
        List<TaskOutcome<O>> outcomes, Consumer<? super O> consumer, BooleanSupplier cancelled) {
      this.outcomes = outcomes;
      this.consumer = consumer;
      this.cancelled = cancelled;
    }

    /** Deliver every newly contiguous outcome, stopping at the first unfinished input index. */
    private void deliverReady() {
      while (nextIndex < outcomes.size() && outcomes.get(nextIndex) != null) {
        // Recheck before every publish, not just once per drain: a consumer callback can itself
        // flip cancellation, and the rest of an already-ready prefix must not then be published to
        // a stopped stream.
        abortIfCancelled();
        int delivered = nextIndex++;
        TaskOutcome<O> outcome = outcomes.get(delivered);
        // Release the delivered outcome from the list: a slot below nextIndex is never re-read, and
        // null keeps its "not yet complete" meaning only for slots at/after nextIndex. This lets
        // forEachOrdered stream — a consumed result becomes collectable instead of being pinned for
        // the whole batch (mapOrdered still retains results in its own list). Memory behaviour
        // only, so no test pins it; removing it is silently correct and silently retaining.
        outcomes.set(delivered, null);
        if (outcome.failure() != null) {
          throw Futures.propagate(outcome.failure(), "unexpected checked exception from fan-out");
        }
        consumer.accept(outcome.result());
      }
      // And once after the last publish: with the window already drained, the scheduler's loop
      // would otherwise exit and return normally on a cancellation the final callback raised.
      abortIfCancelled();
    }

    /**
     * Abandon the batch when the request has cancelled or the scheduler thread carries an interrupt
     * — a completed task's queued result would otherwise let a pre-interrupted caller drain the
     * whole batch without the interrupt ever taking effect. A failure already reachable in input
     * order keeps precedence: it is the diagnosis the caller needs, and the cancellation merely
     * raced it.
     */
    private void abortIfCancelled() {
      if (!cancelled.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
        return;
      }
      Throwable reachable = earliestReachableFailure();
      if (reachable instanceof RuntimeException reachableRuntime) {
        throw reachableRuntime;
      }
      if (reachable instanceof Error reachableError) {
        throw reachableError;
      }
      throw cancelled();
    }

    /**
     * The earliest failure in the contiguous completed prefix from the next undelivered index, or
     * {@code null} — without delivering any success. Lets a task failure already reachable in input
     * order keep its precedence over an interrupting cancellation.
     */
    private Throwable earliestReachableFailure() {
      for (int i = nextIndex; i < outcomes.size() && outcomes.get(i) != null; i++) {
        Throwable failure = outcomes.get(i).failure();
        if (failure != null) {
          return failure;
        }
      }
      return null;
    }
  }
}
