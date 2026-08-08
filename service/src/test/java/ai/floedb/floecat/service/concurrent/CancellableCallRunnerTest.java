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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Exercises admission, cancellation ownership, and executor-discard races for blocking calls. */
class CancellableCallRunnerTest {

  /**
   * Saturated-admission count for the pool under test; asserted where saturation is the subject.
   * Each test owns one, so the accounting stays with the pool instead of a process-wide counter.
   */
  private final java.util.concurrent.atomic.AtomicInteger saturations =
      new java.util.concurrent.atomic.AtomicInteger();

  /** An open runtime. Shutdown-specific tests pass their own literal signal instead. */
  private static final java.util.function.BooleanSupplier notClosed = () -> false;

  private static final CancellableCallRunner.FailureMessages CALL_FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

  @Test
  void aTaskDispatchedBeforeClosureDoesNotStartItsStoreCall() {
    // Guards the window between admission and the task starting: acquire's checks cover before
    // admission, the caller's wait loop covers after dispatch, and neither stops a task already
    // handed to the executor from beginning a round trip once closure latches.
    var permits = new Semaphore(1);
    var closed = new java.util.concurrent.atomic.AtomicBoolean();
    var bodyRan = new java.util.concurrent.atomic.AtomicBoolean();
    var gate = new CountDownLatch(1);
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      // Occupy the worker so the admitted task queues, then latch closure before it can start.
      pool.execute(() -> awaitLatch(gate));
      var outcome = new AtomicReference<Throwable>();
      Thread caller =
          new Thread(
              () -> {
                try {
                  CancellableCallRunner.callWithoutCancellation(
                      pool,
                      permits,
                      closed::get,
                      () -> {
                        bodyRan.set(true);
                        return "ran";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                } catch (Throwable t) {
                  outcome.set(t);
                }
              },
              "pre-closure-dispatch");
      caller.start();
      awaitCondition(
          () -> permits.availablePermits() == 0, "the call to be admitted and dispatched");
      closed.set(true);
      gate.countDown();
      try {
        caller.join(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
      assertFalse(bodyRan.get(), "a task must not begin a store call after closure latched");
      assertInstanceOf(CancellationException.class, outcome.get());
    }
  }

  @Test
  void aWorkerOnlyCancellationPredicateFailureReleasesAdmission() {
    var permits = new Semaphore(1);
    Thread caller = Thread.currentThread();
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      assertThrows(
          IllegalStateException.class,
          () ->
              CancellableCallRunner.call(
                  pool,
                  permits,
                  () -> {
                    if (Thread.currentThread() != caller) {
                      throw new IllegalStateException("worker cancellation predicate failed");
                    }
                    return false;
                  },
                  notClosed,
                  () -> "unreachable",
                  CALL_FAILURES,
                  saturations::incrementAndGet));
      awaitPermits(permits, 1, "a preflight failure must not retire admission");
      assertEquals(
          "next",
          CancellableCallRunner.callWithoutCancellation(
              pool, permits, notClosed, () -> "next", CALL_FAILURES, saturations::incrementAndGet));
    }
  }

  @Test
  void anOperationThrownMessageLessCancellationIsPreserved() {
    var permits = new Semaphore(1);
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      CancellationException thrown =
          assertThrows(
              CancellationException.class,
              () ->
                  CancellableCallRunner.callWithoutCancellation(
                      pool,
                      permits,
                      notClosed,
                      () -> {
                        throw new CancellationException();
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet));
      assertNull(thrown.getMessage());
      awaitPermits(permits, 1, "the cancelled operation must release admission");
    }
  }

  @Test
  void anAlreadyInterruptedCallerIsRejectedBeforeTakingAPermit() {
    // acquire's interrupt gate. rejectCancelledStart reads the cancellation signal, not the
    // interrupt flag, so nothing else covers this.
    var permits = new Semaphore(1);
    var bodyRan = new java.util.concurrent.atomic.AtomicBoolean();
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      Thread.currentThread().interrupt();
      try {
        assertThrows(
            CancellationException.class,
            () ->
                CancellableCallRunner.call(
                    pool,
                    permits,
                    () -> false,
                    notClosed,
                    () -> {
                      bodyRan.set(true);
                      return "ran";
                    },
                    CALL_FAILURES,
                    saturations::incrementAndGet));
      } finally {
        Thread.interrupted(); // clear for the rest of the suite
      }
      assertFalse(bodyRan.get(), "an interrupted caller must not dispatch work");
      assertEquals(1, permits.availablePermits(), "and must not retain a permit");
    }
  }

  @Test
  void aQueuedCallDiscardedByShutdownSaysSoRatherThanBlamingTheRequest() {
    // done() picks its reason from cancellationRequested. Asserting only the exception type lets a
    // swapped ternary report a shutdown-discarded call as a request cancellation.
    var permits = new Semaphore(2);
    var gate = new CountDownLatch(1);
    ThreadPoolExecutor pool = MetadataIoExecutors.newBoundedDaemonPool(1, 4, "discard-test-");
    var outcome = new AtomicReference<Throwable>();
    var occupantFailure = new AtomicReference<Throwable>();
    try {
      // Not awaitLatch: the shutdownNow this test stages interrupts this worker by design, and
      // awaitLatch turns that into an AssertionError thrown on the pool thread — printed by
      // surefire, invisible to JUnit, and indistinguishable from a real worker failure.
      pool.execute(
          () -> {
            try {
              gate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException expected) {
              Thread.currentThread().interrupt();
            } catch (Throwable unexpected) {
              occupantFailure.set(unexpected);
            }
          });
      Thread caller =
          new Thread(
              () -> {
                try {
                  CancellableCallRunner.callWithoutCancellation(
                      pool,
                      permits,
                      notClosed,
                      () -> "queued",
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                } catch (Throwable t) {
                  outcome.set(t);
                }
              },
              "discarded");
      caller.start();
      awaitCondition(() -> pool.getQueue().size() >= 1, "the call to be queued behind the worker");
      CancellableCallRunner.cancelDiscardedTasks(pool.shutdownNow());
      gate.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(10));

      assertInstanceOf(CancellationException.class, outcome.get());
      // The reason travels as the cause here, not the top-level message: CompletableFuture.get
      // surfaces its own CancellationException and hangs ours beneath it. Assert on the chain so
      // this pins the attribution rather than the wrapper.
      assertTrue(
          reasonChain(outcome.get()).contains("metadata I/O executor closed"),
          "a shutdown-discarded call must not be blamed on the request: "
              + reasonChain(outcome.get()));
      assertFalse(
          reasonChain(outcome.get()).contains("cancelled"),
          "the request was never cancelled: " + reasonChain(outcome.get()));
      assertNull(occupantFailure.get(), "the pool occupant must not have failed unexpectedly");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    } finally {
      gate.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void eachEntryPointPollsAtItsOwnCadence() throws Exception {
    // The two intervals were once swapped and nothing noticed: a grep for the constant name passes
    // while it sits in the wrong method. Cadence is only observable by counting reads, so count
    // them. call() polls at request scale to stay responsive to cancellation; the non-cancellable
    // path has only a shutdown flag to watch and polls an order of magnitude slower.
    var release = new CountDownLatch(1);
    var cancelReads = new java.util.concurrent.atomic.AtomicInteger();
    var closedReads = new java.util.concurrent.atomic.AtomicInteger();
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      Runnable holdBriefly =
          () -> {
            try {
              release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          };

      Thread cancellable =
          new Thread(
              () ->
                  CancellableCallRunner.call(
                      pool,
                      new Semaphore(1),
                      () -> {
                        cancelReads.incrementAndGet();
                        return false;
                      },
                      notClosed,
                      () -> {
                        holdBriefly.run();
                        return "done";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet),
              "cadence-cancellable");
      Thread nonCancellable =
          new Thread(
              () ->
                  CancellableCallRunner.callWithoutCancellation(
                      pool,
                      new Semaphore(1),
                      () -> {
                        closedReads.incrementAndGet();
                        return false;
                      },
                      () -> {
                        holdBriefly.run();
                        return "done";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet),
              "cadence-non-cancellable");

      cancellable.start();
      nonCancellable.start();
      Thread.sleep(600);
      release.countDown();
      cancellable.join(TimeUnit.SECONDS.toMillis(10));
      nonCancellable.join(TimeUnit.SECONDS.toMillis(10));

      // ~600ms in flight: request scale gives ~24 reads, shutdown scale ~2. A swap inverts this.
      assertTrue(
          cancelReads.get() > 8,
          "the cancellable path must poll at request scale, saw " + cancelReads.get());
      assertTrue(
          closedReads.get() < 8,
          "the non-cancellable path must poll at shutdown scale, saw " + closedReads.get());
      assertTrue(
          cancelReads.get() > closedReads.get() * 3,
          "cadences look swapped: cancellable="
              + cancelReads.get()
              + " nonCancellable="
              + closedReads.get());
    }
  }

  @Test
  void aPermitTakenByThePollingLoopIsReturnedWhenClosureLatchesFirst() {
    // The fast path's copy of this guard is pinned; the polling loop's was not, so deleting it left
    // the suite green — the exact regression the shared helper exists to prevent.
    //
    // Closure must land between the loop-top check and tryAcquire succeeding: the signal reports
    // open for the loop-top reads and closed only once a permit has been handed over.
    var permits = new Semaphore(1);
    var reads = new java.util.concurrent.atomic.AtomicInteger();
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      permits.acquire(); // force the polling loop
      java.util.function.BooleanSupplier closedOnceAdmitted =
          () -> reads.incrementAndGet() >= 3; // reads 1-2 are loop tops, 3 is inside admitOrRelease

      var outcome = new AtomicReference<Throwable>();
      Thread waiter =
          new Thread(
              () -> {
                try {
                  CancellableCallRunner.call(
                      pool,
                      permits,
                      () -> false,
                      closedOnceAdmitted,
                      () -> "must not run",
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                } catch (Throwable t) {
                  outcome.set(t);
                }
              },
              "polling-waiter");
      waiter.start();
      awaitCondition(() -> reads.get() >= 2, "the waiter to reach its second loop top");
      permits.release();
      waiter.join(TimeUnit.SECONDS.toMillis(10));

      assertInstanceOf(RejectedExecutionException.class, outcome.get());
      assertEquals(
          1, permits.availablePermits(), "a permit taken then rejected must be handed back");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void aSaturatedArrivalIsCountedAndAnUncontendedOneIsNot() {
    // S2: the saturated-wait counter feeds a published metric and nothing exercised it — neither
    // that it fires when the ceiling is full, nor that it stays quiet when it is not.
    var permits = new Semaphore(1);
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      CancellableCallRunner.call(
          pool,
          permits,
          () -> false,
          notClosed,
          () -> "uncontended",
          CALL_FAILURES,
          saturations::incrementAndGet);
      assertEquals(0, saturations.get(), "an admission that never waited is not saturation");

      permits.acquire(); // hold the only permit so the next arrival must wait
      var admitted = new CountDownLatch(1);
      Thread waiter =
          new Thread(
              () ->
                  CancellableCallRunner.call(
                      pool,
                      permits,
                      () -> false,
                      notClosed,
                      () -> {
                        admitted.countDown();
                        return "queued";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet),
              "saturated-waiter");
      waiter.start();
      awaitCondition(() -> saturations.get() == 1, "the waiter to observe a full ceiling");
      permits.release();
      awaitLatch(admitted);
      waiter.join(TimeUnit.SECONDS.toMillis(10));

      assertEquals(1, saturations.get(), "an arrival that found the ceiling full is saturation");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void aClosedRuntimeRejectsRatherThanCancelsOnTheFastPath() {
    // S1: the existing test asserted RuntimeException, which cannot tell the two apart. The split
    // is documented as load-bearing for RPC status mapping: not-yet-admitted is a rejection,
    // in-flight is a cancellation.
    var permits = new Semaphore(2);
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      assertThrowsExactly(
          RejectedExecutionException.class,
          () ->
              CancellableCallRunner.call(
                  pool,
                  permits,
                  () -> false,
                  () -> true,
                  () -> "must not run",
                  CALL_FAILURES,
                  saturations::incrementAndGet));
    }
  }

  @Test
  void aPermitIsReturnedExactlyOnceWhenBothReleaseSitesFire() {
    // PermitLease's CAS is the only thing stopping a double release, and a double release silently
    // raises the process ceiling above capacity — nothing else would notice.
    //
    // Both sites only fire when the tombstone is NOT evicted: done() releases on the cancel, and
    // the pool later runs the dead task so run()'s post-super block releases again. A delegating
    // executor defeats the ThreadPoolExecutor downcast, so no eviction happens — the same shape as
    // a ManagedExecutor or Executors.newSingleThreadExecutor in production.
    var permits = new Semaphore(2);
    ExecutorService backing = Executors.newFixedThreadPool(1);
    Executor undowncastable = backing::execute;
    try {
      var blocked = new UninterruptibleBlocker();
      Thread holder =
          new Thread(
              () ->
                  CancellableCallRunner.callWithoutCancellation(
                      undowncastable,
                      permits,
                      notClosed,
                      () -> {
                        blocked.await();
                        return "held";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet),
              "holder");
      holder.start();
      awaitLatch(blocked.started);

      var cancel = new java.util.concurrent.atomic.AtomicBoolean();
      Thread queued =
          new Thread(
              () -> {
                try {
                  CancellableCallRunner.call(
                      undowncastable,
                      permits,
                      cancel::get,
                      notClosed,
                      () -> "queued",
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                } catch (RuntimeException expected) {
                  // cancelled while queued behind the holder
                }
              },
              "queued");
      queued.start();
      awaitCondition(() -> permits.availablePermits() == 0, "the queued call to be dispatched");
      cancel.set(true);
      try {
        queued.join(TimeUnit.SECONDS.toMillis(10));
        blocked.release.countDown();
        holder.join(TimeUnit.SECONDS.toMillis(10));
        backing.shutdown();
        assertTrue(backing.awaitTermination(10, TimeUnit.SECONDS), "pool must drain");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
      assertEquals(
          2, permits.availablePermits(), "a double release would raise the ceiling above capacity");
    } finally {
      blockedRelease(backing);
    }
  }

  private static void blockedRelease(ExecutorService pool) {
    pool.shutdownNow();
  }

  /** Every message in a throwable's cause chain, so an assertion cannot miss a wrapped reason. */
  private static String reasonChain(Throwable failure) {
    var out = new StringBuilder();
    for (Throwable t = failure; t != null && out.length() < 500; t = t.getCause()) {
      out.append(t.getMessage()).append(" | ");
    }
    return out.toString();
  }

  /** Block until a condition holds, or fail rather than hang. */
  private static void awaitCondition(java.util.function.BooleanSupplier condition, String what) {
    for (int i = 0; i < 500; i++) {
      if (condition.getAsBoolean()) {
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
    throw new AssertionError("timed out waiting for " + what);
  }

  @Test
  void aClosedRuntimeIsRejectedOnTheFastPathWithPermitsFree() throws Exception {
    // What this pins is the permit return: a permit taken by the uncontended fast path after
    // closure latched must be handed back, not retired. The body not running is enforced separately
    // by the task's own closure check, and the exception type by the sibling test below.
    var permits = new Semaphore(4);
    var bodies = new java.util.concurrent.atomic.AtomicInteger();
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      for (int i = 0; i < 50; i++) {
        assertThrows(
            RuntimeException.class,
            () ->
                CancellableCallRunner.call(
                    pool,
                    permits,
                    () -> false,
                    () -> true,
                    () -> {
                      bodies.incrementAndGet();
                      return "must not run";
                    },
                    CALL_FAILURES,
                    saturations::incrementAndGet));
        assertThrows(
            RuntimeException.class,
            () ->
                CancellableCallRunner.callWithoutCancellation(
                    pool,
                    permits,
                    () -> true,
                    () -> {
                      bodies.incrementAndGet();
                      return "must not run";
                    },
                    CALL_FAILURES,
                    saturations::incrementAndGet));
      }
    }
    assertEquals(0, bodies.get(), "no store call may start once closure has latched");
    assertEquals(
        4, permits.availablePermits(), "a rejected fast-path admission must return its permit");
  }

  @Test
  void aCompletedResultBeatsACancellationObservedInTheSameIteration() throws Exception {
    // isDone() and the cancellation read are separate observations. A supplier that completes the
    // operation and only then reports cancellation lands exactly in that window; the finished
    // value (or store failure) must still win, or a real failure is replaced by a cancellation.
    var permits = new Semaphore(1);
    var release = new CountDownLatch(1);
    try (ExecutorService pool = Executors.newFixedThreadPool(1)) {
      Thread callerThread = Thread.currentThread();
      var operationStarted = new CountDownLatch(1);
      java.util.function.BooleanSupplier completeThenCancel =
          () -> {
            // The submitted task also reads this signal before running, on a pool thread. Report
            // cancellation only to the caller's wait loop, which is the window under test.
            if (Thread.currentThread() != callerThread) {
              return false;
            }
            // Report cancellation only once the operation is genuinely in flight. Firing on the
            // first caller-thread read would make this test sensitive to WHERE the runner reads the
            // signal rather than to the race it exists for: a read added before dispatch would
            // short-circuit the call, and the completion-wins path would never be exercised.
            if (operationStarted.getCount() != 0) {
              return false;
            }
            release.countDown();
            // Wait for the permit to come back, not for the operation body: the task releases its
            // lease only after completing the result, so this is the point at which the loop's next
            // observation is guaranteed to see a completed result AND a true cancellation flag.
            for (int i = 0; i < 500 && permits.availablePermits() == 0; i++) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
            return true;
          };

      IllegalStateException thrown =
          assertThrows(
              IllegalStateException.class,
              () ->
                  CancellableCallRunner.call(
                      pool,
                      permits,
                      completeThenCancel,
                      notClosed,
                      () -> {
                        operationStarted.countDown();
                        awaitLatch(release);
                        throw new IllegalStateException("store failure");
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet));

      assertEquals(
          "store failure",
          thrown.getMessage(),
          "the completed store failure must win over the racing cancellation");
    }
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("latch never released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void rejectsAnExecutorThatRunsTheBlockingCallInline() {
    Executor directExecutor = Runnable::run;
    var permits = new Semaphore(1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CancellableCallRunner.call(
                directExecutor,
                permits,
                () -> false,
                notClosed,
                () -> "must not run",
                CALL_FAILURES,
                saturations::incrementAndGet));
    assertEquals(1, permits.availablePermits());
  }

  @Test
  void aCompletedFailureWinsOverCancellationObservedAfterTheOperationFinished() {
    // Reviewer scenario: the operation finishes exceptionally, then cancellation flips before the
    // waiting caller next looks. The completed store failure must propagate unchanged — a racing
    // cancellation must not mask it. This executor runs the task on another thread but blocks until
    // it finishes, so the future is already completed exceptionally when call() begins waiting. The
    // operation flips cancellation as its last act (so acquire() does not reject the call before it
    // runs). What surfaces the failure is abort()'s completeExceptionally arbiter, not the
    // isDone-before-cancelled fast path — mutating that check to false leaves this green.
    // assertThrowsExactly
    // matters: CancellationException — what the masking bug would throw — is a subclass of
    // IllegalStateException and would slip past assertThrows.
    Executor completeBeforeWaiting =
        runnable -> {
          Thread worker = new Thread(runnable, "completed-before-waiting");
          worker.start();
          try {
            worker.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    var permits = new Semaphore(1);
    var cancelled = new AtomicBoolean(false);

    IllegalStateException thrown =
        assertThrowsExactly(
            IllegalStateException.class,
            () ->
                CancellableCallRunner.call(
                    completeBeforeWaiting,
                    permits,
                    cancelled::get,
                    notClosed,
                    () -> {
                      cancelled.set(true); // cancellation flips as the operation finishes
                      throw new IllegalStateException("store failure");
                    },
                    CALL_FAILURES,
                    saturations::incrementAndGet));
    assertEquals("store failure", thrown.getMessage());
    awaitPermits(permits, 1, "the completed call must release its permit");
  }

  @Test
  void callWithoutCancellationWaitsForTheEventualResult() throws Exception {
    ExecutorService metadataExecutor = Executors.newFixedThreadPool(1);
    ExecutorService callerExecutor = Executors.newFixedThreadPool(1);
    var permits = new Semaphore(1);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try {
      CompletableFuture<String> result =
          CompletableFuture.supplyAsync(
              () ->
                  CancellableCallRunner.callWithoutCancellation(
                      metadataExecutor,
                      permits,
                      notClosed,
                      () -> {
                        started.countDown();
                        try {
                          release.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return "done";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet),
              callerExecutor);
      assertTrue(started.await(1, TimeUnit.SECONDS));
      assertThrows(
          java.util.concurrent.TimeoutException.class, () -> result.get(25, TimeUnit.MILLISECONDS));
      assertEquals(0, permits.availablePermits(), "active I/O must retain its admission slot");
      release.countDown();
      assertEquals("done", result.get(1, TimeUnit.SECONDS));
      awaitPermits(permits, 1, "the completed call must release its permit");
    } finally {
      release.countDown();
      metadataExecutor.shutdownNow();
      callerExecutor.shutdownNow();
    }
  }

  @Test
  void shutdownNowReleasesAndCompletesQueuedCall() throws Exception {
    var executor =
        new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new java.util.concurrent.LinkedBlockingQueue<>());
    var permits = new Semaphore(2);
    var firstStarted = new CountDownLatch(1);
    var allowFirst = new CountDownLatch(1);
    try {
      CompletableFuture<String> first =
          CompletableFuture.supplyAsync(
              () ->
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      () -> false,
                      notClosed,
                      () -> {
                        firstStarted.countDown();
                        try {
                          allowFirst.await();
                        } catch (InterruptedException ignored) {
                          // Simulate a store call that does not abort immediately.
                          try {
                            allowFirst.await();
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                          }
                        }
                        return "first";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> queued =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      () -> false,
                      notClosed,
                      () -> "queued",
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      for (int attempt = 0; attempt < 100 && executor.getQueue().isEmpty(); attempt++) {
        Thread.sleep(10);
      }
      assertEquals(1, executor.getQueue().size());

      CancellableCallRunner.cancelDiscardedTasks(executor.shutdownNow());

      assertTrue(queued.get(250, TimeUnit.MILLISECONDS) instanceof CancellationException);
      awaitPermits(permits, 1, "the discarded call must release its permit");

      allowFirst.countDown();
      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
      awaitPermits(permits, 2, "every call must release its permit");
    } finally {
      allowFirst.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void cancellingQueuedCallRemovesItBeforeRecyclingAdmission() throws Exception {
    var executor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.AbortPolicy());
    var permits = new Semaphore(2);
    var firstStarted = new CountDownLatch(1);
    var allowFirst = new CountDownLatch(1);
    var cancelQueued = new AtomicBoolean();
    try {
      CompletableFuture<String> first =
          CompletableFuture.supplyAsync(
              () ->
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      () -> false,
                      notClosed,
                      () -> {
                        firstStarted.countDown();
                        try {
                          allowFirst.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return "first";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> cancelledQueued =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      cancelQueued::get,
                      notClosed,
                      () -> "cancelled",
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      awaitQueueSize(executor, 1);

      cancelQueued.set(true);
      assertTrue(cancelledQueued.get(1, TimeUnit.SECONDS) instanceof CancellationException);
      awaitQueueSize(executor, 0);

      CompletableFuture<String> replacement =
          CompletableFuture.supplyAsync(
              () ->
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      () -> false,
                      notClosed,
                      () -> "replacement",
                      CALL_FAILURES,
                      saturations::incrementAndGet));
      awaitQueueSize(executor, 1);

      allowFirst.countDown();
      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertEquals("replacement", replacement.get(1, TimeUnit.SECONDS));
      awaitPermits(permits, 2, "the cancelled queued call must have recycled its permit");
    } finally {
      allowFirst.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void closingTheRuntimeReleasesACancellableCallerParkedOnAnUninterruptibleOperation()
      throws Exception {
    // The sibling coverage exercises callWithoutCancellation. call() has its own copy of the
    // closure branch in the abort reason it passes to awaitOutcome; losing it parks a cancellable
    // caller forever, because this tier has no deadline and the operation ignores interruption.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    var permits = new Semaphore(1);
    var closed = new AtomicBoolean();
    var blocker = new UninterruptibleBlocker();
    try {
      CompletableFuture<Throwable> caller =
          CompletableFuture.supplyAsync(
              () ->
                  assertThrows(
                      Throwable.class,
                      () ->
                          CancellableCallRunner.call(
                              executor,
                              permits,
                              () -> false,
                              closed::get,
                              () -> {
                                blocker.await();
                                return "done";
                              },
                              CALL_FAILURES,
                              saturations::incrementAndGet)));
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      closed.set(true);
      Throwable failure = caller.get(5, TimeUnit.SECONDS);
      assertInstanceOf(CancellationException.class, failure);
      assertEquals(CancellableCallRunner.RUNTIME_CLOSED, failure.getMessage());
    } finally {
      blocker.release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void aCallCancelledWhileQueuedNeverEntersTheOperation() throws Exception {
    // Cancellation between admission and the worker picking the task up. What stops the body here
    // is the caller's abort cancelling the FutureTask before the worker claims it; the task's own
    // rejectCancelledStart check covers the narrower window where the worker already claimed it.
    // Verified: disabling that check leaves this green, so the assertion below is on the outcome —
    // no body, both permits back — not on which of the two gates fired.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    var permits = new Semaphore(2);
    var cancel = new AtomicBoolean();
    var occupied = new UninterruptibleBlocker();
    var secondBodyRan = new AtomicBoolean();
    try {
      // Occupy the single worker so the second call is admitted but sits in the queue.
      CompletableFuture.runAsync(
          () ->
              CancellableCallRunner.call(
                  executor,
                  permits,
                  () -> false,
                  notClosed,
                  () -> {
                    occupied.await();
                    return "held";
                  },
                  CALL_FAILURES,
                  saturations::incrementAndGet));
      assertTrue(occupied.started.await(1, TimeUnit.SECONDS));

      cancel.set(true);
      assertThrows(
          CancellationException.class,
          () ->
              CancellableCallRunner.call(
                  executor,
                  permits,
                  cancel::get,
                  notClosed,
                  () -> {
                    secondBodyRan.set(true);
                    return "queued";
                  },
                  CALL_FAILURES,
                  saturations::incrementAndGet));

      occupied.release.countDown();
      // Give the freed worker every chance to run the queued task before asserting it did not.
      for (int attempt = 0; attempt < 100 && permits.availablePermits() < 2; attempt++) {
        Thread.sleep(10);
      }
      assertFalse(
          secondBodyRan.get(), "a call cancelled while queued must not start its operation");
      assertEquals(2, permits.availablePermits(), "both permits must come back");
    } finally {
      occupied.release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void admissionIsReturnedWhenTheExecutorRejectsTheSubmission() {
    // The permit is taken before the submit, so a rejection that escapes without releasing shrinks
    // the process-wide ceiling by one for good — silently, one call at a time.
    ExecutorService rejecting =
        new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.AbortPolicy());
    rejecting.shutdown(); // a shut-down pool rejects every submission
    var permits = new Semaphore(1);
    try {
      assertThrows(
          RejectedExecutionException.class,
          () ->
              CancellableCallRunner.call(
                  rejecting,
                  permits,
                  () -> false,
                  notClosed,
                  () -> "unreachable",
                  CALL_FAILURES,
                  saturations::incrementAndGet));
      assertEquals(1, permits.availablePermits(), "a rejected submission must return its permit");
    } finally {
      rejecting.shutdownNow();
    }
  }

  @Test
  void aThrowingSaturationSinkDoesNotFailTheCall() throws Exception {
    // The sink is a telemetry callback owned by another module. A counter that throws under a
    // strict registry would otherwise turn every saturated admission into a failed request.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    var permits = new Semaphore(1);
    var blocker = new UninterruptibleBlocker();
    Runnable throwingSink =
        () -> {
          throw new IllegalStateException("sink is broken");
        };
    try {
      CompletableFuture.runAsync(
          () ->
              CancellableCallRunner.call(
                  executor,
                  permits,
                  () -> false,
                  notClosed,
                  () -> {
                    blocker.await();
                    return "held";
                  },
                  CALL_FAILURES,
                  throwingSink));
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      CompletableFuture<String> waiter =
          CompletableFuture.supplyAsync(
              () ->
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      () -> false,
                      notClosed,
                      () -> "admitted",
                      CALL_FAILURES,
                      throwingSink));
      Thread.sleep(50); // let the waiter reach the saturated path and call the sink
      blocker.release.countDown();
      assertEquals("admitted", waiter.get(5, TimeUnit.SECONDS));
    } finally {
      blocker.release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void cancellingRunningCallRetainsAdmissionUntilOperationExits() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    var permits = new Semaphore(1);
    var cancel = new AtomicBoolean();
    var blocker = new UninterruptibleBlocker();
    try {
      CompletableFuture<Throwable> caller =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor,
                      permits,
                      cancel::get,
                      notClosed,
                      () -> {
                        blocker.await();
                        return "done";
                      },
                      CALL_FAILURES,
                      saturations::incrementAndGet);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      cancel.set(true);
      assertTrue(caller.get(1, TimeUnit.SECONDS) instanceof CancellationException);
      assertEquals(0, permits.availablePermits());
      // The permit is retained because the operation is still running on a pool worker, and it is
      // that worker — not the caller — that cancellation interrupts. Asserting both is what
      // distinguishes retained admission from a leaked permit.
      Thread ranOn = blocker.executionThread.get();
      assertNotNull(ranOn, "the operation must have started on a worker");
      assertNotSame(Thread.currentThread(), ranOn, "the operation must not run on the caller");
      assertTrue(
          blocker.interrupted.await(1, TimeUnit.SECONDS),
          "cancellation must interrupt the thread running the operation");

      blocker.release.countDown();
      for (int attempt = 0; attempt < 100 && permits.availablePermits() == 0; attempt++) {
        Thread.sleep(10);
      }
      assertEquals(1, permits.availablePermits());
    } finally {
      blocker.release.countDown();
      executor.shutdownNow();
    }
  }

  /**
   * Wait for admission to settle at {@code expected}. The submitted runnable publishes its result
   * before releasing the permit in its {@code finally}, so a caller that has observed the value can
   * legitimately still see the old count for a moment — asserting immediately races that window. A
   * leaked permit still fails here, it just takes the timeout to say so.
   */
  private static void awaitPermits(Semaphore permits, int expected, String message) {
    for (int attempt = 0; attempt < 200 && permits.availablePermits() != expected; attempt++) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    assertEquals(expected, permits.availablePermits(), message);
  }

  private static void awaitQueueSize(ThreadPoolExecutor executor, int expected) throws Exception {
    for (int attempt = 0; attempt < 100 && executor.getQueue().size() != expected; attempt++) {
      Thread.sleep(10);
    }
    assertEquals(expected, executor.getQueue().size());
  }
}
