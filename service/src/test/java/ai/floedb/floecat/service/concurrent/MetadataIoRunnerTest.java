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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies process-wide admission shared by metadata callers. */
class MetadataIoRunnerTest {

  /**
   * Both statics these tests touch — the shared runtime and the saturation sink — back every
   * default-constructed runner in this JVM. Restored whichever way a test exits.
   *
   * <p>Scoped to this classloader, not the fork. {@code MetadataIoRunner} is an application class,
   * so each {@code @QuarkusTest} restart loads its own copy with its own statics, and Quarkus's
   * {@code QuarkusClassOrderer} runs non-Quarkus classes last anyway. What this protects is the
   * plain unit tests that run after these, which do share them.
   */
  @AfterEach
  void restoreProcessWideState() {
    MetadataIoRunner.reopenSharedRuntime();
    // The default sink is a no-op, so clearing is a full restore: nothing outside CDI installs one.
    MetadataIoRunner.clearSaturationSink();
  }

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

  @Test
  void anUnresolvableConfiguredCapacityFailsStartup() {
    // getOptionalValue reports a declared-but-unexpanded ${VAR} as an empty Optional rather than
    // throwing, so reading through it accepts the property and configuredCapacity() then falls back
    // to the default: an operator who lowered the ceiling silently gets it back.
    String previous = System.getProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    try {
      System.setProperty(
          MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, "${FLOECAT_NO_SUCH_VAR_FOR_THIS_TEST}");
      assertThrows(IllegalStateException.class, MetadataIoRunner::validateConfiguredCapacity);
    } finally {
      if (previous == null) {
        System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
      } else {
        System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, previous);
      }
    }
  }

  @Test
  void closingASharedRuntimeFacadeDoesNotTearDownTheProcessWideRuntime() {
    // One bean's @PreDestroy must not close the runtime every other caller is still using. The
    // guard is a single early return, and nothing else would report its loss until an unrelated
    // caller started seeing RejectedExecutionException.
    MetadataIoRunner facade = new MetadataIoRunner();
    assertEquals("ok", facade.callWithoutCancellation(() -> "ok", FAILURES));
    facade.close();
    assertEquals(
        "ok",
        new MetadataIoRunner().callWithoutCancellation(() -> "ok", FAILURES),
        "closing a shared-runtime facade must leave the process-wide runtime open");
  }

  @Test
  void aCachedFacadeFollowsTheRuntimeAcrossARestart() {
    // A facade outlives the runtime it was created against. A CDI-scoped bean is exactly this
    // shape:
    // built once, held for the container's life. If it captured the RuntimeState it would keep
    // serving the one a shutdown closed — closed is sticky and reopen only clears the sentinel.
    MetadataIoRunner longLived = new MetadataIoRunner();
    assertEquals("ok", longLived.callWithoutCancellation(() -> "ok", FAILURES));

    MetadataIoRunner.closeSharedRuntimeIfStarted();
    MetadataIoRunner.reopenSharedRuntime();

    assertEquals(
        "ok",
        longLived.callWithoutCancellation(() -> "ok", FAILURES),
        "a facade created before the restart must follow the new runtime, not the closed one");
  }

  @Test
  void theSaturationSinkIsInstalledAndClearedAcrossTheBeanBoundary() {
    // setSaturationSink lives in this class, the install is in the telemetry bean and the clear is
    // in the lifecycle bean — three files, and nothing exercised the wire itself.
    var hits = new java.util.concurrent.atomic.AtomicInteger();
    MetadataIoRunner.setSaturationSink(hits::incrementAndGet);
    var runner = new MetadataIoRunner(1);
    var blocker = new UninterruptibleBlocker();
    Thread holder =
        new Thread(
            () ->
                runner.callWithoutCancellation(
                    () -> {
                      blocker.await();
                      return "held";
                    },
                    FAILURES),
            "sink-holder");
    try {
      holder.start();
      await(blocker.started);
      Thread waiter =
          new Thread(() -> runner.callWithoutCancellation(() -> "queued", FAILURES), "sink-waiter");
      waiter.start();
      awaitSaturation(hits);
      blocker.release.countDown();
      waiter.join(TimeUnit.SECONDS.toMillis(10));
      assertEquals(1, hits.get(), "a saturated admission must reach the installed sink");

      MetadataIoRunner.clearSaturationSink();
      hits.set(0);
      runner.callWithoutCancellation(() -> "uncontended", FAILURES);
      assertEquals(0, hits.get(), "a cleared sink must not be called");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    } finally {
      blocker.release.countDown();
      MetadataIoRunner.clearSaturationSink();
      runner.close();
    }
  }

  private static void awaitSaturation(java.util.concurrent.atomic.AtomicInteger hits) {
    for (int i = 0; i < 500 && hits.get() == 0; i++) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }

  @Test
  void aNestedCallIntoAnotherRuntimeAcquiresThatRuntimesPermit() {
    // S4: the re-entrant fast path compares RuntimeState by identity. Reusing a permit granted by a
    // different runtime would leave that runtime's ceiling unenforced and run the work on the wrong
    // pool — two semaphores where the design promises one per runtime.
    var outer = new MetadataIoRunner(1);
    var inner = new MetadataIoRunner(1);
    try {
      int innerInUse =
          outer.callWithoutCancellation(
              () -> inner.callWithoutCancellation(inner::permitsInUse, FAILURES), FAILURES);
      assertEquals(1, innerInUse, "the nested call must hold a permit from the runtime it entered");
      assertEquals(1, outer.capacity());
    } finally {
      outer.close();
      inner.close();
    }
  }

  @Test
  void anAlreadyInterruptedCallerStartsNoNestedWork() {
    // S5: the re-entrant path applies the same interrupt gate a fresh admission does. Without it a
    // nested read starts store I/O for a caller whose owner thread has already been unwound.
    var runner = new MetadataIoRunner(1);
    try {
      var nestedRan = new java.util.concurrent.atomic.AtomicBoolean();
      assertThrows(
          CancellationException.class,
          () ->
              runner.callWithoutCancellation(
                  () -> {
                    Thread.currentThread().interrupt();
                    try {
                      return runner.callWithoutCancellation(
                          () -> {
                            nestedRan.set(true);
                            return "nested";
                          },
                          FAILURES);
                    } finally {
                      Thread.interrupted(); // clear so the pool worker is reusable
                    }
                  },
                  FAILURES));
      assertFalse(nestedRan.get(), "an interrupted caller must not start nested store work");
    } finally {
      runner.close();
    }
  }

  @Test
  void anAdmittedOperationIsRefusedEvenWhenTheFanOutIsEmpty() {
    // The guard is about the call site, not the data. Gating it on item count would make a wiring
    // mistake data-dependent: invisible wherever a caller happens to have no rows — unit tests,
    // fresh fixtures, "no rows matched" — and fatal in production the first time it has some.
    var runner = new MetadataIoRunner(1);
    try (ExecutorService units = Executors.newFixedThreadPool(1)) {
      IllegalStateException thrown =
          runner.callWithoutCancellation(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          BoundedFanout.mapOrdered(
                              List.<Integer>of(), 1, units, i -> i, BoundedFanout.NEVER_CANCELLED)),
              FAILURES);
      assertTrue(thrown.getMessage().contains("BoundedFanout.forEachOrdered"), thrown.getMessage());
    } finally {
      runner.close();
    }
  }

  @Test
  void theSharedRuntimeIsUsableAgainAfterAnApplicationRestart() {
    // Quarkus reuses the runtime classloader across dev-mode reloads and @QuarkusTest restarts, and
    // `closed` is sticky. A static-final holder therefore handed every call in the restarted
    // application a closed runtime, so the whole tier failed until the JVM exited.
    MetadataIoRunner before = new MetadataIoRunner();
    assertEquals("ok", before.callWithoutCancellation(() -> "ok", FAILURES));

    MetadataIoRunner.closeSharedRuntimeIfStarted(); // the ShutdownEvent observer
    MetadataIoRunner.reopenSharedRuntime(); // the next lifecycle's StartupEvent observer

    MetadataIoRunner afterRestart = new MetadataIoRunner();
    assertEquals(
        "ok",
        afterRestart.callWithoutCancellation(() -> "ok", FAILURES),
        "a restarted application must get a fresh runtime, not the closed one");
    // shared() is the sibling entry point: it used to cache a facade whose RuntimeState was
    // captured once, so it kept serving the closed runtime after a restart while the constructor
    // recovered.
    assertEquals("ok", MetadataIoRunner.shared().callWithoutCancellation(() -> "ok", FAILURES));
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    MetadataIoRunner.reopenSharedRuntime();
  }

  @Test
  void noRuntimeIsBuiltAfterShutdownHasBegun() {
    // A call arriving inside the shutdown window used to install a replacement runtime that
    // outlived the shutdown, starting fresh platform threads after the ShutdownEvent and leaving
    // nothing to reclaim them.
    new MetadataIoRunner().callWithoutCancellation(() -> "warm", FAILURES);
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    try {
      // Construction itself resolves nothing (see runtime()), so the rejection lands at the point
      // of use. Asserting it here rather than on the constructor keeps the test on the invariant —
      // no replacement runtime is installed — instead of on where the resolution happens to occur.
      MetadataIoRunner late = new MetadataIoRunner();
      assertThrows(
          RejectedExecutionException.class,
          () -> late.callWithoutCancellation(() -> "late", FAILURES));
    } finally {
      MetadataIoRunner.reopenSharedRuntime();
    }
  }

  @Test
  void aNestedReadDuringShutdownOfTheSharedRuntimeSurfacesAsCancellation() {
    // The sibling test uses an explicit-capacity runtime, which resolves through a field and never
    // touches the shutdown latch. Only the shared runtime — the production one — takes the path
    // where a drain in progress has to answer a re-entrancy check with something other than a
    // rejection, and the contract for a call already in flight is cancellation.
    try {
      Throwable nested =
          new MetadataIoRunner()
              .callWithoutCancellation(
                  () -> {
                    MetadataIoRunner.closeSharedRuntimeIfStarted();
                    // close() runs shutdownNow on this worker; clear it so the closure check, not
                    // the interrupt check, is what answers.
                    Thread.interrupted();
                    return assertThrows(
                        Throwable.class,
                        () ->
                            new MetadataIoRunner()
                                .callWithoutCancellation(() -> "nested", FAILURES));
                  },
                  FAILURES);
      assertInstanceOf(CancellationException.class, nested);
      assertEquals(CancellableCallRunner.RUNTIME_CLOSED, nested.getMessage());
    } finally {
      MetadataIoRunner.reopenSharedRuntime();
    }
  }

  @Test
  void theAdmissionGaugesKeepReadingThroughTheDrainWindow() {
    // These three feed Micrometer gauges. A throw here is not surfaced — DefaultGauge catches it
    // and publishes NaN, with a warning per scrape — so the whole graceful-shutdown drain would
    // report no capacity, no usage and no waiters at the one moment an operator is watching.
    MetadataIoRunner gauges = new MetadataIoRunner();
    gauges.callWithoutCancellation(() -> "warm", FAILURES);
    int capacityBefore = gauges.capacity();
    try {
      MetadataIoRunner.closeSharedRuntimeIfStarted();

      assertEquals(capacityBefore, gauges.capacity(), "capacity is a configured value, not state");
      assertEquals(0, gauges.permitsInUse(), "the drained runtime holds nothing");
      assertEquals(0, gauges.admissionWaiters(), "nothing can be queued behind a closed runtime");
    } finally {
      MetadataIoRunner.reopenSharedRuntime();
    }
  }

  @Test
  void aNestedReadDuringShutdownSurfacesAsCancellationNotRejection() {
    // The re-entrant path only runs on a thread already holding a permit, so it is in flight by
    // construction. Closure there is a cancellation; reporting a rejected submission would put a
    // spurious INTERNAL beside the clean cancellations an identical non-nested call produces.
    var runner = new MetadataIoRunner(1);
    try {
      Throwable nested =
          runner.callWithoutCancellation(
              () -> {
                runner.close(); // latch closure while this admitted call is running
                // close() runs shutdownNow on this worker, so this thread is also interrupted.
                // Clearing it leaves the closure check as the only gate that can fire — without
                // this the test passes off the interrupt branch and says nothing about closure.
                Thread.interrupted();
                return assertThrows(
                    Throwable.class,
                    () -> runner.callWithoutCancellation(() -> "nested", FAILURES));
              },
              FAILURES);
      assertInstanceOf(CancellationException.class, nested);
      assertEquals(CancellableCallRunner.RUNTIME_CLOSED, nested.getMessage());
    } finally {
      runner.close();
    }
  }

  @Test
  void aFanOutDispatchedFromInsideAnAdmittedOperationIsRefused() {
    // The deadlock this prevents: units dispatched to other threads acquire their own permits while
    // the outer operation still holds one, so `capacity` such operations wedge the whole ceiling.
    var runner = new MetadataIoRunner(1);
    try {
      IllegalStateException thrown =
          runner.callWithoutCancellation(
              () -> {
                assertTrue(
                    MetadataIoRunner.isRunningAdmittedOperation(),
                    "precondition: the operation body runs marked as admitted");
                return assertThrows(
                    IllegalStateException.class,
                    () -> MetadataIoRunner.rejectFanOutFromAdmittedOperation("TestFanout"));
              },
              FAILURES);
      assertTrue(thrown.getMessage().contains("TestFanout"), thrown.getMessage());
    } finally {
      runner.close();
    }
  }

  @Test
  void aRealFanOutStartedInsideAnAdmittedOperationIsRefused() {
    // The guard wired at BoundedFanout's single dispatch point, exercised through the real API
    // rather than by calling the predicate directly: this is the shape that would wedge the
    // ceiling, so it must fail loudly instead of deadlocking.
    var runner = new MetadataIoRunner(1);
    try (ExecutorService units = Executors.newFixedThreadPool(2)) {
      IllegalStateException thrown =
          runner.callWithoutCancellation(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          BoundedFanout.mapOrdered(
                              List.of(1, 2), 2, units, i -> i, BoundedFanout.NEVER_CANCELLED)),
              FAILURES);
      assertTrue(
          thrown.getMessage().contains("BoundedFanout.forEachOrdered"),
          "the failure must name the dispatch site: " + thrown.getMessage());
    } finally {
      runner.close();
    }
  }

  @Test
  void aFanOutDispatchedOutsideAnyAdmittedOperationIsAllowed() {
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
    assertDoesNotThrow(() -> MetadataIoRunner.rejectFanOutFromAdmittedOperation("TestFanout"));
  }

  @Test
  void theConcurrencyKnobIsDeclaredUnderTheExactKeyTheRunnerReads() throws Exception {
    // The clamp test below exercises clampConfiguredCapacity but never the property NAME, so a
    // spelling drift between the constant and application.properties would go unnoticed: SmallRye
    // treats floecat.query.metadata-io.max-concurrency and ...metadata_io.max_concurrency as
    // different keys, so an operator lowering the ceiling would silently keep the 64 default.
    // Kebab-case is the repo convention (every other declared key uses it).
    assertEquals(
        "floecat.query.metadata-io.max-concurrency", MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);

    // Enumerate every application.properties on the classpath rather than taking the first: the
    // test-resources copy shadows the main one, so getResourceAsStream would read the wrong file.
    boolean declaredSomewhere = false;
    var resources =
        MetadataIoRunnerTest.class.getClassLoader().getResources("application.properties");
    assertTrue(resources.hasMoreElements(), "no application.properties on the test classpath");
    while (resources.hasMoreElements()) {
      try (var in = resources.nextElement().openStream()) {
        String body = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (body.contains(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY + "=")) {
          declaredSomewhere = true;
          break;
        }
      }
    }
    assertTrue(
        declaredSomewhere,
        "the knob must be declared in application.properties so it is discoverable, under the same"
            + " key the runner reads");
  }

  @Test
  void aMalformedCapacityValueFailsStartupRatherThanDefaultingSilently() {
    // configuredCapacity() falls back so a config failure cannot break every later admission; the
    // validator is what turns a typo into a failed deployment instead of a silent 64-permit
    // ceiling. System properties are a SmallRye config source, so this exercises the real lookup.
    System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, "sixty-four");
    try {
      IllegalStateException thrown =
          assertThrows(IllegalStateException.class, MetadataIoRunner::validateConfiguredCapacity);
      assertTrue(
          thrown.getMessage().contains(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY),
          "the failure must name the property so an operator can find it");
      assertTrue(
          thrown.getMessage().contains("sixty-four"),
          "the failure must quote the offending value: " + thrown.getMessage());
    } finally {
      System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    }
  }

  @Test
  void anAbsentOrInRangeCapacityValuePassesValidation() {
    System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    assertDoesNotThrow(MetadataIoRunner::validateConfiguredCapacity);

    System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, "8");
    try {
      assertDoesNotThrow(MetadataIoRunner::validateConfiguredCapacity);
    } finally {
      System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "100000"})
  void anOutOfRangeCapacityValueFailsStartupRatherThanBeingClamped(String configured) {
    // Clamping is silent, and the low end is the dangerous one: 0 or negative clamps to a single
    // permit, serialising every metadata round trip behind it with one WARN line as the only trace.
    System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, configured);
    try {
      IllegalStateException thrown =
          assertThrows(IllegalStateException.class, MetadataIoRunner::validateConfiguredCapacity);
      assertTrue(
          thrown.getMessage().contains(configured),
          "the failure must quote the offending value: " + thrown.getMessage());
    } finally {
      System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    }
  }

  @Test
  void aClosedRuntimeStaysClosedAndReportsItself() {
    // Shutdown is one-way: closed is sticky with no reopen path, so a runner handed a closed
    // runtime must reject work rather than quietly building a second pool.
    var runner = new MetadataIoRunner(1);
    runner.start();
    runner.close();

    assertThrows(RejectedExecutionException.class, runner::start);
    // Also covered here rather than in a near-duplicate test: a runtime closed without ever having
    // started must stay closed too, so close() cannot be mistaken for a no-op on an unused runner.
    var neverStarted = new MetadataIoRunner(1);
    neverStarted.close();
    assertThrows(RejectedExecutionException.class, neverStarted::start);
    assertThrows(
        RejectedExecutionException.class,
        () -> runner.callWithoutCancellation(() -> "unreachable", FAILURES));
  }

  @Test
  void closingTheRuntimeReleasesACallerParkedOnAnUninterruptibleStoreCall() throws Exception {
    // shutdownNow interrupts workers and returns only queued tasks, so a store call that ignores
    // its interrupt keeps running. The caller must be released by the closure signal rather than
    // waiting for that call to finish.
    //
    // Dedicated threads, not CompletableFuture: the common pool is shared with the rest of the
    // suite, so a queued task there can miss the deadline for reasons unrelated to this behaviour.
    var runner = new MetadataIoRunner(1);
    var blocker = new UninterruptibleBlocker();
    var outcome = new AtomicReference<Throwable>();
    Thread caller =
        new Thread(
            () -> {
              try {
                runner.callWithoutCancellation(
                    () -> {
                      blocker.await();
                      return "done";
                    },
                    FAILURES);
              } catch (Throwable t) {
                outcome.set(t);
              }
            },
            "admitted-caller");
    Thread closer = new Thread(runner::close, "closer");
    try {
      caller.start();
      await(blocker.started);

      // close() blocks for the shutdown await while the task runs on, so it cannot be inline: the
      // caller must be released well before shutdown finishes.
      closer.start();
      caller.join(TimeUnit.SECONDS.toMillis(10));

      assertFalse(caller.isAlive(), "closure must release the caller parked on the store call");
      assertInstanceOf(CancellationException.class, outcome.get());
    } finally {
      blocker.release.countDown();
      closer.join(TimeUnit.SECONDS.toMillis(10));
    }
  }

  @Test
  void closingTheRuntimeRejectsACallerWaitingForAdmission() throws Exception {
    // A closed runtime releases no further permits, so an admission wait would never end.
    var runner = new MetadataIoRunner(1);
    var blocker = new UninterruptibleBlocker();
    var outcome = new AtomicReference<Throwable>();
    var holderOutcome = new AtomicReference<Throwable>();
    Thread holder =
        new Thread(
            () -> {
              try {
                runner.callWithoutCancellation(
                    () -> {
                      blocker.await();
                      return "holder";
                    },
                    FAILURES);
              } catch (Throwable expected) {
                // The close under test cancels this call too. Swallowed rather than left to the
                // default handler, which printed a stack trace on every run.
                holderOutcome.set(expected);
              }
            },
            "permit-holder");
    Thread waiter =
        new Thread(
            () -> {
              try {
                runner.callWithoutCancellation(() -> "queued", FAILURES);
              } catch (Throwable t) {
                outcome.set(t);
              }
            },
            "admission-waiter");
    Thread closer = new Thread(runner::close, "closer");
    try {
      holder.start();
      await(blocker.started);
      waiter.start();
      // Deterministic barrier: close only once the waiter is genuinely parked on the semaphore.
      awaitAdmissionWaiter(runner);

      closer.start();
      waiter.join(TimeUnit.SECONDS.toMillis(10));

      assertFalse(waiter.isAlive(), "closure must release the caller waiting for admission");
      assertInstanceOf(RejectedExecutionException.class, outcome.get());
    } finally {
      blocker.release.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(10));
      closer.join(TimeUnit.SECONDS.toMillis(10));
    }
  }

  /** Block until a caller is parked waiting for admission, so shutdown races nothing. */
  private static void awaitAdmissionWaiter(MetadataIoRunner runner) throws InterruptedException {
    for (int attempt = 0; attempt < 500; attempt++) {
      if (runner.admissionWaiters() > 0) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("no caller ever parked waiting for admission");
  }

  @Test
  void configuredProcessCapacityIsClampedToSafeBounds() {
    assertEquals(1, MetadataIoRunner.clampConfiguredCapacity(Integer.MIN_VALUE));
    assertEquals(64, MetadataIoRunner.clampConfiguredCapacity(64));
    assertEquals(256, MetadataIoRunner.clampConfiguredCapacity(Integer.MAX_VALUE));
  }

  @Test
  void defaultConstructionSharesBoundedProcessRuntimeOutsideCdi() {
    MetadataIoRunner direct = new MetadataIoRunner();
    MetadataIoRunner shared = MetadataIoRunner.shared();

    assertTrue(direct.sharesRuntimeWith(shared));
    String workerName =
        direct.callWithoutCancellation(() -> Thread.currentThread().getName(), FAILURES);
    assertTrue(workerName.startsWith("floecat-metadata-io-"), workerName);
  }

  @Test
  void oneRunnerAppliesOneAdmissionCeilingAcrossCallers() throws Exception {
    var runner = new MetadataIoRunner(1);
    var firstStarted = new CountDownLatch(1);
    var releaseFirst = new CountDownLatch(1);
    var secondStarted = new CountDownLatch(1);
    runner.start();
    try {
      CompletableFuture<String> first =
          CompletableFuture.supplyAsync(
              () ->
                  runner.call(
                      () -> false,
                      () -> {
                        firstStarted.countDown();
                        await(releaseFirst);
                        return "first";
                      },
                      FAILURES));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<String> second =
          CompletableFuture.supplyAsync(
              () ->
                  runner.call(
                      () -> false,
                      () -> {
                        secondStarted.countDown();
                        return "second";
                      },
                      FAILURES));

      assertFalse(secondStarted.await(50, TimeUnit.MILLISECONDS));
      releaseFirst.countDown();
      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertEquals("second", second.get(1, TimeUnit.SECONDS));
    } finally {
      releaseFirst.countDown();
      runner.close();
    }
  }

  @Test
  void cancelledCallerReturnsWhileWaitingForSharedAdmission() throws Exception {
    var runner = new MetadataIoRunner(1);
    var blocker = new UninterruptibleBlocker();
    var cancelled = new AtomicBoolean();
    var waitingOperationStarted = new AtomicBoolean();
    runner.start();
    try {
      CompletableFuture<String> active =
          CompletableFuture.supplyAsync(
              () ->
                  runner.call(
                      () -> false,
                      () -> {
                        blocker.await();
                        return "active";
                      },
                      FAILURES));
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> waiting =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  runner.call(
                      cancelled::get,
                      () -> {
                        waitingOperationStarted.set(true);
                        return "waiting";
                      },
                      FAILURES);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      cancelled.set(true);

      assertInstanceOf(CancellationException.class, waiting.get(250, TimeUnit.MILLISECONDS));
      assertFalse(waitingOperationStarted.get());
      blocker.release.countDown();
      assertEquals("active", active.get(1, TimeUnit.SECONDS));
    } finally {
      blocker.release.countDown();
      runner.close();
    }
  }

  @Test
  void reentrantCancellableCallDoesNotStartNestedWorkAfterCancellation() {
    // Re-entrant admission reuses the held permit and runs inline, but must still honor
    // cancellation: an outer admitted call that ignores its cancellation interrupt must not be able
    // to start a new nested store round-trip once the request has cancelled. The nested supplier
    // must not run.
    var runner = new MetadataIoRunner(1);
    var cancelled = new AtomicBoolean(false);
    var nestedRan = new AtomicBoolean(false);
    try {
      assertThrows(
          CancellationException.class,
          () ->
              runner.call(
                  cancelled::get,
                  () -> {
                    cancelled.set(true); // request cancels while the outer op holds the permit
                    return runner.call(
                        cancelled::get,
                        () -> {
                          nestedRan.set(true);
                          return "nested";
                        },
                        FAILURES);
                  },
                  FAILURES));
      assertFalse(nestedRan.get(), "a cancelled request must not start nested store work");
    } finally {
      runner.close();
    }
  }

  @Test
  void nestedAdmissionReusesTheHeldPermitInsteadOfDeadlocking() {
    // Re-entrant admission: a store call reached from within an admitted operation on the same
    // thread reuses the one held permit and runs inline. With capacity 1 a second acquire would
    // block forever, so returning within the timeout proves the permit was reused, not re-acquired.
    var runner = new MetadataIoRunner(1);
    try {
      String result =
          assertTimeoutPreemptively(
              Duration.ofSeconds(5),
              () ->
                  runner.call(
                      () -> false,
                      () -> runner.call(() -> false, () -> "inner", FAILURES),
                      FAILURES));
      assertEquals("inner", result);
    } finally {
      runner.close();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted");
    }
  }
}
