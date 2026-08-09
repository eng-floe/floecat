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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
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

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "\t"})
  void aBlankConfiguredCapacityFailsStartup(String blank) {
    // A stray "FLOECAT_QUERY_METADATA_IO_MAX_CONCURRENCY=" must not boot on 64 after the operator
    // explicitly tried to configure another ceiling.
    withCapacityProperty(
        blank,
        () ->
            assertThrows(
                IllegalStateException.class, MetadataIoRunner::validateConfiguredCapacity));
  }

  @Test
  void aRestartCannotExceedTheCeilingWhileAnOldCallIsStillRunning() {
    // close() gives up after its timeout when a store call ignores interruption. A restart must not
    // treat that timed-out executor as a completed lifecycle: it rejects new work until the old
    // worker has actually stopped, rather than creating a parallel admission executor.
    var blocker = new UninterruptibleBlocker();
    var nestedFailure = new AtomicReference<Throwable>();
    var nestedFinished = new java.util.concurrent.CountDownLatch(1);
    Thread survivor = null;
    try {
      survivor =
          new Thread(
              () -> {
                try {
                  new MetadataIoRunner()
                      .callWithoutCancellation(
                          () -> {
                            blocker.await();
                            try {
                              new MetadataIoRunner()
                                  .callWithoutCancellation(() -> "nested", FAILURES);
                            } catch (Throwable failure) {
                              nestedFailure.set(failure);
                            } finally {
                              nestedFinished.countDown();
                            }
                            return "survivor";
                          },
                          FAILURES);
                } catch (Throwable expected) {
                  // The shutdown below cancels this call; its permit is what matters here.
                }
              },
              "restart-survivor");
      survivor.start();
      assertTrue(blocker.started.await(5, TimeUnit.SECONDS), "the first call must be admitted");

      // Shut down and restart while that call is still holding its permit.
      MetadataIoRunner.closeSharedRuntimeIfStarted();
      // A repeated close must keep the first timed-out executor as the drain owner; otherwise it
      // would overwrite closingExecutor with null and the reopen below could mint a replacement.
      MetadataIoRunner.closeSharedRuntimeIfStarted();
      MetadataIoRunner.reopenSharedRuntime();

      MetadataIoRunner restarted = new MetadataIoRunner();
      assertThrows(
          RejectedExecutionException.class,
          () -> restarted.callWithoutCancellation(() -> "contender", FAILURES),
          "a restarted lifecycle must not admit work while the previous executor is still running");

      blocker.release.countDown();
      assertTrue(
          nestedFinished.await(10, TimeUnit.SECONDS),
          "a nested call from the retired runtime must not wait for its own permit");
      assertInstanceOf(CancellationException.class, nestedFailure.get());
      survivor.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(survivor.isAlive(), "the old executor must terminate once the blocker releases");
      assertEquals("ok", awaitUsableAfterDrain(restarted));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    } finally {
      blocker.release.countDown();
      if (survivor != null) {
        try {
          survivor.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      MetadataIoRunner.closeSharedRuntimeIfStarted();
      MetadataIoRunner.reopenSharedRuntime();
    }
  }

  @Test
  void theAdmissionGaugesReadAnUntouchedRuntimeThroughTheDrainWindow() {
    // The likely shape today, not an edge case: no caller routes store I/O through this tier yet,
    // so a process that shuts down before its first scrape never resolved a runtime at all. The
    // gauges are still registered, and with nothing installed they would publish NaN.
    // Get to the state a fresh process is in: close whatever an earlier test installed, then
    // reopen to drop the dead reference, leaving SHARED genuinely empty. Without this the runtime
    // an earlier test left behind satisfies the gauges and the test passes without ever reaching
    // the case it names.
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    MetadataIoRunner.reopenSharedRuntime();
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    try {
      MetadataIoRunner untouched = new MetadataIoRunner();
      assertTrue(untouched.capacity() > 0, "capacity is configured, not observed");
      assertEquals(0, untouched.permitsInUse());
      assertEquals(0, untouched.admissionWaiters());
    } finally {
      MetadataIoRunner.reopenSharedRuntime();
    }
  }

  /** Run one assertion with a temporary metadata-I/O capacity property value. */
  private static void withCapacityProperty(String value, Runnable body) {
    String previous = System.getProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    try {
      System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, value);
      body.run();
    } finally {
      if (previous == null) {
        System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
      } else {
        System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, previous);
      }
    }
  }

  @Test
  void anUnresolvableConfiguredCapacityFailsStartup() {
    // ConfigValue exposes an unresolved expression as a null resolved value. Treat that as invalid
    // rather than silently accepting a ceiling different from the one the operator supplied.
    withCapacityProperty(
        "${FLOECAT_NO_SUCH_VAR_FOR_THIS_TEST}",
        () ->
            assertThrows(
                IllegalStateException.class, MetadataIoRunner::validateConfiguredCapacity));
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
    // A CDI-scoped facade outlives individual runtimes, so each call must resolve the replacement
    // after sticky closure rather than retaining the runtime captured at construction.
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
    // The telemetry and lifecycle beans install and clear this process-wide callback.
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

  /** Wait until a saturated admission reports through the installed telemetry sink. */
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
  void theSharedRuntimeIsUsableAgainAfterAnApplicationRestart() {
    // `closed` is sticky, so each restarted application lifecycle must resolve a fresh runtime.
    MetadataIoRunner before = new MetadataIoRunner();
    assertEquals("ok", before.callWithoutCancellation(() -> "ok", FAILURES));

    MetadataIoRunner.closeSharedRuntimeIfStarted(); // an embedding-controlled lifecycle transition
    MetadataIoRunner.reopenSharedRuntime(); // the next lifecycle's startup hook

    MetadataIoRunner afterRestart = new MetadataIoRunner();
    assertEquals(
        "ok",
        afterRestart.callWithoutCancellation(() -> "ok", FAILURES),
        "a restarted application must get a fresh runtime, not the closed one");
    // The shared() entry point must resolve the same fresh runtime as direct construction.
    assertEquals("ok", MetadataIoRunner.shared().callWithoutCancellation(() -> "ok", FAILURES));
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    MetadataIoRunner.reopenSharedRuntime();
  }

  @Test
  void anIdleWorkerDoesNotRetainTheSubmittingApplicationClassloader() {
    // The shared runtime deliberately stays open through CDI teardown so later @PreDestroy
    // consumers can finish. Its workers therefore must not keep an old application classloader
    // alive merely while they are idle between a dev-mode reload and their keep-alive timeout.
    var runner = new MetadataIoRunner(1);
    var worker = new AtomicReference<Thread>();
    try {
      assertEquals(
          "ok",
          runner.callWithoutCancellation(
              () -> {
                worker.set(Thread.currentThread());
                return "ok";
              },
              FAILURES));

      assertEquals(
          ClassLoader.getPlatformClassLoader(),
          worker.get().getContextClassLoader(),
          "an idle worker must not retain the submitting application classloader");
    } finally {
      runner.close();
    }
  }

  @Test
  void noRuntimeIsBuiltAfterShutdownHasBegun() {
    // A call arriving during controlled close must not install a replacement runtime after the
    // latch goes up.
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
  void theConcurrencyKnobIsDeclaredUnderTheExactKeyTheRunnerReads() throws Exception {
    // The direct-construction validation tests below never exercise the property name, so a
    // spelling
    // drift between the constant and application.properties would go unnoticed: SmallRye
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
  void theProcessWideAdmissionHolderIsConfiguredParentFirst() throws Exception {
    // The core holder is intentionally the only application-owned static that spans Quarkus
    // reloads. This assertion protects the configuration link that makes the loader arrangement
    // exercised by ProcessWideAdmissionTest match the one used by Quarkus.
    String expected =
        "quarkus.class-loading.parent-first-artifacts=ai.floedb.floecat:floecat-core-engine-utils";
    java.nio.file.Path testClasses =
        java.nio.file.Path.of(
            MetadataIoRunnerTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    java.nio.file.Path deployedProperties =
        testClasses.resolveSibling("classes/application.properties");
    assertTrue(
        java.nio.file.Files.isRegularFile(deployedProperties),
        "the deployed application.properties must be available to this test");
    String body = java.nio.file.Files.readString(deployedProperties);
    assertTrue(
        body.contains(expected),
        "the deployed ProcessWideAdmission holder must remain parent-first across reloads");
  }

  @Test
  void aMalformedCapacityValueFailsStartupRatherThanDefaultingSilently() {
    // System properties are a SmallRye config source, so this exercises the real lookup.
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

  @ParameterizedTest
  @ValueSource(strings = {"", "sixty-four", "0", "257", "${FLOECAT_NO_SUCH_VAR_FOR_THIS_TEST}"})
  void directConstructionRejectsAnInvalidConfiguredCapacity(String configured) {
    String previous = System.getProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    try {
      // Make the next public/default call resolve a runtime rather than inheriting one from a
      // preceding test. It must validate the same configuration as CDI startup.
      MetadataIoRunner.closeSharedRuntimeIfStarted();
      MetadataIoRunner.reopenSharedRuntime();
      System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, configured);

      assertThrows(
          IllegalStateException.class,
          () -> new MetadataIoRunner().callWithoutCancellation(() -> "unreachable", FAILURES));
    } finally {
      if (previous == null) {
        System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
      } else {
        System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, previous);
      }
      MetadataIoRunner.closeSharedRuntimeIfStarted();
      MetadataIoRunner.reopenSharedRuntime();
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
  void sameRuntimeNestedCallsReuseTheHeldPermit() {
    var runner = new MetadataIoRunner(1);
    try {
      assertEquals(
          "without-cancellation",
          runner.callWithoutCancellation(
              () -> runner.callWithoutCancellation(() -> "without-cancellation", FAILURES),
              FAILURES));
      assertEquals(
          "cancellable",
          runner.call(
              () -> false,
              () -> runner.call(() -> false, () -> "cancellable", FAILURES),
              FAILURES));
    } finally {
      runner.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void cancelledNestedCallCannotBeRetriedBeforeTheOuterOperationReturns(boolean cancellableRetry)
      throws Exception {
    var runner = new MetadataIoRunner(1);
    var blocker = new UninterruptibleBlocker();
    var cancelled = new AtomicBoolean();
    var nestedOperationReturned = new CountDownLatch(1);
    var retryRejected = new CountDownLatch(1);
    var allowOuterReturn = new CountDownLatch(1);
    var contenderStarted = new CountDownLatch(1);
    runner.start();
    try {
      CompletableFuture<String> outer =
          CompletableFuture.supplyAsync(
              () ->
                  runner.callWithoutCancellation(
                      () -> {
                        try {
                          runner.call(
                              cancelled::get,
                              () -> {
                                blocker.await();
                                nestedOperationReturned.countDown();
                                return "nested";
                              },
                              FAILURES);
                        } catch (CancellationException expected) {
                          // Retry is intentionally attempted before this outer operation returns.
                        }
                        IllegalStateException rejection =
                            assertThrows(
                                IllegalStateException.class,
                                () -> {
                                  Supplier<String> retry = () -> "unexpected";
                                  if (cancellableRetry) {
                                    runner.call(() -> false, retry, FAILURES);
                                  } else {
                                    runner.callWithoutCancellation(retry, FAILURES);
                                  }
                                });
                        retryRejected.countDown();
                        await(allowOuterReturn);
                        return rejection.getMessage();
                      },
                      FAILURES));
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      cancelled.set(true);
      assertTrue(
          retryRejected.await(250, TimeUnit.MILLISECONDS),
          "same-runtime retry must reject rather than wait for fresh admission");
      assertEquals(1, runner.permitsInUse());

      blocker.release.countDown();
      assertTrue(nestedOperationReturned.await(1, TimeUnit.SECONDS));

      CompletableFuture<String> contender =
          CompletableFuture.supplyAsync(
              () ->
                  runner.callWithoutCancellation(
                      () -> {
                        contenderStarted.countDown();
                        return "contender";
                      },
                      FAILURES));
      assertFalse(
          contenderStarted.await(50, TimeUnit.MILLISECONDS),
          "the outer operation must retain admission after its abandoned child exits");

      allowOuterReturn.countDown();
      assertTrue(outer.get(1, TimeUnit.SECONDS).contains("outer admitted operation"));
      assertEquals("contender", contender.get(1, TimeUnit.SECONDS));
    } finally {
      blocker.release.countDown();
      allowOuterReturn.countDown();
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

  /** Await a test barrier while preserving cancellation-style interruption semantics. */
  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted");
    }
  }

  /** Retry through the bounded shutdown drain until the replacement runtime becomes usable. */
  private static String awaitUsableAfterDrain(MetadataIoRunner runner) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try {
        return runner.callWithoutCancellation(() -> "ok", FAILURES);
      } catch (RejectedExecutionException stillDraining) {
        if (System.nanoTime() >= deadline) {
          throw stillDraining;
        }
        java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
      }
    }
  }
}
