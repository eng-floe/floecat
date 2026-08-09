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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the startup and shutdown observers directly. Booting the container to reach them would
 * pull in the whole application; what needs pinning is the order of the statements inside each
 * method, which is observable without CDI.
 */
class MetadataIoLifecycleTest {

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

  private final MetadataIoLifecycle lifecycle = new MetadataIoLifecycle();

  /**
   * These call the real observers, which re-arm an explicitly closed runtime and drop the telemetry
   * sink. Restore both for the plain unit tests that run after this class in the same classloader.
   */
  @AfterEach
  void restoreProcessWideState() {
    MetadataIoRunner.reopenSharedRuntime();
    MetadataIoRunner.clearSaturationSink();
  }

  @Test
  void startupClearsTheShutdownLatch() {
    // The latch is sticky until startup re-arms the shared runtime for the next lifecycle.
    MetadataIoRunner.closeSharedRuntimeIfStarted();
    assertThrows(
        RejectedExecutionException.class,
        () -> new MetadataIoRunner().callWithoutCancellation(() -> "before", FAILURES));

    lifecycle.validateMetadataIoConfig(null);

    assertEquals("after", new MetadataIoRunner().callWithoutCancellation(() -> "after", FAILURES));
  }

  @Test
  void startupRejectsAMalformedCeiling() {
    String previous = System.getProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
    try {
      System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, "sixty-four");
      assertThrows(IllegalStateException.class, () -> lifecycle.validateMetadataIoConfig(null));
    } finally {
      if (previous == null) {
        System.clearProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY);
      } else {
        System.setProperty(MetadataIoRunner.MAX_CONCURRENCY_PROPERTY, previous);
      }
    }
  }

  @Test
  void shutdownDropsTheTelemetrySinkAndLeavesTheRuntimeAvailableForTeardown() {
    // The sink closes over this container's beans and lives in a static that outlives a dev-mode
    // reload, so leaving it installed sends later increments to a dead bean.
    var hits = new java.util.concurrent.atomic.AtomicInteger();
    MetadataIoRunner.setSaturationSink(hits::incrementAndGet);
    new MetadataIoRunner().callWithoutCancellation(() -> "warm", FAILURES);

    lifecycle.clearSaturationSinkAtShutdown(null);

    assertEquals("late", new MetadataIoRunner().callWithoutCancellation(() -> "late", FAILURES));
    // Saturate for real: an uncontended call never reaches the sink, so it would report 0 whether
    // or not shutdown dropped it.
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
      assertTrue(blocker.started.await(5, TimeUnit.SECONDS));
      Thread waiter =
          new Thread(() -> runner.callWithoutCancellation(() -> "queued", FAILURES), "sink-waiter");
      waiter.start();
      Thread.sleep(100); // let the waiter reach the saturated path
      blocker.release.countDown();
      waiter.join(TimeUnit.SECONDS.toMillis(10));
      holder.join(TimeUnit.SECONDS.toMillis(10));
      assertEquals(0, hits.get(), "shutdown must have dropped the sink");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    } finally {
      blocker.release.countDown();
      runner.close();
    }
  }
}
