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

import static ai.floedb.floecat.service.testsupport.ConcurrentTestSupport.awaitUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.context.PropagatedContext;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Verifies explicit repository reads run under admission and propagated cancellation. */
class MetadataResourceReaderTest {

  @Test
  void readRunsUnderAdmission() throws Exception {
    withReader(
        1,
        reads -> {
          AtomicBoolean admittedDuringCall = new AtomicBoolean();
          assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
          String result =
              reads.read(
                  () -> {
                    admittedDuringCall.set(MetadataIoRunner.isRunningAdmittedOperation());
                    return "value";
                  });
          assertEquals("value", result);
          assertTrue(admittedDuringCall.get(), "the read must run inside an admitted operation");
          assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
        });
  }

  @Test
  void nestedReadsReuseTheOnePermit() throws Exception {
    withReader(
        1,
        reads -> {
          String result =
              assertTimeoutPreemptively(
                  Duration.ofSeconds(5),
                  () -> reads.read(() -> reads.read(() -> "inner")),
                  "nested admission deadlocked instead of reusing the held permit");
          assertEquals("inner", result);
        });
  }

  @Test
  void readHonorsThePropagatedCancellationSignal() throws Exception {
    withReader(
        1,
        reads -> {
          AtomicBoolean readRan = new AtomicBoolean(false);
          try (PropagatedContext.CancellationScope ignored =
              PropagatedContext.bindCancellation(() -> true)) {
            assertThrows(
                CancellationException.class,
                () ->
                    reads.read(
                        () -> {
                          readRan.set(true);
                          return "unreachable";
                        }));
            assertFalse(readRan.get(), "an already-cancelled request must not run the store read");
          }
        });
  }

  @Test
  void blockedReadIsAbandonedWhenTheRequestCancels() throws Exception {
    withReader(
        2,
        reads -> {
          AtomicBoolean cancelled = new AtomicBoolean(false);
          CountDownLatch started = new CountDownLatch(1);
          CountDownLatch release = new CountDownLatch(1);
          ExecutorService caller = Executors.newSingleThreadExecutor();
          try {
            Future<Throwable> call =
                caller.submit(
                    () -> {
                      try (PropagatedContext.CancellationScope ignored =
                          PropagatedContext.bindCancellation(cancelled::get)) {
                        reads.read(
                            () -> {
                              started.countDown();
                              awaitUninterruptibly(release);
                              return "unreachable";
                            });
                        return null;
                      } catch (Throwable failure) {
                        return failure;
                      }
                    });
            assertTrue(started.await(1, TimeUnit.SECONDS));
            cancelled.set(true);
            assertInstanceOf(CancellationException.class, call.get(1, TimeUnit.SECONDS));
          } finally {
            release.countDown();
            caller.shutdownNow();
          }
        });
  }

  /** Run a test with one isolated metadata reader and always close its worker pool. */
  private static void withReader(int capacity, ReaderTest test) throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(capacity);
    try {
      test.run(new MetadataResourceReader(runner));
    } finally {
      runner.close();
    }
  }

  /** Test body allowed to use blocking assertions that throw checked exceptions. */
  @FunctionalInterface
  private interface ReaderTest {
    void run(MetadataResourceReader reads) throws Exception;
  }
}
