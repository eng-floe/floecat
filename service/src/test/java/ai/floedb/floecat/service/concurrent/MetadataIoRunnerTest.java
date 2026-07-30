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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Verifies process-wide admission shared by metadata callers. */
class MetadataIoRunnerTest {
  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

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

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted");
    }
  }
}
