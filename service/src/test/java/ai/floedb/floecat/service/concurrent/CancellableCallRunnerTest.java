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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Exercises admission, cancellation ownership, and executor-discard races for blocking calls. */
class CancellableCallRunnerTest {

  private static final CancellableCallRunner.FailureMessages CALL_FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

  @Test
  void rejectsAnExecutorThatRunsTheBlockingCallInline() {
    Executor directExecutor = Runnable::run;
    var permits = new Semaphore(1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CancellableCallRunner.call(
                directExecutor, permits, () -> false, () -> "must not run", CALL_FAILURES));
    assertEquals(1, permits.availablePermits());
  }

  @Test
  void callWithoutCancellationWaitsForTheEventualResult() throws Exception {
    ExecutorService metadataExecutor = Executors.newSingleThreadExecutor();
    ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
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
                      () -> {
                        started.countDown();
                        try {
                          release.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return "done";
                      },
                      CALL_FAILURES),
              callerExecutor);
      assertTrue(started.await(1, TimeUnit.SECONDS));
      assertThrows(
          java.util.concurrent.TimeoutException.class, () -> result.get(25, TimeUnit.MILLISECONDS));
      assertEquals(0, permits.availablePermits(), "active I/O must retain its admission slot");
      release.countDown();
      assertEquals("done", result.get(1, TimeUnit.SECONDS));
      assertEquals(1, permits.availablePermits());
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
                      CALL_FAILURES));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> queued =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor, permits, () -> false, () -> "queued", CALL_FAILURES);
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
      assertEquals(1, permits.availablePermits(), "the discarded call must release its permit");

      allowFirst.countDown();
      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
      assertEquals(2, permits.availablePermits());
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
                      () -> {
                        firstStarted.countDown();
                        try {
                          allowFirst.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        return "first";
                      },
                      CALL_FAILURES));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> cancelledQueued =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor, permits, cancelQueued::get, () -> "cancelled", CALL_FAILURES);
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
                      executor, permits, () -> false, () -> "replacement", CALL_FAILURES));
      awaitQueueSize(executor, 1);

      allowFirst.countDown();
      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertEquals("replacement", replacement.get(1, TimeUnit.SECONDS));
      assertEquals(2, permits.availablePermits());
    } finally {
      allowFirst.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void cancellingRunningCallRetainsAdmissionUntilOperationExits() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
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
                      () -> {
                        blocker.await();
                        return "done";
                      },
                      CALL_FAILURES);
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(blocker.started.await(1, TimeUnit.SECONDS));

      cancel.set(true);
      assertTrue(caller.get(1, TimeUnit.SECONDS) instanceof CancellationException);
      assertEquals(0, permits.availablePermits());
      assertTrue(blocker.interrupted.await(1, TimeUnit.SECONDS));

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

  private static void awaitQueueSize(ThreadPoolExecutor executor, int expected) throws Exception {
    for (int attempt = 0; attempt < 100 && executor.getQueue().size() != expected; attempt++) {
      Thread.sleep(10);
    }
    assertEquals(expected, executor.getQueue().size());
  }
}
