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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CancellableCallRunnerTest {

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
                      "cancelled",
                      "interrupted"));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Throwable> queued =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  CancellableCallRunner.call(
                      executor, permits, () -> false, () -> "queued", "cancelled", "interrupted");
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
}
