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
package ai.floedb.floecat.service.testsupport;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

/** Shared synchronization helpers for deterministic concurrency tests. */
public final class ConcurrentTestSupport {

  private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(2);

  private ConcurrentTestSupport() {}

  /** Await a latch to completion and restore the worker's interrupt status before returning. */
  public static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Wait up to {@code timeout} for a concurrent condition to become observable. */
  public static void await(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("condition did not become true before " + timeout);
      }
      Thread.sleep(10);
    }
  }

  /** Wait for a concurrent condition using the shared focused-test timeout. */
  public static void await(BooleanSupplier condition) throws InterruptedException {
    await(condition, DEFAULT_AWAIT_TIMEOUT);
  }
}
