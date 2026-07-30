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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Simulates a downstream call that records but does not obey interruption until released. */
public final class UninterruptibleBlocker {

  /** Signals that the downstream call entered the blocker. */
  public final CountDownLatch started = new CountDownLatch(1);

  /** Signals that the blocked call observed at least one interrupt. */
  public final CountDownLatch interrupted = new CountDownLatch(1);

  /** Allows the simulated downstream call to return. */
  public final CountDownLatch release = new CountDownLatch(1);

  /** Captures the thread that executes the simulated downstream call. */
  public final AtomicReference<Thread> executionThread = new AtomicReference<>();

  /** Block until released, recording and deliberately ignoring interrupts. */
  public void await() {
    executionThread.set(Thread.currentThread());
    started.countDown();
    while (true) {
      try {
        release.await();
        return;
      } catch (InterruptedException ignored) {
        interrupted.countDown();
      }
    }
  }
}
