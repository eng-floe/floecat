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

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates and tears down bounded platform-worker pools for blocking metadata I/O.
 *
 * <p>The pools use daemon threads so an interruption-insensitive client cannot hold JVM exit, a
 * bounded queue so cancelled request closures cannot accumulate without limit, and abort-on-full
 * submission so the owning admission semaphore remains the single backpressure policy.
 */
public final class MetadataIoExecutors {
  private MetadataIoExecutors() {}

  /**
   * Create a fixed-size daemon pool with a bounded handoff queue and diagnostic thread names.
   * Invalid sizes fail before a request can submit work.
   */
  public static ExecutorService newBoundedDaemonPool(
      int workers, int queueCapacity, String threadPrefix) {
    if (workers < 1 || queueCapacity < 1) {
      throw new IllegalArgumentException("metadata worker and queue sizes must be positive");
    }
    String prefix = Objects.requireNonNull(threadPrefix, "threadPrefix");
    AtomicInteger sequence = new AtomicInteger();
    ThreadFactory threads =
        runnable -> {
          Thread thread = new Thread(runnable, prefix + sequence.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        };
    return new ThreadPoolExecutor(
        workers,
        workers,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity),
        threads,
        new ThreadPoolExecutor.AbortPolicy());
  }

  /**
   * Interrupt live work, release admission held by tasks discarded from the queue, and await no
   * longer than the supplied bound. Returns whether the pool terminated; interruption restores the
   * caller's interrupt status and returns {@code false}.
   */
  public static boolean shutdownNowAndAwait(ExecutorService executor, long timeout, TimeUnit unit) {
    CancellableCallRunner.cancelDiscardedTasks(executor.shutdownNow());
    try {
      return executor.awaitTermination(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
