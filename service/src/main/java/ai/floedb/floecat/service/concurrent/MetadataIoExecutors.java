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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * Creates and tears down bounded platform-worker pools for blocking metadata I/O.
 *
 * <p>The pools use daemon threads so an interruption-insensitive client cannot hold JVM exit, a
 * bounded queue so cancelled request closures cannot accumulate without limit, and abort-on-full
 * submission so the owning admission semaphore remains the single backpressure policy.
 */
final class MetadataIoExecutors {

  private static final Logger LOG = Logger.getLogger(MetadataIoExecutors.class);

  /** How long an idle metadata-I/O worker survives before the pool reclaims its thread. */
  private static final long IDLE_WORKER_TIMEOUT_SECONDS = 60L;

  private MetadataIoExecutors() {}

  /**
   * Create a fixed-size daemon pool with a bounded handoff queue and diagnostic thread names.
   * Invalid sizes fail before a request can submit work.
   */
  static ThreadPoolExecutor newBoundedDaemonPool(
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
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(
            workers,
            workers,
            IDLE_WORKER_TIMEOUT_SECONDS,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            threads,
            new ThreadPoolExecutor.AbortPolicy());
    // Core threads are exempt from the keep-alive unless this is set, so without it a single burst
    // to the ceiling pins `workers` platform threads and their stacks for the life of the process.
    // Requires a non-zero keep-alive; allowCoreThreadTimeOut throws otherwise.
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Interrupt live work, hand the tasks discarded from the queue to {@code onDiscarded}, and await
   * no longer than the supplied bound. Returns whether the pool terminated. Interruption restores
   * the caller's interrupt status and returns {@code true}.
   *
   * <p>The discarded tasks are passed out rather than released here: what a queued task owns is the
   * submitter's concern, and this class stays a pool factory that knows nothing about admission.
   */
  static boolean shutdownNowAndAwait(
      ExecutorService executor, Consumer<List<Runnable>> onDiscarded, long timeout, TimeUnit unit) {
    onDiscarded.accept(executor.shutdownNow());
    try {
      return executor.awaitTermination(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Distinct from a timeout: the awaiting thread was interrupted, which happens when close()
      // runs on one of the pool's own workers and shutdownNow interrupts the closer. Reporting it
      // as a timeout would tell an operator the pool failed to drain when it never got to wait.
      LOG.debug("interrupted while awaiting metadata I/O executor termination");
      return true;
    }
  }
}
