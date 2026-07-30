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

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Application-wide admission and platform-worker ownership for blocking metadata I/O.
 *
 * <p>Every concurrent overlay caller shares this runner, so the configured capacity is a process
 * ceiling rather than a per-service multiplier. Admission remains held until the downstream call
 * exits, even when its waiting request has already cancelled.
 */
@ApplicationScoped
public class MetadataIoRunner {
  private static final Logger LOG = Logger.getLogger(MetadataIoRunner.class);
  private static final int DEFAULT_CAPACITY = 64;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;
  private static final long CANCELLATION_POLL_MILLIS = 10;

  private final int capacity;
  private final Semaphore permits;
  private volatile ExecutorService executor = ForkJoinPool.commonPool();
  private ExecutorService ownedExecutor;

  /** Create the production runner with the application-wide default capacity. */
  public MetadataIoRunner() {
    this(DEFAULT_CAPACITY);
  }

  /** Create an isolated runner with a caller-selected capacity, primarily for focused tests. */
  public MetadataIoRunner(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("metadata I/O capacity must be positive");
    }
    this.capacity = capacity;
    this.permits = new Semaphore(capacity, true /* best-effort request fairness */);
  }

  /** Start the owned bounded platform-worker pool; repeated lifecycle calls are harmless. */
  @PostConstruct
  public synchronized void start() {
    if (ownedExecutor != null) {
      return;
    }
    ownedExecutor =
        MetadataIoExecutors.newBoundedDaemonPool(capacity, capacity, "floecat-metadata-io-");
    executor = ownedExecutor;
  }

  /** Run one blocking call with cancellation polling and application-wide admission. */
  public <T> T call(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.call(executor, permits, cancelled, operation, failureMessages);
  }

  /** Run one blocking call off-thread without imposing cancellation or a new deadline. */
  public <T> T callWithoutCancellation(
      Supplier<T> operation, CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.callWithoutCancellation(
        executor, permits, operation, failureMessages);
  }

  /** Run one thread-confined callback on its caller while polling cancellation during admission. */
  public <T> T callOnCallerThread(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    boolean acquired = false;
    try {
      while (!acquired) {
        if (cancelled.getAsBoolean()) {
          throw new CancellationException(failureMessages.cancellation());
        }
        try {
          acquired = permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CancellationException(failureMessages.interruption());
        }
      }
      T result = operation.get();
      if (cancelled.getAsBoolean()) {
        throw new CancellationException(failureMessages.cancellation());
      }
      return result;
    } finally {
      if (acquired) {
        permits.release();
      }
    }
  }

  /** Run one thread-confined callback with blocking fair admission and no invented deadline. */
  public <T> T callOnCallerThreadWithoutCancellation(Supplier<T> operation, String interruption) {
    boolean acquired = false;
    try {
      try {
        permits.acquire();
        acquired = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException(interruption);
      }
      return operation.get();
    } finally {
      if (acquired) {
        permits.release();
      }
    }
  }

  /** Stop owned workers without allowing an interruption-insensitive call to block JVM exit. */
  @PreDestroy
  public synchronized void close() {
    if (ownedExecutor == null) {
      return;
    }
    ExecutorService closing = ownedExecutor;
    ownedExecutor = null;
    executor = ForkJoinPool.commonPool();
    if (!MetadataIoExecutors.shutdownNowAndAwait(
        closing, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      LOG.warn("metadata I/O executor did not terminate before shutdown timeout");
    }
  }
}
