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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Application-wide admission and platform-worker ownership for blocking metadata I/O.
 *
 * <p>Every production/default overlay caller shares one runtime, so the configured capacity is a
 * process ceiling rather than a per-service multiplier. Admission remains held until the downstream
 * call exits, even when its waiting request has already cancelled. Explicit-capacity instances are
 * isolated for focused tests.
 */
@ApplicationScoped
public class MetadataIoRunner {
  private static final Logger LOG = Logger.getLogger(MetadataIoRunner.class);
  private static final int DEFAULT_CAPACITY = 64;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;
  private static final long CANCELLATION_POLL_MILLIS = 10;

  private final RuntimeState runtime;

  /**
   * Create a facade over the process-wide production runtime. Direct and CDI construction share the
   * same admission semaphore and bounded daemon worker pool.
   */
  public MetadataIoRunner() {
    this(SharedRuntime.RUNTIME);
  }

  /** Return the process-wide runner for compatibility constructors outside CDI. */
  public static MetadataIoRunner shared() {
    return SharedRunner.INSTANCE;
  }

  /** Create an isolated runner with a caller-selected capacity, primarily for focused tests. */
  public MetadataIoRunner(int capacity) {
    this(new RuntimeState(capacity));
  }

  private MetadataIoRunner(RuntimeState runtime) {
    this.runtime = java.util.Objects.requireNonNull(runtime, "runtime");
    runtime.start();
  }

  /** Start the bounded platform-worker pool; repeated lifecycle calls are harmless. */
  @PostConstruct
  public void start() {
    runtime.start();
  }

  /** True when two facades share the same process or test runtime. */
  boolean sharesRuntimeWith(MetadataIoRunner other) {
    return other != null && runtime == other.runtime;
  }

  /** Run one blocking call with cancellation polling and application-wide admission. */
  public <T> T call(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.call(
        runtime.executor(), runtime.permits, cancelled, operation, failureMessages);
  }

  /** Run one blocking call off-thread without imposing cancellation or a new deadline. */
  public <T> T callWithoutCancellation(
      Supplier<T> operation, CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.callWithoutCancellation(
        runtime.executor(), runtime.permits, operation, failureMessages);
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
          acquired = runtime.permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
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
        runtime.permits.release();
      }
    }
  }

  /** Run one thread-confined callback with blocking fair admission and no invented deadline. */
  public <T> T callOnCallerThreadWithoutCancellation(Supplier<T> operation, String interruption) {
    boolean acquired = false;
    try {
      try {
        runtime.permits.acquire();
        acquired = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException(interruption);
      }
      return operation.get();
    } finally {
      if (acquired) {
        runtime.permits.release();
      }
    }
  }

  /**
   * Stop this runtime's workers without allowing an interruption-insensitive call to block exit.
   */
  @PreDestroy
  public void close() {
    if (!runtime.close()) {
      LOG.warn("metadata I/O executor did not terminate before shutdown timeout");
    }
  }

  /** Holder indirection avoids eagerly starting the process runtime when this class is unused. */
  private static final class SharedRuntime {
    private static final RuntimeState RUNTIME = new RuntimeState(DEFAULT_CAPACITY);
  }

  /** Stable facade used by direct-construction compatibility paths. */
  private static final class SharedRunner {
    private static final MetadataIoRunner INSTANCE = new MetadataIoRunner(SharedRuntime.RUNTIME);
  }

  /** Shared lifecycle and admission state behind one or more runner facades. */
  private static final class RuntimeState {
    private final int capacity;
    private final Semaphore permits;
    private volatile ExecutorService executor;

    private RuntimeState(int capacity) {
      if (capacity < 1) {
        throw new IllegalArgumentException("metadata I/O capacity must be positive");
      }
      this.capacity = capacity;
      this.permits = new Semaphore(capacity, true /* best-effort request fairness */);
    }

    private synchronized void start() {
      if (executor == null || executor.isShutdown()) {
        executor =
            MetadataIoExecutors.newBoundedDaemonPool(capacity, capacity, "floecat-metadata-io-");
      }
    }

    private ExecutorService executor() {
      ExecutorService current = executor;
      if (current == null || current.isShutdown()) {
        start();
        current = executor;
      }
      return current;
    }

    private synchronized boolean close() {
      if (executor == null) {
        return true;
      }
      ExecutorService closing = executor;
      executor = null;
      return MetadataIoExecutors.shutdownNowAndAwait(
          closing, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }
}
