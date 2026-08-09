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

import ai.floedb.floecat.service.context.PropagatedContext;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/** Runs one blocking call on a virtual thread while holding process-wide admission. */
final class CancellableCallRunner {

  private static final Logger LOG = Logger.getLogger(CancellableCallRunner.class);
  private static final long CANCELLATION_POLL_MILLIS = 25;
  private static final long SLOW_ADMISSION_WARN_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final ThreadFactory THREADS =
      Thread.ofVirtual()
          .inheritInheritableThreadLocals(false)
          .name("floecat-metadata-io-", 1)
          .factory();

  private CancellableCallRunner() {}

  /** Run a cancellable operation after acquiring one permit. */
  static <T> T call(
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation) {
    Objects.requireNonNull(cancelled, "cancelled");
    return call(permits, cancelled, operation, messages, onSaturation, PropagatedContext::capture);
  }

  /** Cancellable call with injectable context capture for focused tests. */
  static <T> T call(
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation,
      Supplier<PropagatedContext> captureContext) {
    AdmittedTask<T> task =
        admitAndStart(permits, cancelled, operation, messages, onSaturation, captureContext, true);
    return awaitCancellable(task, cancelled, messages);
  }

  /**
   * Run an operation without cooperative cancellation; caller interruption still aborts waiting.
   */
  static <T> T callWithoutCancellation(
      Semaphore permits, Supplier<T> operation, FailureMessages messages, Runnable onSaturation) {
    return callWithoutCancellation(
        permits, operation, messages, onSaturation, PropagatedContext::capture);
  }

  /** Uncancellable call with injectable context capture for focused tests. */
  static <T> T callWithoutCancellation(
      Semaphore permits,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation,
      Supplier<PropagatedContext> captureContext) {
    AdmittedTask<T> task =
        admitAndStart(
            permits, () -> false, operation, messages, onSaturation, captureContext, false);
    return awaitUncancellable(task, messages);
  }

  /**
   * Acquire one permit and transfer it to a started task, releasing it if task construction or
   * thread start fails. {@code pollCancellation} selects whether admission polls the live request
   * signal while queued.
   */
  private static <T> AdmittedTask<T> admitAndStart(
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      FailureMessages messages,
      Runnable onSaturation,
      Supplier<PropagatedContext> captureContext,
      boolean pollCancellation) {
    acquire(permits, cancelled, messages, onSaturation, pollCancellation);
    return startTask(
        permits, operation, captureContext, Thread.currentThread().getContextClassLoader());
  }

  /** Construct and start the sole owner of execution, outcome, interruption, and permit release. */
  private static <T> AdmittedTask<T> startTask(
      Semaphore permits,
      Supplier<T> operation,
      Supplier<PropagatedContext> captureContext,
      ClassLoader applicationClassLoader) {
    AdmittedTask<T> task = null;
    try {
      task =
          new AdmittedTask<>(
              permits,
              Objects.requireNonNull(captureContext, "captureContext").get(),
              Objects.requireNonNull(operation, "operation"),
              applicationClassLoader);
      Thread worker = THREADS.newThread(task);
      worker.start();
      return task;
    } catch (RuntimeException | Error failure) {
      if (task == null) {
        permits.release();
      } else {
        task.releaseBeforeStart();
      }
      throw failure;
    }
  }

  /** Wait until completion or cooperative cancellation wins FutureTask's terminal transition. */
  private static <T> T awaitCancellable(
      AdmittedTask<T> task, BooleanSupplier cancelled, FailureMessages messages) {
    while (true) {
      if (cancelled.getAsBoolean() && task.cancel(true)) {
        throw new CancellationException(messages.cancellation());
      }
      try {
        return task.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // Re-read the live cancellation signal.
      } catch (InterruptedException interrupted) {
        task.cancel(true);
        Thread.currentThread().interrupt();
        throw new CancellationException(messages.interruption());
      } catch (CancellationException cancelledTask) {
        throw new CancellationException(messages.cancellation());
      } catch (ExecutionException failed) {
        throw Futures.propagate(
            failed.getCause(), "unexpected checked exception from metadata I/O");
      }
    }
  }

  /** Await completion without polling; interruption abandons the caller but not the permit. */
  private static <T> T awaitUncancellable(AdmittedTask<T> task, FailureMessages messages) {
    try {
      return task.get();
    } catch (InterruptedException interrupted) {
      task.cancel(true);
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    } catch (CancellationException cancelledTask) {
      throw new CancellationException(messages.cancellation());
    } catch (ExecutionException failed) {
      throw Futures.propagate(failed.getCause(), "unexpected checked exception from metadata I/O");
    }
  }

  /** Acquire admission, reporting saturation once and preserving interruption semantics. */
  private static void acquire(
      Semaphore permits,
      BooleanSupplier cancelled,
      FailureMessages messages,
      Runnable onSaturation,
      boolean pollCancellation) {
    Objects.requireNonNull(permits, "permits");
    Objects.requireNonNull(messages, "messages");
    Objects.requireNonNull(onSaturation, "onSaturation");
    try {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      if (pollCancellation && cancelled.getAsBoolean()) {
        throw new CancellationException(messages.cancellation());
      }
      if (permits.tryAcquire()) {
        return;
      }
      recordSaturation(onSaturation);
      long waitingSince = System.nanoTime();
      boolean warned = false;
      while (true) {
        if (pollCancellation && cancelled.getAsBoolean()) {
          throw new CancellationException(messages.cancellation());
        }
        if (!warned && System.nanoTime() - waitingSince >= SLOW_ADMISSION_WARN_NANOS) {
          warned = true;
          LOG.warnf(
              "%s has waited over %ds for metadata I/O admission (%d free, %d waiting)",
              messages.label(),
              TimeUnit.NANOSECONDS.toSeconds(SLOW_ADMISSION_WARN_NANOS),
              permits.availablePermits(),
              permits.getQueueLength());
        }
        if (pollCancellation) {
          if (permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
            return;
          }
        } else {
          permits.acquire();
          return;
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new CancellationException(messages.interruption());
    }
  }

  /** Report one saturated arrival without letting telemetry failure change admission behavior. */
  private static void recordSaturation(Runnable onSaturation) {
    try {
      onSaturation.run();
    } catch (RuntimeException telemetryFailure) {
      LOG.warnf(telemetryFailure, "metadata I/O saturation telemetry failed");
    }
  }

  /** Caller-facing failure text for one admitted operation family. */
  record FailureMessages(String label, String cancellation, String interruption) {
    FailureMessages {
      Objects.requireNonNull(label, "label");
      Objects.requireNonNull(cancellation, "cancellation");
      Objects.requireNonNull(interruption, "interruption");
    }

    FailureMessages(String cancellation, String interruption) {
      this("metadata-io", cancellation, interruption);
    }
  }

  /** FutureTask remains terminal owner while this wrapper retains admission until run returns. */
  private static final class AdmittedTask<T> extends FutureTask<T> {
    private final Semaphore permits;
    private boolean released;

    /**
     * Capture the request context and application classloader for one operation; this task assumes
     * ownership of the already-acquired permit before its worker starts.
     */
    AdmittedTask(
        Semaphore permits,
        PropagatedContext context,
        Supplier<T> operation,
        ClassLoader applicationClassLoader) {
      super(
          () -> {
            Thread worker = Thread.currentThread();
            worker.setContextClassLoader(applicationClassLoader);
            try {
              return context.supply(operation);
            } finally {
              worker.setContextClassLoader(ClassLoader.getPlatformClassLoader());
            }
          });
      this.permits = permits;
    }

    /** Run the operation and release its permit after the downstream callable actually returns. */
    @Override
    public void run() {
      try {
        super.run();
      } finally {
        release();
      }
    }

    /** Cancel an unstarted task and return the permit transferred to it by its caller. */
    synchronized void releaseBeforeStart() {
      cancel(false);
      release();
    }

    /** Return the task-owned permit exactly once across start-failure and worker completion. */
    private synchronized void release() {
      if (!released) {
        released = true;
        permits.release();
      }
    }
  }
}
