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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Runs one blocking call behind process-wide admission while preserving prompt caller cancellation.
 *
 * <p>Admission transfers to the dispatched task and remains held until its callable returns. A
 * caller that cancels interrupts the worker when possible and returns immediately; an
 * interruption-insensitive downstream call still owns its admission slot, bounding retained work.
 */
public final class CancellableCallRunner {

  private static final long CANCELLATION_POLL_MILLIS = 10;

  private CancellableCallRunner() {}

  /**
   * Run {@code operation} under {@code permits}, polling {@code cancelled} while awaiting both
   * admission and completion. The operation receives the caller's propagated request context.
   */
  public static <T> T call(
      Executor executor,
      Semaphore permits,
      BooleanSupplier cancelled,
      Supplier<T> operation,
      String cancellationMessage,
      String interruptionMessage) {
    acquire(permits, cancelled, cancellationMessage, interruptionMessage);
    PropagatedContext context = PropagatedContext.capture();
    CompletableFuture<T> result = new CompletableFuture<>();
    AtomicReference<Thread> worker = new AtomicReference<>();
    FutureTask<Void> submitted =
        new FutureTask<>(
            () -> {
              worker.set(Thread.currentThread());
              try {
                throwIfCancelled(cancelled, cancellationMessage);
                result.complete(context.supply(operation));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              } finally {
                worker.set(null);
                permits.release();
              }
            },
            null);
    try {
      // An explicit FutureTask prevents ForkJoinPool from help-running a submitted lambda inline
      // on the caller, which would bypass this method's cancellation polling loop.
      executor.execute(submitted);
    } catch (RuntimeException | Error submissionFailure) {
      permits.release();
      throw submissionFailure;
    }
    try {
      while (true) {
        if (cancelled.getAsBoolean()) {
          interrupt(worker);
          throw new CancellationException(cancellationMessage);
        }
        try {
          return result.get(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
          // A bounded wait lets the caller interrupt a stalled operation on cancellation.
        } catch (InterruptedException e) {
          interrupt(worker);
          Thread.currentThread().interrupt();
          throw new CancellationException(interruptionMessage);
        } catch (ExecutionException e) {
          rethrow(e.getCause());
        }
      }
    } catch (CancellationException e) {
      interrupt(worker);
      throw e;
    }
  }

  private static void acquire(
      Semaphore permits,
      BooleanSupplier cancelled,
      String cancellationMessage,
      String interruptionMessage) {
    try {
      while (true) {
        throwIfCancelled(cancelled, cancellationMessage);
        if (permits.tryAcquire(CANCELLATION_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException(interruptionMessage);
    }
  }

  private static void throwIfCancelled(BooleanSupplier cancelled, String message) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException(message);
    }
  }

  private static void interrupt(AtomicReference<Thread> worker) {
    Thread active = worker.get();
    if (active != null) {
      active.interrupt();
    }
  }

  private static void rethrow(Throwable failure) {
    if (failure instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new IllegalStateException("unexpected checked exception from cancellable call", failure);
  }
}
