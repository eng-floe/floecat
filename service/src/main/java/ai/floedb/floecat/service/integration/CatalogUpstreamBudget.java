/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.service.context.PropagatedContext;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A wall-clock deadline spanning every call one request makes to an upstream catalog.
 *
 * <p>An upstream that accepts the connection and then stalls is not bounded by socket timeouts: an
 * Iceberg REST client alone makes a config round trip, an OAuth exchange and a loadTable, each with
 * its own timeout and no limit on the total. Whatever holds the caller's thread -- a gRPC handler,
 * scan planning -- is held for the sum. Sharing one budget across those calls is what stops any of
 * them being the one that blocks indefinitely.
 *
 * <p>Used by validation, discovery and storage-credential vending. {@code CatalogOverlayReconciler}
 * is deliberately not on it yet: it walks an entire catalog, so it needs a per-call budget like the
 * listing paths rather than one window over the whole walk, and giving it a single deadline would
 * cap a legitimately long reconcile.
 *
 * <p>The window is this budget's alone. It deliberately does not consult the caller's own gRPC
 * deadline: see {@link #remainingNanos()} for why reading one here is unsafe.
 *
 * <p>The operation runs on a virtual thread so the deadline can be enforced on a provider that
 * ignores interruption. An abandoned call may still produce a value that owns resources -- a client
 * holding an HTTP connection pool -- so callers that hand back such a value pass a cleanup
 * consumer, and a result that arrives after the deadline is closed rather than leaked.
 */
public record CatalogUpstreamBudget(long deadlineNanos, LongSupplier nanoTime) {
  public static CatalogUpstreamBudget start(Duration timeout, LongSupplier nanoTime) {
    long timeoutNanos = Math.max(0L, timeout.toNanos());
    return new CatalogUpstreamBudget(nanoTime.getAsLong() + timeoutNanos, nanoTime);
  }

  /** Fails if the budget is already spent, without starting another upstream call. */
  public void check() {
    remainingNanos();
  }

  public <T> T call(Supplier<T> operation) {
    return call(operation, null);
  }

  public <T> T call(Supplier<T> operation, Consumer<T> abandonedResult) {
    long remainingNanos = remainingNanos();
    PropagatedContext context = PropagatedContext.capture();
    AbandonedResult<T> result = new AbandonedResult<>(context, abandonedResult);
    FutureTask<T> task =
        new FutureTask<>(() -> context.supply(() -> result.publish(operation.get())));
    Thread.ofVirtual().name("catalog-integration-upstream").start(task);
    try {
      T value = task.get(remainingNanos, TimeUnit.NANOSECONDS);
      result.claim();
      return value;
    } catch (TimeoutException failure) {
      result.abandon();
      task.cancel(true);
      throw timeout(failure);
    } catch (InterruptedException failure) {
      result.abandon();
      task.cancel(true);
      Thread.currentThread().interrupt();
      CancellationException cancelled =
          new CancellationException("Catalog upstream operation was cancelled");
      cancelled.initCause(failure);
      throw cancelled;
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof RuntimeException runtimeFailure) {
        throw runtimeFailure;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new CatalogAccessException(
          CatalogAccessException.Code.INTERNAL, "Catalog provider operation failed", cause);
    }
  }

  public void run(Runnable operation) {
    call(
        () -> {
          operation.run();
          return null;
        });
  }

  /**
   * Time left in this budget.
   *
   * <p>Deliberately not shortened by the caller's inbound gRPC deadline, and not for want of
   * wanting it: {@code io.grpc.Context} cannot be read safely here. {@code ResolvedCallContexts}
   * documents why -- under Quarkus, attach/detach on a duplicated context is one shared
   * last-writer-wins slot that several threads of the same call race, so a key can read back empty
   * or, on a reused worker, stale via the thread-local fallback. An empty read would be harmless
   * here, but a stale one is not: a deadline left behind by a finished call yields a negative
   * remaining time and would cancel this call's upstream request immediately, for no reason the
   * operator can see. That failure mode is what {@code PrincipalProvider} exists to avoid
   * (eng-floe/floecat#361).
   *
   * <p>Do not reintroduce a caller-deadline ceiling by reading the ambient context. If the ceiling
   * is wanted, the deadline has to arrive the way every other per-call value does -- passed in, or
   * off the duplicated-context channel -- not sampled from a thread-local.
   */
  private long remainingNanos() {
    long remaining = deadlineNanos - nanoTime.getAsLong();
    if (remaining <= 0L) {
      throw timeout(null);
    }
    return remaining;
  }

  private static CatalogAccessException timeout(Throwable cause) {
    return new CatalogAccessException(
        CatalogAccessException.Code.TIMEOUT,
        "Catalog upstream operation exceeded the time limit",
        cause);
  }

  private static final class AbandonedResult<T> {
    private final PropagatedContext context;
    private final Consumer<T> cleanup;
    private boolean abandoned;
    private boolean published;
    private T value;

    private AbandonedResult(PropagatedContext context, Consumer<T> cleanup) {
      this.context = context;
      this.cleanup = cleanup;
    }

    private T publish(T publishedValue) {
      synchronized (this) {
        if (!abandoned) {
          published = true;
          value = publishedValue;
          return publishedValue;
        }
      }
      cleanup(publishedValue);
      return publishedValue;
    }

    private synchronized void claim() {
      published = false;
      value = null;
    }

    private void abandon() {
      T abandonedValue;
      synchronized (this) {
        abandoned = true;
        if (!published) {
          return;
        }
        published = false;
        abandonedValue = value;
        value = null;
      }
      if (cleanup != null) {
        Thread.ofVirtual()
            .name("catalog-integration-cleanup")
            .start(() -> context.supply(() -> cleanup(abandonedValue)));
      }
    }

    private Void cleanup(T abandonedValue) {
      if (cleanup != null) {
        cleanup.accept(abandonedValue);
      }
      return null;
    }
  }
}
