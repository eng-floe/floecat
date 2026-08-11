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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.context.api.ManagedExecutorConfig;
import io.smallrye.context.api.NamedInstance;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.context.ThreadContext;
import org.jboss.logging.Logger;

/**
 * Releases cancelled streams' transient pin roots (GC protections for referenced blobs) away from
 * transport threads. Cancellation callbacks coalesce work by query while at most four
 * container-owned drainers perform cleanup and at most 256 releases await a drainer. At that
 * retained-work ceiling, synchronous cleanup is the bounded fail-safe that preserves root ownership
 * without allocating more teardown state. Request context is cleared because cleanup needs only the
 * explicit query id and pin set.
 */
@ApplicationScoped
final class CancelledQueryPinCleanup {
  private static final Logger LOG = Logger.getLogger(CancelledQueryPinCleanup.class);
  private static final int MAX_CONCURRENT_RELEASES = 4;
  static final int MAX_RETAINED_RELEASES = 256;

  private final QueryContextStore queryStore;
  private final Executor executor;
  private final ConcurrentHashMap<String, PendingQuery> pendingByQuery = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> readyQueries = new ConcurrentLinkedQueue<>();
  private final AtomicInteger retainedReleases = new AtomicInteger();
  private final Object lifecycleHandoff = new Object();
  private int activeDrainers;
  private int releaseCallsInFlight;
  private boolean closing;
  private boolean closed;

  @Inject
  CancelledQueryPinCleanup(
      QueryContextStore queryStore,
      @NamedInstance("bundle-cancelled-query-pin-cleanup")
          @ManagedExecutorConfig(
              maxAsync = MAX_CONCURRENT_RELEASES,
              maxQueued = MAX_CONCURRENT_RELEASES,
              propagated = {},
              cleared = ThreadContext.ALL_REMAINING)
          ManagedExecutor executor) {
    this.queryStore = queryStore;
    this.executor = executor;
  }

  CancelledQueryPinCleanup(QueryContextStore queryStore, Executor executor) {
    this.queryStore = queryStore;
    this.executor = executor;
  }

  /**
   * Schedule release without making the transport termination callback wait for executor capacity.
   * One ready-queue entry represents all pending releases for a query, and a fixed number of
   * drainers consume those entries. The managed executor therefore never receives more than four
   * tasks from this component, regardless of cancellation volume. Once the retained-work ceiling is
   * full, the caller performs that release synchronously instead of dropping roots or growing the
   * buffer.
   */
  void release(String queryId, RelationPinSet pins) {
    if (pins.getPinsCount() == 0) {
      return;
    }
    synchronized (lifecycleHandoff) {
      if (closed) {
        throw new IllegalStateException("Cancelled-query pin cleanup is closed");
      }
      releaseCallsInFlight++;
    }
    try {
      List<String> roots = List.copyOf(QueryPins.gcRootUris(pins));
      boolean releaseInline;
      synchronized (lifecycleHandoff) {
        releaseInline = closing || retainedReleases.get() >= MAX_RETAINED_RELEASES;
        if (!releaseInline) {
          retainedReleases.incrementAndGet();
          AtomicBoolean newQuery = new AtomicBoolean();
          pendingByQuery.compute(
              queryId,
              (ignored, pending) -> {
                PendingQuery releases = pending;
                if (releases == null) {
                  releases = new PendingQuery();
                  newQuery.set(true);
                }
                releases.add(roots);
                return releases;
              });
          if (newQuery.get()) {
            readyQueries.add(queryId);
          }
        }
      }
      if (releaseInline) {
        releaseNow(queryId, roots);
      } else {
        startDrainer();
      }
    } finally {
      releaseCallFinished();
    }
  }

  /** Claim one of the bounded managed drainers before handing its task to the executor. */
  private void startDrainer() {
    synchronized (lifecycleHandoff) {
      if (closing || activeDrainers >= MAX_CONCURRENT_RELEASES || readyQueries.isEmpty()) {
        return;
      }
      activeDrainers++;
    }
    try {
      executor.execute(this::drain);
    } catch (RejectedExecutionException unavailable) {
      drainerFinished();
      LOG.debugf(unavailable, "Cancellation teardown executor temporarily unavailable");
    }
  }

  /**
   * Retry retained cleanup after transient executor saturation without blocking request threads.
   */
  @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void retryPending() {
    startDrainer();
  }

  /**
   * Release retained roots once request handling has stopped, then wait for drainers that already
   * claimed ownership. Interrupts do not shorten teardown; their status is restored after cleanup.
   */
  @PreDestroy
  void drainOnShutdown() {
    synchronized (lifecycleHandoff) {
      if (closed) {
        return;
      }
      closing = true;
    }
    drainInline();
    awaitOwnedReleases();
  }

  /** Drain retained batches while holding one managed-drainer lifecycle claim. */
  private void drain() {
    try {
      drainInline();
    } finally {
      drainerFinished();
      startDrainer();
    }
  }

  /** Relinquish a managed-drainer claim and wake bean teardown if it owns the last one. */
  private void drainerFinished() {
    synchronized (lifecycleHandoff) {
      activeDrainers--;
      lifecycleHandoff.notifyAll();
    }
  }

  /** Relinquish a request-side release claim after queuing or inline cleanup completes. */
  private void releaseCallFinished() {
    synchronized (lifecycleHandoff) {
      releaseCallsInFlight--;
      lifecycleHandoff.notifyAll();
    }
  }

  /** Wait for every request-side and managed-drainer owner, then close the lifecycle. */
  private void awaitOwnedReleases() {
    boolean interrupted = false;
    synchronized (lifecycleHandoff) {
      while (activeDrainers != 0 || releaseCallsInFlight != 0) {
        try {
          lifecycleHandoff.wait();
        } catch (InterruptedException shutdownInterrupted) {
          interrupted = true;
        }
      }
      closed = true;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Claim ready query batches and release every retained registration exactly once. */
  private void drainInline() {
    String queryId;
    while ((queryId = readyQueries.poll()) != null) {
      PendingQuery releases = pendingByQuery.remove(queryId);
      if (releases == null) {
        continue;
      }
      for (Map.Entry<List<String>, AtomicInteger> release : releases.entries()) {
        for (int remaining = release.getValue().get(); remaining > 0; remaining--) {
          try {
            releaseNow(queryId, release.getKey());
          } finally {
            retainedReleases.decrementAndGet();
          }
        }
      }
    }
  }

  /** Apply one release while containing store failures to this best-effort cleanup boundary. */
  private void releaseNow(String queryId, List<String> roots) {
    try {
      queryStore.releaseResolvingPinBlobs(queryId, roots);
    } catch (RuntimeException releaseFailure) {
      LOG.warnf(
          releaseFailure, "Failed to release cancelled stream pin roots query_id=%s", queryId);
    }
  }

  /** Coalesces identical release sets while preserving registration multiplicity. */
  private static final class PendingQuery {
    private final ConcurrentHashMap<List<String>, AtomicInteger> releases =
        new ConcurrentHashMap<>();

    void add(List<String> roots) {
      releases.computeIfAbsent(roots, ignored -> new AtomicInteger()).incrementAndGet();
    }

    Iterable<Map.Entry<List<String>, AtomicInteger>> entries() {
      return releases.entrySet();
    }
  }
}
