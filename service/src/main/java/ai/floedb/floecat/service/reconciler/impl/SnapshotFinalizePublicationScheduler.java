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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.gc.ReconcileJobGcScheduler;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

/** Publishes durably accepted snapshot finalizers outside the worker-control gRPC path. */
@ApplicationScoped
public class SnapshotFinalizePublicationScheduler {
  private static final Logger LOG = Logger.getLogger(SnapshotFinalizePublicationScheduler.class);
  private static final long RETRY_STATE_TTL_MS = 600_000L;

  @Inject ReconcileJobStore jobs;
  @Inject LeasedSnapshotFinalizeExecutionService publicationService;

  private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
  private final Map<String, RetryState> retries = new ConcurrentHashMap<>();
  private volatile Executor executor = ForkJoinPool.commonPool();
  private volatile String pageToken = "";
  private int pageSize;
  private int maxParallelism;

  @Inject
  void initExecutor(Instance<ManagedExecutor> managedExecutors) {
    if (managedExecutors != null) {
      managedExecutors.stream().findFirst().ifPresent(value -> executor = value);
    }
  }

  @PostConstruct
  void init() {
    var config = ConfigProvider.getConfig();
    pageSize =
        Math.max(
            1,
            config
                .getOptionalValue(
                    "floecat.reconciler.snapshot-finalize-publication.page-size", Integer.class)
                .orElse(100));
    maxParallelism =
        Math.max(
            1,
            config
                .getOptionalValue(
                    "floecat.reconciler.snapshot-finalize-publication.max-parallelism",
                    Integer.class)
                .orElse(4));
  }

  @Scheduled(
      every = "{floecat.reconciler.snapshot-finalize-publication.tick-every:250ms}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = ReconcileJobGcScheduler.DisabledOrStopping.class)
  void tick() {
    if (inFlight.size() >= maxParallelism) {
      return;
    }
    ReconcileJobStore.SnapshotFinalizeCommitPage page =
        jobs.pendingSnapshotFinalizeCommits(pageSize, pageToken);
    long now = System.currentTimeMillis();
    boolean consumedPage = true;
    for (ReconcileJobStore.SnapshotFinalizeCommitIntent intent : page.intents()) {
      if (inFlight.size() >= maxParallelism) {
        consumedPage = false;
        break;
      }
      if (intent == null) {
        continue;
      }
      RetryState retry = retries.get(intent.jobId());
      if (retry != null && retry.nextAttemptAtMs() > now) {
        continue;
      }
      if (!inFlight.add(intent.jobId())) {
        continue;
      }
      try {
        executor.execute(() -> publish(intent));
      } catch (RuntimeException e) {
        inFlight.remove(intent.jobId());
        LOG.warnf(e, "Could not schedule snapshot finalizer publication jobId=%s", intent.jobId());
      }
    }
    if (consumedPage) {
      pageToken = page.nextPageToken();
      if (pageToken.isBlank()) {
        pageToken = "";
      }
    }
    pruneStaleRetries(now);
  }

  /**
   * Drops backoff records that are long overdue and still not in flight. Those belong to intents
   * that no longer appear in any page — cleared or published by another instance — so keeping them
   * would grow the map without bound. Dropping an overdue record for an intent that is merely
   * backlogged only forfeits its remaining (already elapsed) backoff.
   */
  private void pruneStaleRetries(long now) {
    retries
        .entrySet()
        .removeIf(
            entry ->
                !inFlight.contains(entry.getKey())
                    && entry.getValue().nextAttemptAtMs() + RETRY_STATE_TTL_MS < now);
  }

  private void publish(ReconcileJobStore.SnapshotFinalizeCommitIntent intent) {
    try {
      // Both outcomes resolve this jobId locally: true published it, false means another instance
      // cleared or requeued the intent. Either way any recorded backoff state is now stale.
      publicationService.publishAcceptedSnapshotFinalize(intent.jobId());
      retries.remove(intent.jobId());
    } catch (IllegalArgumentException e) {
      retries.remove(intent.jobId());
      jobs.markFailedTerminal(
          intent.jobId(),
          intent.leaseEpoch(),
          System.currentTimeMillis(),
          "Invalid accepted snapshot finalizer result: " + message(e),
          0L,
          0L,
          1L,
          0L,
          0L);
      LOG.warnf(e, "Rejected accepted snapshot finalizer result jobId=%s", intent.jobId());
    } catch (RuntimeException e) {
      // Transient publication failures must never discard a durably accepted result: markFailed
      // clears the finalize intent and requeues the job, throwing away completed capture work. The
      // intent stays accepted and this instance keeps retrying under capped backoff; if the process
      // dies, another instance picks the same intent up from pendingSnapshotFinalizeCommits. Only a
      // proven-invalid payload (IllegalArgumentException above) is promoted to a terminal failure.
      RetryState prior = retries.get(intent.jobId());
      int attempts = prior == null ? 1 : prior.attempts() + 1;
      long delayMs = Math.min(30_000L, 250L << Math.min(16, attempts - 1));
      retries.put(intent.jobId(), new RetryState(attempts, System.currentTimeMillis() + delayMs));
      LOG.warnf(
          e,
          "Snapshot finalizer publication failed; will retry jobId=%s attempt=%d delayMs=%d",
          intent.jobId(),
          attempts,
          delayMs);
    } finally {
      inFlight.remove(intent.jobId());
    }
  }

  private static String message(Throwable error) {
    return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
  }

  private record RetryState(int attempts, long nextAttemptAtMs) {}
}
