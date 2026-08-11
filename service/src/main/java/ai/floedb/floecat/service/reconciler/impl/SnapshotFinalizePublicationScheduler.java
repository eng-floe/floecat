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
  }

  private void publish(ReconcileJobStore.SnapshotFinalizeCommitIntent intent) {
    try {
      if (publicationService.publishAcceptedSnapshotFinalize(intent.jobId())) {
        retries.remove(intent.jobId());
      }
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
      RetryState prior = retries.get(intent.jobId());
      int attempts = prior == null ? 1 : prior.attempts() + 1;
      if (attempts >= 8) {
        retries.remove(intent.jobId());
        jobs.markFailed(
            intent.jobId(),
            intent.leaseEpoch(),
            System.currentTimeMillis(),
            "Snapshot finalizer publication failed: " + message(e),
            0L,
            0L,
            1L,
            0L,
            0L);
        LOG.warnf(
            e,
            "Snapshot finalizer publication exhausted local retries; requeued jobId=%s",
            intent.jobId());
        return;
      }
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
