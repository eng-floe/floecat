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

package ai.floedb.floecat.service.statistics;

import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Bounded synchronous stats capture helper for the query-time resolution path.
 *
 * <p>Enqueues a {@code CAPTURE_ONLY} reconcile job and polls the job store until the job reaches a
 * terminal state or the supplied budget is exhausted. This is intentionally kept separate from
 * {@link StatsOrchestrator} so that the polling logic is testable in isolation.
 */
@ApplicationScoped
class StatsSyncCapture {

  private static final Logger LOG = Logger.getLogger(StatsSyncCapture.class);
  private static final long POLL_INTERVAL_MS = 100L;

  private final ReconcileJobStore reconcileJobStore;

  @Inject
  StatsSyncCapture(ReconcileJobStore reconcileJobStore) {
    this.reconcileJobStore = reconcileJobStore;
  }

  /**
   * Enqueues a scoped capture job and waits for it to reach a terminal state within {@code budget}.
   *
   * @return {@link StatsSyncOutcome#CAPTURED} on success, {@link StatsSyncOutcome#TIMEOUT} if the
   *     budget elapsed before completion, {@link StatsSyncOutcome#FAILED} on any error or
   *     cancellation
   */
  StatsSyncOutcome capture(
      String accountId, String connectorId, ReconcileScope scope, Duration budget) {
    try {
      String jobId =
          reconcileJobStore.enqueue(
              accountId, connectorId, false, ReconcilerService.CaptureMode.CAPTURE_ONLY, scope);
      LOG.debugf(
          "stats_sync_capture enqueued account=%s connector=%s job=%s budget=%s",
          accountId, connectorId, jobId, budget);
      return pollUntilTerminal(accountId, jobId, budget);
    } catch (RuntimeException e) {
      LOG.warnf(
          e, "stats_sync_capture enqueue failed account=%s connector=%s", accountId, connectorId);
      return StatsSyncOutcome.FAILED;
    }
  }

  private StatsSyncOutcome pollUntilTerminal(String accountId, String jobId, Duration budget) {
    long deadlineNanos = System.nanoTime() + budget.toNanos();
    while (true) {
      if (System.nanoTime() >= deadlineNanos) {
        LOG.debugf("stats_sync_capture timeout account=%s job=%s", accountId, jobId);
        return StatsSyncOutcome.TIMEOUT;
      }
      Optional<ReconcileJobStore.ReconcileJob> job = reconcileJobStore.get(accountId, jobId);
      if (job.isEmpty()) {
        LOG.warnf("stats_sync_capture job disappeared account=%s job=%s", accountId, jobId);
        return StatsSyncOutcome.FAILED;
      }
      switch (job.get().state) {
        case "JS_SUCCEEDED" -> {
          LOG.debugf("stats_sync_capture succeeded account=%s job=%s", accountId, jobId);
          return StatsSyncOutcome.CAPTURED;
        }
        case "JS_FAILED" -> {
          LOG.debugf(
              "stats_sync_capture job failed account=%s job=%s msg=%s",
              accountId, jobId, job.get().message);
          return StatsSyncOutcome.FAILED;
        }
        case "JS_CANCELLED" -> {
          LOG.debugf("stats_sync_capture job cancelled account=%s job=%s", accountId, jobId);
          return StatsSyncOutcome.FAILED;
        }
        default -> sleepIfBudgetRemains(deadlineNanos);
      }
    }
  }

  private static void sleepIfBudgetRemains(long deadlineNanos) {
    long remainingMs = Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime())).toMillis();
    if (remainingMs <= 0) {
      return;
    }
    try {
      Thread.sleep(Math.min(POLL_INTERVAL_MS, remainingMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
