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

package ai.floedb.floecat.service.reconciler.jobs;

import ai.floedb.floecat.service.gc.ReconcileJobGcScheduler;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/** Periodically repairs waiting snapshot plans without relying on dirty-projection markers. */
@ApplicationScoped
public class ReconcileSnapshotOrphanMaintenanceScheduler {
  private static final Logger LOG =
      Logger.getLogger(ReconcileSnapshotOrphanMaintenanceScheduler.class);

  @Inject DurableReconcileJobStore jobs;

  @Scheduled(
      every = "{floecat.reconciler.job-store.snapshot-orphan-maintenance.tick-every:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = ReconcileJobGcScheduler.DisabledOrStopping.class)
  void tick() {
    var config = ConfigProvider.getConfig();
    boolean enabled =
        config
            .getOptionalValue(
                "floecat.reconciler.job-store.snapshot-orphan-maintenance.enabled", Boolean.class)
            .orElse(true);
    if (!enabled) {
      return;
    }
    int maxJobs =
        config
            .getOptionalValue(
                "floecat.reconciler.job-store.snapshot-orphan-maintenance.max-jobs-per-tick",
                Integer.class)
            .orElse(32);
    try {
      jobs.runSnapshotOrphanMaintenanceOnce(Math.max(1, maxJobs));
    } catch (RuntimeException e) {
      LOG.warnf(e, "Reconcile snapshot orphan maintenance tick failed");
    }
  }
}
