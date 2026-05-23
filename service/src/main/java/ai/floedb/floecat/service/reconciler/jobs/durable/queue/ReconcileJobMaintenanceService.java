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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileJobMaintenanceService.class);

  @FunctionalInterface
  public interface ReclaimCanonicalJob {
    void accept(ReconcileLeaseStore.LeaseExpiryEntry leaseExpiryEntry, long nowMs);
  }

  @FunctionalInterface
  public interface RepairActiveStateJobs {
    void run(long nowMs, long deadlineMs, int pageSize);
  }

  private ReconcileLeaseStore leaseStore;
  private ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer;
  private RepairActiveStateJobs repairActiveStateJobs;
  private int readyScanLimit;
  private long reclaimIntervalMs;

  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

  public void bind(
      ReconcileLeaseStore leaseStore,
      ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer,
      RepairActiveStateJobs repairActiveStateJobs,
      int readyScanLimit,
      long reclaimIntervalMs) {
    this.leaseStore = leaseStore;
    this.reclaimExpiredLeaseFromCanonicalPointer = reclaimExpiredLeaseFromCanonicalPointer;
    this.repairActiveStateJobs = repairActiveStateJobs;
    this.readyScanLimit = readyScanLimit;
    this.reclaimIntervalMs = reclaimIntervalMs;
  }

  public void runMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(
        startedAtMs, maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis));
    LOG.debugf(
        "runMaintenanceOnce total_ms=%d reclaim_ms=%d",
        System.currentTimeMillis() - startedAtMs, System.currentTimeMillis() - startedAtMs);
  }

  private void reclaimExpiredLeasesIfDue(long nowMs, long deadlineMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    if (!reclaimLock.tryLock()) {
      return;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return;
      }
      boolean completed = scanLeaseExpiryPointersForReclaim(nowMs, deadlineMs);
      if (System.currentTimeMillis() <= deadlineMs && repairActiveStateJobs != null) {
        repairActiveStateJobs.run(nowMs, deadlineMs, readyScanLimit);
      }
      if (completed && System.currentTimeMillis() <= deadlineMs) {
        lastReclaimAtMs = System.currentTimeMillis();
      }
    } finally {
      reclaimLock.unlock();
    }
  }

  private boolean scanLeaseExpiryPointersForReclaim(long nowMs, long deadlineMs) {
    String token = "";
    int pages = 0;
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        return false;
      }
      ReconcileLeaseStore.LeaseExpiryScanPage page =
          leaseStore.scanExpiredLeasePointersPage(nowMs, readyScanLimit, token);
      List<ReconcileLeaseStore.LeaseExpiryEntry> leaseExpiries = page.entries();
      if (leaseExpiries.isEmpty()) {
        return true;
      }
      for (ReconcileLeaseStore.LeaseExpiryEntry leaseExpiry : leaseExpiries) {
        if (System.currentTimeMillis() > deadlineMs) {
          return false;
        }
        reclaimExpiredLeaseFromCanonicalPointer.accept(leaseExpiry, nowMs);
      }

      String nextToken = page.nextPageToken();
      if (nextToken.isBlank()) {
        return true;
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile lease-expiry reclaim pagination token did not advance; aborting scan to"
                + " avoid livelock");
        return true;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease-expiry reclaim pagination hit safety page cap; aborting scan");
        return true;
      }
    }
  }
}
