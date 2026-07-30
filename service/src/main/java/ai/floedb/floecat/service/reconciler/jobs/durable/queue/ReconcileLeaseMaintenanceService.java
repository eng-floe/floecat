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
public class ReconcileLeaseMaintenanceService {

  private static final long SLOW_MAINTENANCE_LOG_THRESHOLD_MS = 30_000L;
  private static final Logger LOG = Logger.getLogger(ReconcileLeaseMaintenanceService.class);

  @FunctionalInterface
  public interface ReclaimCanonicalJob {
    void accept(ReconcileLeaseStore.LeaseExpiryEntry leaseExpiryEntry, long nowMs);
  }

  private ReconcileLeaseStore leaseStore;
  private ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer;
  private int readyScanLimit;
  private long reclaimIntervalMs;

  private volatile long lastReclaimAtMs;
  private volatile String leaseExpiryScanToken = "";
  private final ReentrantLock reclaimLock = new ReentrantLock();

  public void bind(
      ReconcileLeaseStore leaseStore,
      ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer,
      int readyScanLimit,
      long reclaimIntervalMs) {
    this.leaseStore = leaseStore;
    this.reclaimExpiredLeaseFromCanonicalPointer = reclaimExpiredLeaseFromCanonicalPointer;
    this.readyScanLimit = readyScanLimit;
    this.reclaimIntervalMs = reclaimIntervalMs;
  }

  public void runLeaseMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    LeaseReclaimStats reclaimStats = reclaimExpiredLeasesIfDue(startedAtMs, deadlineMs);
    logMaintenanceSummary(startedAtMs, reclaimStats);
  }

  private LeaseReclaimStats reclaimExpiredLeasesIfDue(long nowMs, long deadlineMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return LeaseReclaimStats.skippedStats();
    }
    if (!reclaimLock.tryLock()) {
      return LeaseReclaimStats.skippedStats();
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return LeaseReclaimStats.skippedStats();
      }
      LeaseReclaimStats stats = scanLeaseExpiryPointersForReclaim(nowMs, deadlineMs);
      if (stats.completed() && System.currentTimeMillis() <= deadlineMs) {
        lastReclaimAtMs = System.currentTimeMillis();
      }
      return stats;
    } finally {
      reclaimLock.unlock();
    }
  }

  private LeaseReclaimStats scanLeaseExpiryPointersForReclaim(long nowMs, long deadlineMs) {
    String token = blankToEmpty(leaseExpiryScanToken);
    int pages = 0;
    int scanned = 0;
    int reclaimed = 0;
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        return new LeaseReclaimStats(false, false, pages, scanned, reclaimed);
      }
      ReconcileLeaseStore.LeaseExpiryScanPage page =
          leaseStore.scanExpiredLeasePointersPage(nowMs, readyScanLimit, token);
      List<ReconcileLeaseStore.LeaseExpiryEntry> leaseExpiries = page.entries();
      if (leaseExpiries.isEmpty()) {
        leaseExpiryScanToken = "";
        return new LeaseReclaimStats(false, true, pages, scanned, reclaimed);
      }
      for (ReconcileLeaseStore.LeaseExpiryEntry leaseExpiry : leaseExpiries) {
        if (System.currentTimeMillis() > deadlineMs) {
          return new LeaseReclaimStats(false, false, pages, scanned, reclaimed);
        }
        scanned++;
        reclaimExpiredLeaseFromCanonicalPointer.accept(leaseExpiry, nowMs);
        reclaimed++;
      }

      String nextToken = blankToEmpty(page.nextPageToken());
      if (nextToken.isBlank()) {
        leaseExpiryScanToken = "";
        return new LeaseReclaimStats(false, true, pages + 1, scanned, reclaimed);
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile lease-expiry reclaim pagination token did not advance; aborting scan to"
                + " avoid livelock");
        leaseExpiryScanToken = "";
        return new LeaseReclaimStats(false, true, pages + 1, scanned, reclaimed);
      }
      leaseExpiryScanToken = nextToken;
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease-expiry reclaim pagination hit safety page cap; aborting scan");
        leaseExpiryScanToken = "";
        return new LeaseReclaimStats(false, true, pages, scanned, reclaimed);
      }
    }
  }

  private void logMaintenanceSummary(long startedAtMs, LeaseReclaimStats reclaimStats) {
    long elapsedMs = System.currentTimeMillis() - startedAtMs;
    boolean noteworthy =
        !reclaimStats.completed()
            || reclaimStats.reclaimed() > 0
            || elapsedMs >= SLOW_MAINTENANCE_LOG_THRESHOLD_MS;
    if (!noteworthy) {
      LOG.debugf(
          "runLeaseMaintenanceOnce total_ms=%d"
              + " lease_reclaim_skipped=%s lease_reclaim_completed=%s lease_reclaim_pages=%d"
              + " lease_reclaim_scanned=%d lease_reclaimed=%d",
          Long.valueOf(elapsedMs),
          Boolean.valueOf(reclaimStats.skipped()),
          Boolean.valueOf(reclaimStats.completed()),
          Integer.valueOf(reclaimStats.pages()),
          Integer.valueOf(reclaimStats.scanned()),
          Integer.valueOf(reclaimStats.reclaimed()));
      return;
    }
    LOG.infof(
        "runLeaseMaintenanceOnce total_ms=%d"
            + " lease_reclaim_skipped=%s lease_reclaim_completed=%s lease_reclaim_pages=%d"
            + " lease_reclaim_scanned=%d lease_reclaimed=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(reclaimStats.skipped()),
        Boolean.valueOf(reclaimStats.completed()),
        Integer.valueOf(reclaimStats.pages()),
        Integer.valueOf(reclaimStats.scanned()),
        Integer.valueOf(reclaimStats.reclaimed()));
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private record LeaseReclaimStats(
      boolean skipped, boolean completed, int pages, int scanned, int reclaimed) {
    static LeaseReclaimStats skippedStats() {
      return new LeaseReclaimStats(true, true, 0, 0, 0);
    }

    boolean active() {
      return !completed || scanned > 0 || reclaimed > 0;
    }
  }
}
