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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
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
  public interface RefreshDirtyParentProjection {
    void accept(String accountId, String parentJobId);
  }

  private ReconcileLeaseStore leaseStore;
  private PointerStore pointerStore;
  private ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer;
  private RefreshDirtyParentProjection refreshDirtyParentProjection;
  private int readyScanLimit;
  private long reclaimIntervalMs;

  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

  public void bind(
      ReconcileLeaseStore leaseStore,
      PointerStore pointerStore,
      ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer,
      RefreshDirtyParentProjection refreshDirtyParentProjection,
      int readyScanLimit,
      long reclaimIntervalMs) {
    this.leaseStore = leaseStore;
    this.pointerStore = pointerStore;
    this.reclaimExpiredLeaseFromCanonicalPointer = reclaimExpiredLeaseFromCanonicalPointer;
    this.refreshDirtyParentProjection = refreshDirtyParentProjection;
    this.readyScanLimit = readyScanLimit;
    this.reclaimIntervalMs = reclaimIntervalMs;
  }

  public void runMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    reclaimExpiredLeasesIfDue(startedAtMs, deadlineMs);
    refreshDirtyParents(deadlineMs);
    LOG.debugf(
        "runMaintenanceOnce total_ms=%d reclaim_ms=%d",
        System.currentTimeMillis() - startedAtMs, System.currentTimeMillis() - startedAtMs);
  }

  public void runLeaseMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    reclaimExpiredLeasesIfDue(startedAtMs, deadlineMs);
    LOG.debugf("runLeaseMaintenanceOnce total_ms=%d", System.currentTimeMillis() - startedAtMs);
  }

  public void runProjectionMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    refreshDirtyParents(deadlineMs);
    LOG.debugf(
        "runProjectionMaintenanceOnce total_ms=%d", System.currentTimeMillis() - startedAtMs);
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

  private void refreshDirtyParents(long deadlineMs) {
    if (pointerStore == null || refreshDirtyParentProjection == null) {
      return;
    }
    String token = "";
    int pages = 0;
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        return;
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileDirtyParentPointerPrefix(), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        return;
      }
      for (Pointer pointer : pointers) {
        if (System.currentTimeMillis() > deadlineMs) {
          return;
        }
        if (pointer == null || pointer.getKey().isBlank()) {
          continue;
        }
        DirtyParentMarker marker = parseDirtyParentMarker(pointer);
        if (marker == null) {
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
          continue;
        }
        try {
          refreshDirtyParentProjection.accept(marker.accountId(), marker.parentJobId());
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
        } catch (RuntimeException e) {
          LOG.warnf(
              e,
              "Failed to refresh reconcile parent projection accountId=%s parentJobId=%s",
              marker.accountId(),
              marker.parentJobId());
        }
      }
      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return;
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile dirty-parent refresh pagination token did not advance; aborting scan to"
                + " avoid livelock");
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile dirty-parent refresh pagination hit safety page cap; aborting scan");
        return;
      }
    }
  }

  private DirtyParentMarker parseDirtyParentMarker(Pointer pointer) {
    String payload = pointer == null ? "" : pointer.getBlobUri();
    if (payload == null || payload.isBlank()) {
      return null;
    }
    int delimiter = payload.indexOf('\n');
    if (delimiter <= 0 || delimiter >= payload.length() - 1) {
      return null;
    }
    String accountId = payload.substring(0, delimiter).trim();
    String parentJobId = payload.substring(delimiter + 1).trim();
    if (accountId.isBlank() || parentJobId.isBlank()) {
      return null;
    }
    return new DirtyParentMarker(accountId, parentJobId);
  }

  private record DirtyParentMarker(String accountId, String parentJobId) {}
}
