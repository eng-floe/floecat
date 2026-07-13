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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileProjectionMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileProjectionMaintenanceService.class);

  @FunctionalInterface
  public interface RefreshDirtyParentProjection {
    void accept(String accountId, String parentJobId);
  }

  @FunctionalInterface
  public interface ObsoleteDirtyParentProjection {
    boolean test(String accountId, String parentJobId);
  }

  private PointerStore pointerStore;
  private RefreshDirtyParentProjection refreshDirtyParentProjection;
  private ObsoleteDirtyParentProjection obsoleteDirtyParentProjection;
  private int readyScanLimit;

  private volatile String dirtyParentScanToken = "";

  public void bind(
      PointerStore pointerStore,
      RefreshDirtyParentProjection refreshDirtyParentProjection,
      ObsoleteDirtyParentProjection obsoleteDirtyParentProjection,
      int readyScanLimit) {
    this.pointerStore = pointerStore;
    this.refreshDirtyParentProjection = refreshDirtyParentProjection;
    this.obsoleteDirtyParentProjection = obsoleteDirtyParentProjection;
    this.readyScanLimit = readyScanLimit;
  }

  public void runProjectionMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    DirtyParentStats dirtyParentStats = refreshDirtyParents(deadlineMs);
    logMaintenanceSummary(startedAtMs, dirtyParentStats);
  }

  private DirtyParentStats refreshDirtyParents(long deadlineMs) {
    if (pointerStore == null || refreshDirtyParentProjection == null) {
      return DirtyParentStats.empty();
    }
    String token = blankToEmpty(dirtyParentScanToken);
    int pages = 0;
    int scanned = 0;
    int refreshed = 0;
    int invalidDeleted = 0;
    int obsoleteDeleted = 0;
    int deleted = 0;
    int failures = 0;
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        return new DirtyParentStats(
            false, pages, scanned, refreshed, invalidDeleted, obsoleteDeleted, deleted, failures);
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileDirtyParentPointerPrefix(), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true, pages, scanned, refreshed, invalidDeleted, obsoleteDeleted, deleted, failures);
      }
      String nextToken = next.toString();
      for (Pointer pointer : pointers) {
        if (System.currentTimeMillis() > deadlineMs) {
          return new DirtyParentStats(
              false, pages, scanned, refreshed, invalidDeleted, obsoleteDeleted, deleted, failures);
        }
        if (pointer == null || pointer.getKey().isBlank()) {
          continue;
        }
        scanned++;
        DirtyParentMarker marker = parseDirtyParentMarker(pointer);
        if (marker == null) {
          if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
            invalidDeleted++;
            deleted++;
          }
          continue;
        }
        try {
          if (obsoleteDirtyParentProjection != null
              && obsoleteDirtyParentProjection.test(marker.accountId(), marker.parentJobId())) {
            if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
              obsoleteDeleted++;
              deleted++;
            }
            continue;
          }
          refreshDirtyParentProjection.accept(marker.accountId(), marker.parentJobId());
          refreshed++;
          if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
            deleted++;
          }
        } catch (RuntimeException e) {
          failures++;
          LOG.warnf(
              e,
              "Failed to refresh reconcile parent projection accountId=%s parentJobId=%s",
              marker.accountId(),
              marker.parentJobId());
        }
      }
      if (nextToken.isBlank()) {
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true,
            pages + 1,
            scanned,
            refreshed,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile dirty-parent refresh pagination token did not advance; aborting scan to"
                + " avoid livelock");
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true,
            pages + 1,
            scanned,
            refreshed,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      dirtyParentScanToken = blankToEmpty(nextToken);
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile dirty-parent refresh pagination hit safety page cap; aborting scan");
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true, pages, scanned, refreshed, invalidDeleted, obsoleteDeleted, deleted, failures);
      }
    }
  }

  private void logMaintenanceSummary(long startedAtMs, DirtyParentStats dirtyParentStats) {
    long elapsedMs = System.currentTimeMillis() - startedAtMs;
    if (!dirtyParentStats.active() && elapsedMs <= 500L) {
      LOG.debugf(
          "runProjectionMaintenanceOnce total_ms=%d projection_completed=%s",
          Long.valueOf(elapsedMs), Boolean.valueOf(dirtyParentStats.completed()));
      return;
    }
    LOG.infof(
        "runProjectionMaintenanceOnce total_ms=%d"
            + " projection_completed=%s projection_pages=%d projection_markers=%d"
            + " projection_refreshed=%d projection_invalid_deleted=%d"
            + " projection_obsolete_deleted=%d"
            + " projection_markers_deleted=%d projection_failures=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(dirtyParentStats.completed()),
        Integer.valueOf(dirtyParentStats.pages()),
        Integer.valueOf(dirtyParentStats.scanned()),
        Integer.valueOf(dirtyParentStats.refreshed()),
        Integer.valueOf(dirtyParentStats.invalidDeleted()),
        Integer.valueOf(dirtyParentStats.obsoleteDeleted()),
        Integer.valueOf(dirtyParentStats.deleted()),
        Integer.valueOf(dirtyParentStats.failures()));
  }

  private DirtyParentMarker parseDirtyParentMarker(Pointer pointer) {
    if (!PointerReferences.isOpaqueMarkerPointer(pointer)) {
      return null;
    }
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

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private record DirtyParentMarker(String accountId, String parentJobId) {}

  private record DirtyParentStats(
      boolean completed,
      int pages,
      int scanned,
      int refreshed,
      int invalidDeleted,
      int obsoleteDeleted,
      int deleted,
      int failures) {
    static DirtyParentStats empty() {
      return new DirtyParentStats(true, 0, 0, 0, 0, 0, 0, 0);
    }

    boolean active() {
      return !completed
          || scanned > 0
          || refreshed > 0
          || invalidDeleted > 0
          || obsoleteDeleted > 0
          || deleted > 0
          || failures > 0;
    }
  }
}
