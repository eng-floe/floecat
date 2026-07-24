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
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileProjectionMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileProjectionMaintenanceService.class);

  public enum RefreshResult {
    COMMITTED,
    OBSOLETE,
    PROJECTION_CONFLICT,
    MARKER_ACK_CONFLICT
  }

  @FunctionalInterface
  public interface RefreshDirtyParentProjection {
    RefreshResult apply(
        String accountId,
        String parentJobId,
        long generation,
        String markerKey,
        long markerVersion);
  }

  private PointerStore pointerStore;
  private RefreshDirtyParentProjection refreshDirtyParentProjection;
  private int readyScanLimit;
  private int maxMarkersPerTick = Integer.MAX_VALUE;

  private volatile String dirtyParentScanToken = "";
  private volatile boolean workHint = true;
  private final AtomicLong workGeneration = new AtomicLong();
  private volatile long nextRecoveryScanAtMs;
  private volatile long earliestDeferredMarkerAtMs = Long.MAX_VALUE;
  private long idleRecoveryMillis = 60_000L;

  public void bind(
      PointerStore pointerStore,
      RefreshDirtyParentProjection refreshDirtyParentProjection,
      int readyScanLimit) {
    this.pointerStore = pointerStore;
    this.refreshDirtyParentProjection = refreshDirtyParentProjection;
    this.readyScanLimit = readyScanLimit;
  }

  public void runProjectionMaintenanceOnce(long maxMillis) {
    runProjectionMaintenanceOnce(maxMillis, maxMarkersPerTick);
  }

  public void runProjectionMaintenanceOnce(long maxMillis, int maxMarkers) {
    long startedAtMs = System.currentTimeMillis();
    if (!workHint && startedAtMs < nextRecoveryScanAtMs) {
      return;
    }
    long generationAtStart = workGeneration.get();
    workHint = true;
    earliestDeferredMarkerAtMs = Long.MAX_VALUE;
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    DirtyParentStats dirtyParentStats = refreshDirtyParents(deadlineMs, Math.max(1, maxMarkers));
    if (dirtyParentStats.completed()
        && dirtyParentStats.failures() == 0
        && dirtyParentStats.commitConflicts() == 0
        && dirtyParentStats.markerAckConflicts() == 0
        && (dirtyParentStats.deleted() == dirtyParentStats.scanned()
            || earliestDeferredMarkerAtMs != Long.MAX_VALUE)
        && workGeneration.get() == generationAtStart) {
      workHint = false;
      long recoveryAt = System.currentTimeMillis() + Math.max(1L, idleRecoveryMillis);
      nextRecoveryScanAtMs = Math.min(recoveryAt, earliestDeferredMarkerAtMs);
    }
    logMaintenanceSummary(startedAtMs, dirtyParentStats);
  }

  public void signalWork() {
    workGeneration.incrementAndGet();
    workHint = true;
  }

  private DirtyParentStats refreshDirtyParents(long deadlineMs, int maxMarkers) {
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
    int commitConflicts = 0;
    int markerAckConflicts = 0;
    int failures = 0;
    String lastConsumedKey = "";
    while (true) {
      if (scanned >= maxMarkers || System.currentTimeMillis() > deadlineMs) {
        if (!lastConsumedKey.isBlank()) {
          dirtyParentScanToken = pointerStore.pageTokenAfterKey(lastConsumedKey);
        }
        return new DirtyParentStats(
            false,
            pages,
            scanned,
            refreshed,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            commitConflicts,
            markerAckConflicts,
            failures);
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileDirtyParentPointerPrefix(), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true,
            pages,
            scanned,
            refreshed,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            commitConflicts,
            markerAckConflicts,
            failures);
      }
      String nextToken = next.toString();
      for (Pointer pointer : pointers) {
        if (scanned >= maxMarkers || System.currentTimeMillis() > deadlineMs) {
          if (!lastConsumedKey.isBlank()) {
            dirtyParentScanToken = pointerStore.pageTokenAfterKey(lastConsumedKey);
          }
          return new DirtyParentStats(
              false,
              pages,
              scanned,
              refreshed,
              invalidDeleted,
              obsoleteDeleted,
              deleted,
              commitConflicts,
              markerAckConflicts,
              failures);
        }
        if (pointer == null || pointer.getKey().isBlank()) {
          continue;
        }
        lastConsumedKey = pointer.getKey();
        scanned++;
        DirtyParentMarker marker = parseDirtyParentMarker(pointer);
        if (marker == null) {
          if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
            invalidDeleted++;
            deleted++;
          }
          continue;
        }
        if (System.currentTimeMillis() < marker.dirtyAtMs()) {
          earliestDeferredMarkerAtMs = Math.min(earliestDeferredMarkerAtMs, marker.dirtyAtMs());
          continue;
        }
        try {
          RefreshResult result =
              refreshDirtyParentProjection.apply(
                  marker.accountId(),
                  marker.parentJobId(),
                  marker.generation(),
                  pointer.getKey(),
                  pointer.getVersion());
          refreshed++;
          if (result == RefreshResult.COMMITTED) {
            deleted++;
          } else if (result == RefreshResult.OBSOLETE) {
            if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
              obsoleteDeleted++;
              deleted++;
            } else {
              markerAckConflicts++;
            }
          } else if (result == RefreshResult.MARKER_ACK_CONFLICT) {
            markerAckConflicts++;
          } else {
            commitConflicts++;
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
            commitConflicts,
            markerAckConflicts,
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
            commitConflicts,
            markerAckConflicts,
            failures);
      }
      dirtyParentScanToken = blankToEmpty(nextToken);
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile dirty-parent refresh pagination hit safety page cap; aborting scan");
        dirtyParentScanToken = "";
        return new DirtyParentStats(
            true,
            pages,
            scanned,
            refreshed,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            commitConflicts,
            markerAckConflicts,
            failures);
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
            + " projection_markers_deleted=%d projection_retries=%d"
            + " projection_commit_conflicts=%d projection_marker_ack_conflicts=%d"
            + " projection_failures=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(dirtyParentStats.completed()),
        Integer.valueOf(dirtyParentStats.pages()),
        Integer.valueOf(dirtyParentStats.scanned()),
        Integer.valueOf(dirtyParentStats.refreshed()),
        Integer.valueOf(dirtyParentStats.invalidDeleted()),
        Integer.valueOf(dirtyParentStats.obsoleteDeleted()),
        Integer.valueOf(dirtyParentStats.deleted()),
        Integer.valueOf(dirtyParentStats.commitConflicts()),
        Integer.valueOf(dirtyParentStats.commitConflicts()),
        Integer.valueOf(dirtyParentStats.markerAckConflicts()),
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
    String[] fields = payload.split("\\n", -1);
    if (fields.length != 4) {
      return null;
    }
    String accountId = fields[0].trim();
    String parentJobId = fields[1].trim();
    if (accountId.isBlank() || parentJobId.isBlank()) {
      return null;
    }
    try {
      long generation = Long.parseLong(fields[2]);
      long dirtyAtMs = Long.parseLong(fields[3]);
      if (generation < 0L || dirtyAtMs < 0L) {
        return null;
      }
      return new DirtyParentMarker(accountId, parentJobId, generation, dirtyAtMs);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private record DirtyParentMarker(
      String accountId, String parentJobId, long generation, long dirtyAtMs) {}

  private record DirtyParentStats(
      boolean completed,
      int pages,
      int scanned,
      int refreshed,
      int invalidDeleted,
      int obsoleteDeleted,
      int deleted,
      int commitConflicts,
      int markerAckConflicts,
      int failures) {
    static DirtyParentStats empty() {
      return new DirtyParentStats(true, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    boolean active() {
      return !completed
          || scanned > 0
          || refreshed > 0
          || invalidDeleted > 0
          || obsoleteDeleted > 0
          || deleted > 0
          || commitConflicts > 0
          || markerAckConflicts > 0
          || failures > 0;
    }
  }
}
