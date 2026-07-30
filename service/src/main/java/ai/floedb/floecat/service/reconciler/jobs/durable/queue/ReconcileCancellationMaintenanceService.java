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
public class ReconcileCancellationMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileCancellationMaintenanceService.class);

  @FunctionalInterface
  public interface CleanupCancellationRoot {
    CancellationCleanupResult accept(
        CancellationCleanupRequest request, int childPageSize, long deadlineMs);
  }

  @FunctionalInterface
  public interface IsObsoleteCancellationRoot {
    boolean test(CancellationCleanupRequest request, long deadlineMs);
  }

  private PointerStore pointerStore;
  private CleanupCancellationRoot cleanupCancellationRoot;
  private IsObsoleteCancellationRoot isObsoleteCancellationRoot;
  private int readyScanLimit;
  private volatile String cancellationScanToken = "";

  public void bind(
      PointerStore pointerStore,
      CleanupCancellationRoot cleanupCancellationRoot,
      IsObsoleteCancellationRoot isObsoleteCancellationRoot,
      int readyScanLimit) {
    this.pointerStore = pointerStore;
    this.cleanupCancellationRoot = cleanupCancellationRoot;
    this.isObsoleteCancellationRoot = isObsoleteCancellationRoot;
    this.readyScanLimit = readyScanLimit;
  }

  public void runCancellationMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    CancellationStats stats = cleanupCancellationMarkers(deadlineMs);
    logMaintenanceSummary(startedAtMs, stats);
  }

  private CancellationStats cleanupCancellationMarkers(long deadlineMs) {
    if (pointerStore == null || cleanupCancellationRoot == null) {
      return CancellationStats.empty();
    }
    String token = blankToEmpty(cancellationScanToken);
    int pages = 0;
    int scanned = 0;
    int cleaned = 0;
    int deleted = 0;
    int invalidDeleted = 0;
    int obsoleteDeleted = 0;
    int failures = 0;
    int paused = 0;
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        return new CancellationStats(
            false,
            pages,
            scanned,
            cleaned,
            paused,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileCancellationCleanupPointerPrefix(), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        cancellationScanToken = "";
        return new CancellationStats(
            true,
            pages,
            scanned,
            cleaned,
            paused,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      String nextToken = blankToEmpty(next.toString());
      for (Pointer pointer : pointers) {
        if (System.currentTimeMillis() > deadlineMs) {
          return new CancellationStats(
              false,
              pages,
              scanned,
              cleaned,
              paused,
              invalidDeleted,
              obsoleteDeleted,
              deleted,
              failures);
        }
        if (pointer == null || pointer.getKey().isBlank()) {
          continue;
        }
        scanned++;
        CancellationCleanupMarker marker = parseCancellationCleanupMarker(pointer);
        if (marker == null) {
          if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
            invalidDeleted++;
            deleted++;
          }
          continue;
        }
        if (marker.request().paused()) {
          if (isObsoleteCancellationRoot(marker.request(), deadlineMs)) {
            if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
              obsoleteDeleted++;
              deleted++;
            } else {
              paused++;
            }
          } else {
            paused++;
          }
          continue;
        }
        try {
          CancellationCleanupResult cleanup =
              cleanupCancellationRoot.accept(
                  marker.request(), cancellationChildPageSize(), deadlineMs);
          cleaned++;
          if (cleanup.complete()) {
            if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
              deleted++;
            }
          } else {
            updateCancellationCleanupMarker(pointer, marker.request(), cleanup);
          }
        } catch (ObsoleteCancellationCleanupMarkerException e) {
          if (pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion())) {
            obsoleteDeleted++;
            deleted++;
          }
        } catch (RuntimeException e) {
          failures++;
          LOG.warnf(
              e,
              "Failed to cleanup reconcile cancellation accountId=%s rootJobId=%s",
              marker.request().accountId(),
              marker.request().rootJobId());
        }
      }
      if (nextToken.isBlank()) {
        cancellationScanToken = "";
        return new CancellationStats(
            true,
            pages + 1,
            scanned,
            cleaned,
            paused,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile cancellation cleanup pagination token did not advance; aborting scan to"
                + " avoid livelock");
        cancellationScanToken = "";
        return new CancellationStats(
            true,
            pages + 1,
            scanned,
            cleaned,
            paused,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
      cancellationScanToken = nextToken;
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile cancellation cleanup pagination hit safety page cap; aborting scan");
        cancellationScanToken = "";
        return new CancellationStats(
            true,
            pages,
            scanned,
            cleaned,
            paused,
            invalidDeleted,
            obsoleteDeleted,
            deleted,
            failures);
      }
    }
  }

  private boolean isObsoleteCancellationRoot(CancellationCleanupRequest request, long deadlineMs) {
    if (isObsoleteCancellationRoot == null) {
      return false;
    }
    try {
      return isObsoleteCancellationRoot.test(request, deadlineMs);
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed to validate paused reconcile cancellation cleanup marker accountId=%s rootJobId=%s",
          request == null ? "" : request.accountId(),
          request == null ? "" : request.rootJobId());
      return false;
    }
  }

  private int cancellationChildPageSize() {
    return Math.max(1, Math.min(100, readyScanLimit <= 0 ? 100 : readyScanLimit));
  }

  private void updateCancellationCleanupMarker(
      Pointer pointer, CancellationCleanupRequest request, CancellationCleanupResult cleanup) {
    if (pointer == null || request == null || cleanup == null) {
      return;
    }
    CancellationCleanupRequest nextRequest =
        new CancellationCleanupRequest(
            request.accountId(),
            request.rootJobId(),
            blankToEmpty(cleanup.nextChildPageToken()),
            cleanup.delegatedNonTerminalSeen(),
            cleanup.blockingNonTerminalSeen(),
            cleanup.paused());
    Pointer next =
        PointerReferences.asOpaqueMarkerPointer(
                pointer.toBuilder().setVersion(pointer.getVersion() + 1L),
                cancellationCleanupPayload(nextRequest))
            .build();
    pointerStore.compareAndSet(pointer.getKey(), pointer.getVersion(), next);
  }

  private void logMaintenanceSummary(long startedAtMs, CancellationStats stats) {
    long elapsedMs = System.currentTimeMillis() - startedAtMs;
    if (stats.infoQuiet() && elapsedMs <= 500L) {
      LOG.debugf(
          "runCancellationMaintenanceOnce total_ms=%d completed=%s pages=%d scanned=%d"
              + " paused=%d",
          Long.valueOf(elapsedMs),
          Boolean.valueOf(stats.completed()),
          Integer.valueOf(stats.pages()),
          Integer.valueOf(stats.scanned()),
          Integer.valueOf(stats.paused()));
      return;
    }
    LOG.infof(
        "runCancellationMaintenanceOnce total_ms=%d completed=%s pages=%d scanned=%d cleaned=%d"
            + " paused=%d invalid_deleted=%d obsolete_deleted=%d deleted=%d failures=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(stats.completed()),
        Integer.valueOf(stats.pages()),
        Integer.valueOf(stats.scanned()),
        Integer.valueOf(stats.cleaned()),
        Integer.valueOf(stats.paused()),
        Integer.valueOf(stats.invalidDeleted()),
        Integer.valueOf(stats.obsoleteDeleted()),
        Integer.valueOf(stats.deleted()),
        Integer.valueOf(stats.failures()));
  }

  private CancellationCleanupMarker parseCancellationCleanupMarker(Pointer pointer) {
    CancellationCleanupRequest request = cancellationCleanupRequest(pointer);
    return request == null ? null : new CancellationCleanupMarker(request);
  }

  public static CancellationCleanupRequest cancellationCleanupRequest(Pointer pointer) {
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
    String[] parts = payload.split("\n", -1);
    if (parts.length < 2) {
      return null;
    }
    String accountId = parts[0].trim();
    String rootJobId = parts[1].trim();
    if (accountId.isBlank() || rootJobId.isBlank()) {
      return null;
    }
    String childPageToken = parts.length > 2 ? parts[2].trim() : "";
    boolean delegatedNonTerminalSeen = parts.length > 3 && Boolean.parseBoolean(parts[3].trim());
    boolean blockingNonTerminalSeen = parts.length > 4 && Boolean.parseBoolean(parts[4].trim());
    boolean paused = parts.length > 5 && Boolean.parseBoolean(parts[5].trim());
    return new CancellationCleanupRequest(
        accountId,
        rootJobId,
        childPageToken,
        delegatedNonTerminalSeen,
        blockingNonTerminalSeen,
        paused);
  }

  public static String cancellationCleanupPayload(CancellationCleanupRequest request) {
    if (request == null) {
      return "";
    }
    return blankToEmpty(request.accountId())
        + "\n"
        + blankToEmpty(request.rootJobId())
        + "\n"
        + blankToEmpty(request.childPageToken())
        + "\n"
        + Boolean.toString(request.delegatedNonTerminalSeen())
        + "\n"
        + Boolean.toString(request.blockingNonTerminalSeen())
        + "\n"
        + Boolean.toString(request.paused());
  }

  public static RuntimeException obsoleteMarker() {
    return new ObsoleteCancellationCleanupMarkerException();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  public record CancellationCleanupRequest(
      String accountId,
      String rootJobId,
      String childPageToken,
      boolean delegatedNonTerminalSeen,
      boolean blockingNonTerminalSeen,
      boolean paused) {}

  public record CancellationCleanupResult(
      boolean complete,
      String nextChildPageToken,
      boolean delegatedNonTerminalSeen,
      boolean blockingNonTerminalSeen,
      boolean paused) {}

  private record CancellationCleanupMarker(CancellationCleanupRequest request) {}

  private record CancellationStats(
      boolean completed,
      int pages,
      int scanned,
      int cleaned,
      int paused,
      int invalidDeleted,
      int obsoleteDeleted,
      int deleted,
      int failures) {
    static CancellationStats empty() {
      return new CancellationStats(true, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    boolean infoQuiet() {
      return completed
          && cleaned == 0
          && invalidDeleted == 0
          && obsoleteDeleted == 0
          && deleted == 0
          && failures == 0;
    }
  }

  private static final class ObsoleteCancellationCleanupMarkerException extends RuntimeException {}
}
