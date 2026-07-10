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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NativeReconcileReadyQueueStore implements ReconcileReadyQueueStore {
  private static final Logger LOG = Logger.getLogger(NativeReconcileReadyQueueStore.class);

  private record ReadyIndexSelection(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {}

  private ReconcileReadyQueueBackend readyQueueBackend;
  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileLeaseStore leaseStore;
  private int readyScanLimit;
  private static final int MAX_PAGES_PER_LEASE_SELECTION = 1;
  private static final int MAX_CANDIDATES_PER_LEASE_SELECTION = 16;
  private Predicate<StoredReconcileJob> requiresReadyPointer;
  private Predicate<StoredReconcileJob> blockedByCancellation;
  private final AtomicInteger pinnedSelectionCursor = new AtomicInteger();
  private final AtomicInteger unpinnedSelectionCursor = new AtomicInteger();
  private final ConcurrentMap<ReconcileReadyQueueBackend.ReadyQueueSlice, String> scanPageCursors =
      new ConcurrentHashMap<>();

  public void bind(
      ReconcileReadyQueueBackend readyQueueBackend,
      ReconcileJobIndexStore jobIndexStore,
      ReconcileLeaseStore leaseStore,
      int readyScanLimit,
      Predicate<StoredReconcileJob> requiresReadyPointer,
      Predicate<StoredReconcileJob> blockedByCancellation) {
    this.readyQueueBackend = readyQueueBackend;
    this.jobIndexStore = jobIndexStore;
    this.leaseStore = leaseStore;
    this.readyScanLimit = readyScanLimit;
    this.requiresReadyPointer = requiresReadyPointer;
    this.blockedByCancellation = blockedByCancellation;
  }

  public Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    return leaseReadyDue(nowMs, request, null);
  }

  public ReadyQueueScanPage scanReadySlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, int pageSize, String pageToken) {
    return readyQueueBackend.scanReadySlice(slice, pageSize, pageToken, null);
  }

  public Optional<LeasedJob> leaseReadyDue(
      long nowMs, LeaseRequest request, LeaseScanStats scanStats) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    for (ReadyIndexSelection selection : readyScanSelections(effective)) {
      Optional<LeasedJob> leased =
          leaseReadyDueFromSelection(nowMs, effective, selection, scanStats);
      if (leased.isPresent()) {
        return leased;
      }
    }
    return Optional.empty();
  }

  public boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    String pinnedExecutorId = record == null ? "" : record.pinnedExecutorId();
    if (!blank(pinnedExecutorId) && !effective.executorIds.contains(pinnedExecutorId)) {
      return false;
    }
    if (record == null) {
      return false;
    }
    ReconcileExecutionPolicy policy = record.executionPolicy();
    boolean classMatches =
        effective.executionClasses.isEmpty()
            || effective.executionClasses.contains(policy.executionClass());
    boolean laneMatches =
        effective.lanes.isEmpty()
            || effective.lanes.contains(LeaseRequest.anyLaneToken())
            || effective.lanes.contains(policy.lane())
            || effective.lanes.contains(blankToEmpty(record.laneKey));
    boolean kindMatches =
        effective.jobKinds.isEmpty() || effective.jobKinds.contains(record.jobKind());
    return classMatches && laneMatches && kindMatches;
  }

  public long readyPointerDueAt(StoredReconcileJob record) {
    return ReadyQueueKeys.readyPointerDueAt(record);
  }

  public String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return ReadyQueueKeys.readyPointerKeyFor(record, dueAtMs);
  }

  public String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return ReadyQueueKeys.readyPointerKeyForDue(accountId, laneKey, jobId, dueAtMs);
  }

  public String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue) {
    return ReadyQueueKeys.readyPointerKeyFor(record, indexType, dueAtMs, filterValue);
  }

  public List<String> readyPointerKeys(StoredReconcileJob record) {
    return ReadyQueueKeys.readyPointerKeys(record, requiresReadyPointer);
  }

  private Optional<LeasedJob> leaseReadyDueFromSelection(
      long nowMs, LeaseRequest request, ReadyIndexSelection selection, LeaseScanStats scanStats) {
    ReconcileReadyQueueBackend.ReadyQueueSlice slice = selection.slice();
    String token = scanPageCursor(slice);
    int pages = 0;
    while (true) {
      if (shouldStop(scanStats)) {
        return Optional.empty();
      }
      if (scanStats != null) {
        scanStats.scanCount++;
      }
      ReadyQueueScanPage page =
          readyQueueBackend.scanReadySlice(
              slice,
              Math.min(readyScanLimit, MAX_CANDIDATES_PER_LEASE_SELECTION),
              token,
              scanStats);
      if (page.entries().isEmpty()) {
        clearScanPageCursor(slice);
        return Optional.empty();
      }

      for (ReadyQueueEntry candidate : page.entries()) {
        if (shouldStop(scanStats)) {
          return Optional.empty();
        }
        if (scanStats != null) {
          scanStats.candidateCount++;
        }
        if (candidate.dueAtMs() > nowMs) {
          clearScanPageCursor(slice);
          return Optional.empty();
        }
        CanonicalPointerSnapshot canonicalSnapshot =
            readyQueueBackend
                .loadCanonicalSnapshot(candidate.canonicalPointerKey(), scanStats)
                .orElse(null);
        if (shouldStop(scanStats)) {
          return Optional.empty();
        }
        if (canonicalSnapshot == null) {
          continue;
        }
        var recordOpt = jobIndexStore.readRecord(canonicalSnapshot);
        if (shouldStop(scanStats)) {
          return Optional.empty();
        }
        if (recordOpt.isEmpty()) {
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        if ("JS_WAITING".equals(record.state)) {
          continue;
        }
        if (blockedByCancellation != null
            && Boolean.TRUE.equals(blockedByCancellation.test(record))) {
          continue;
        }
        if (!readyPointerMatchesRecord(candidate, record)) {
          continue;
        }
        // Not stale, just not eligible for this requester: leave the pointer for another lane.
        if (!matchesLeaseRequest(record, request)) {
          continue;
        }
        if (shouldStop(scanStats)) {
          return Optional.empty();
        }
        var leased =
            leaseStore.leaseCanonical(
                candidate.canonicalPointerKey(),
                candidate.readyPointerKey(),
                nowMs,
                canonicalSnapshot,
                record);
        if (leased.isPresent()) {
          clearScanPageCursor(slice);
          return leased;
        }
      }

      String nextToken = blankToEmpty(page.nextPageToken());
      if (nextToken.isBlank()) {
        clearScanPageCursor(slice);
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        clearScanPageCursor(slice);
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        return Optional.empty();
      }
      rememberScanPageCursor(slice, nextToken);
      pages++;
      if (pages >= MAX_PAGES_PER_LEASE_SELECTION) {
        return Optional.empty();
      }
      token = nextToken;
    }
  }

  private List<ReadyIndexSelection> readyScanSelections(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    List<ReadyIndexSelection> selections = new java.util.ArrayList<>(2);
    choosePinnedSelection(effective).ifPresent(selections::add);
    chooseUnpinnedSelection(effective).ifPresent(selections::add);
    return List.copyOf(selections);
  }

  private Optional<ReadyIndexSelection> choosePinnedSelection(LeaseRequest request) {
    List<String> executorIds =
        request.executorIds.stream()
            .map(NativeReconcileReadyQueueStore::blankToEmpty)
            .filter(executorId -> !executorId.isBlank())
            .sorted()
            .toList();
    if (executorIds.isEmpty()) {
      return Optional.empty();
    }
    String executorId = executorIds.get(nextIndex(pinnedSelectionCursor, executorIds.size()));
    return Optional.of(
        new ReadyIndexSelection(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(
                ReadyIndexType.PINNED_EXECUTOR, executorId)));
  }

  private Optional<ReadyIndexSelection> chooseUnpinnedSelection(LeaseRequest request) {
    List<String> lanes =
        request.lanes.stream()
            .map(NativeReconcileReadyQueueStore::blankToEmpty)
            .filter(lane -> !lane.isBlank() && !LeaseRequest.anyLaneToken().equals(lane))
            .sorted()
            .toList();
    if (!lanes.isEmpty()) {
      String lane = lanes.get(nextIndex(unpinnedSelectionCursor, lanes.size()));
      return Optional.of(
          new ReadyIndexSelection(
              new ReconcileReadyQueueBackend.ReadyQueueSlice(ReadyIndexType.EXECUTION_LANE, lane)));
    }
    List<String> jobKinds = request.jobKinds.stream().sorted().map(Enum::name).toList();
    if (!jobKinds.isEmpty()) {
      String jobKind = jobKinds.get(nextIndex(unpinnedSelectionCursor, jobKinds.size()));
      return Optional.of(
          new ReadyIndexSelection(
              new ReconcileReadyQueueBackend.ReadyQueueSlice(ReadyIndexType.JOB_KIND, jobKind)));
    }
    List<String> executionClasses =
        request.executionClasses.stream().sorted().map(Enum::name).toList();
    if (!executionClasses.isEmpty()) {
      String executionClass =
          executionClasses.get(nextIndex(unpinnedSelectionCursor, executionClasses.size()));
      return Optional.of(
          new ReadyIndexSelection(
              new ReconcileReadyQueueBackend.ReadyQueueSlice(
                  ReadyIndexType.EXECUTION_CLASS, executionClass)));
    }
    return Optional.of(
        new ReadyIndexSelection(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(ReadyIndexType.GLOBAL, "")));
  }

  private static int nextIndex(AtomicInteger cursor, int size) {
    return Math.floorMod(cursor.getAndIncrement(), size);
  }

  private String scanPageCursor(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    return blankToEmpty(scanPageCursors.get(slice));
  }

  private void rememberScanPageCursor(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, String nextToken) {
    String normalized = blankToEmpty(nextToken);
    if (slice == null || normalized.isBlank()) {
      return;
    }
    scanPageCursors.put(slice, normalized);
  }

  private void clearScanPageCursor(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    if (slice != null) {
      scanPageCursors.remove(slice);
    }
  }

  public boolean readyPointerMatchesRecord(ReadyQueueEntry candidate, StoredReconcileJob record) {
    if (record == null
        || candidate == null
        || candidate.readyPointerKey() == null
        || candidate.readyPointerKey().isBlank()) {
      return false;
    }
    if ("JS_WAITING".equals(record.state)) {
      return false;
    }
    if (!Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return false;
    }
    if (!candidate.accountId().equals(record.accountId)
        || !candidate.jobId().equals(record.jobId)) {
      return false;
    }
    if (record.nextAttemptAtMs > 0L && record.nextAttemptAtMs != candidate.dueAtMs()) {
      return false;
    }
    if (!readyIndexFilterMatchesRecord(candidate, record)) {
      return false;
    }
    if (!blank(record.pinnedExecutorId())
        && candidate.indexType() != ReadyIndexType.PINNED_EXECUTOR) {
      return false;
    }
    String expectedKey =
        readyPointerKeyFor(
            record, candidate.indexType(), candidate.dueAtMs(), candidate.filterValue());
    if (!candidate.readyPointerKey().equals(expectedKey)) {
      return false;
    }
    return candidate.indexType() != ReadyIndexType.GLOBAL
        || candidate.readyPointerKey().equals(record.readyPointerKey);
  }

  private boolean readyIndexFilterMatchesRecord(
      ReadyQueueEntry candidate, StoredReconcileJob record) {
    if (candidate == null || record == null) {
      return false;
    }
    ReconcileExecutionPolicy policy = record.executionPolicy();
    return switch (candidate.indexType()) {
      case GLOBAL -> true;
      case EXECUTION_CLASS -> candidate.filterValue().equals(policy.executionClass().name());
      case EXECUTION_LANE -> candidate.filterValue().equals(blankToEmpty(record.laneKey));
      case PINNED_EXECUTOR -> candidate.filterValue().equals(record.pinnedExecutorId());
      case JOB_KIND -> candidate.filterValue().equals(record.jobKind().name());
    };
  }

  private static boolean shouldStop(LeaseScanStats scanStats) {
    return scanStats != null && scanStats.shouldStop();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
