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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NativeReconcileReadyQueueStore implements ReconcileReadyQueueStore {
  private static final Logger LOG = Logger.getLogger(NativeReconcileReadyQueueStore.class);

  private record ReadyIndexSelection(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {}

  private record ScanPageCursorKey(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, String requestKey) {}

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
  private final ConcurrentMap<ScanPageCursorKey, String> scanPageCursors =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<ScanPageCursorKey, ReentrantLock> scanPageLocks =
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
    ScanPageCursorKey cursorKey = new ScanPageCursorKey(slice, leaseRequestCursorKey(request));
    ReentrantLock lock = scanPageLocks.computeIfAbsent(cursorKey, ignored -> new ReentrantLock());
    if (!lock.tryLock()) {
      return Optional.empty();
    }
    try {
      String token = scanPageCursor(cursorKey);
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
          clearScanPageCursor(cursorKey);
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
            recordSkip(scanStats, "not_due");
            clearScanPageCursor(cursorKey);
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
            recordSkip(scanStats, "stale_pointer");
            deleteStaleReadyEntry(candidate);
            continue;
          }
          var recordOpt = jobIndexStore.readRecord(canonicalSnapshot);
          if (shouldStop(scanStats)) {
            return Optional.empty();
          }
          if (recordOpt.isEmpty()) {
            recordSkip(scanStats, "missing_record");
            deleteStaleReadyEntry(candidate);
            continue;
          }
          StoredReconcileJob record = recordOpt.get();
          if ("JS_WAITING".equals(record.state)) {
            recordSkip(scanStats, "waiting");
            deleteStaleReadyEntry(candidate);
            continue;
          }
          ReadyQueuePruneSupport.ReadyEntryPruneReason pruneReason =
              ReadyQueuePruneSupport.readyEntryPruneReason(
                  candidate, this, record, blockedByCancellation);
          if (pruneReason == ReadyQueuePruneSupport.ReadyEntryPruneReason.CANCELLATION_BLOCKED) {
            recordSkip(scanStats, "cancellation_blocked");
            deleteStaleReadyEntry(candidate);
            continue;
          }
          if (pruneReason == ReadyQueuePruneSupport.ReadyEntryPruneReason.STALE) {
            recordSkip(scanStats, "pointer_mismatch");
            deleteStaleReadyEntry(candidate);
            continue;
          }
          // Not stale, just not eligible for this requester: leave the pointer for another lane.
          if (!matchesLeaseRequest(record, request)) {
            recordSkip(scanStats, "request_mismatch");
            continue;
          }
          if (shouldStop(scanStats)) {
            return Optional.empty();
          }
          ReconcileLeaseStore.LeaseAttemptStats leaseAttemptStats =
              new ReconcileLeaseStore.LeaseAttemptStats();
          var leased =
              leaseStore.leaseCanonical(
                  candidate.canonicalPointerKey(),
                  candidate.readyPointerKey(),
                  nowMs,
                  canonicalSnapshot,
                  record,
                  leaseAttemptStats);
          if (leased.isPresent()) {
            clearScanPageCursor(cursorKey);
            return leased;
          }
          recordSkip(
              scanStats, classifyFailedLease(candidate, request, scanStats, leaseAttemptStats));
        }

        String nextToken = blankToEmpty(page.nextPageToken());
        if (nextToken.isBlank()) {
          clearScanPageCursor(cursorKey);
          return Optional.empty();
        }
        if (nextToken.equals(token)) {
          clearScanPageCursor(cursorKey);
          LOG.warn(
              "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                  + " livelock");
          return Optional.empty();
        }
        rememberScanPageCursor(cursorKey, nextToken);
        pages++;
        if (pages >= MAX_PAGES_PER_LEASE_SELECTION) {
          return Optional.empty();
        }
        token = nextToken;
      }
    } finally {
      lock.unlock();
    }
  }

  private static void recordSkip(LeaseScanStats scanStats, String reason) {
    if (scanStats != null) {
      scanStats.recordSkip(reason);
    }
  }

  private String classifyFailedLease(
      ReadyQueueEntry candidate,
      LeaseRequest request,
      LeaseScanStats scanStats,
      ReconcileLeaseStore.LeaseAttemptStats leaseAttemptStats) {
    String leaseFailureReason =
        leaseAttemptStats == null ? "" : blankToEmpty(leaseAttemptStats.failureReason());
    CanonicalPointerSnapshot currentSnapshot =
        readyQueueBackend
            .loadCanonicalSnapshot(candidate.canonicalPointerKey(), scanStats)
            .orElse(null);
    if (currentSnapshot == null) {
      deleteStaleReadyEntry(candidate);
      return "lease_race_missing";
    }
    var currentRecordOpt = jobIndexStore.readRecord(currentSnapshot);
    if (currentRecordOpt.isEmpty()) {
      deleteStaleReadyEntry(candidate);
      return "lease_race_missing";
    }
    StoredReconcileJob currentRecord = currentRecordOpt.get();
    String state = blankToEmpty(currentRecord.state);
    if (isTerminalState(state)) {
      deleteStaleReadyEntry(candidate);
      return "lease_race_terminal";
    }
    if ("JS_RUNNING".equals(state)) {
      return "lease_race_running";
    }
    if (!"JS_QUEUED".equals(state)) {
      deleteStaleReadyEntry(candidate);
      return "lease_race_not_queued";
    }
    if (!readyPointerMatchesRecord(candidate, currentRecord)) {
      deleteStaleReadyEntry(candidate);
      return "lease_race_pointer_mismatch";
    }
    if (!matchesLeaseRequest(currentRecord, request)) {
      return "lease_race_other";
    }
    if (!leaseFailureReason.isBlank()) {
      return "lease_conflict_" + leaseFailureReason;
    }
    return "lease_race";
  }

  private void deleteStaleReadyEntry(ReadyQueueEntry candidate) {
    if (candidate == null || blank(candidate.readyPointerKey())) {
      return;
    }
    readyQueueBackend.deleteReadyEntry(candidate.readyPointerKey());
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

  private String scanPageCursor(ScanPageCursorKey cursorKey) {
    return blankToEmpty(scanPageCursors.get(cursorKey));
  }

  private void rememberScanPageCursor(ScanPageCursorKey cursorKey, String nextToken) {
    String normalized = blankToEmpty(nextToken);
    if (cursorKey == null || cursorKey.slice() == null || normalized.isBlank()) {
      return;
    }
    scanPageCursors.put(cursorKey, normalized);
  }

  private void clearScanPageCursor(ScanPageCursorKey cursorKey) {
    if (cursorKey != null) {
      scanPageCursors.remove(cursorKey);
    }
  }

  private static String leaseRequestCursorKey(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    return "classes="
        + effective.executionClasses.stream().map(Enum::name).sorted().toList()
        + "|lanes="
        + effective.lanes.stream()
            .map(NativeReconcileReadyQueueStore::blankToEmpty)
            .sorted()
            .toList()
        + "|executors="
        + effective.executorIds.stream()
            .map(NativeReconcileReadyQueueStore::blankToEmpty)
            .sorted()
            .toList()
        + "|kinds="
        + effective.jobKinds.stream().map(Enum::name).sorted().toList();
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

  private static boolean isTerminalState(String state) {
    return switch (blankToEmpty(state)) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }
}
