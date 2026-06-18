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
import ai.floedb.floecat.service.repo.model.Keys;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
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
  private Predicate<StoredReconcileJob> requiresReadyPointer;

  public void bind(
      ReconcileReadyQueueBackend readyQueueBackend,
      ReconcileJobIndexStore jobIndexStore,
      ReconcileLeaseStore leaseStore,
      int readyScanLimit,
      Predicate<StoredReconcileJob> requiresReadyPointer) {
    this.readyQueueBackend = readyQueueBackend;
    this.jobIndexStore = jobIndexStore;
    this.leaseStore = leaseStore;
    this.readyScanLimit = readyScanLimit;
    this.requiresReadyPointer = requiresReadyPointer;
  }

  public Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    return leaseReadyDue(nowMs, request, null);
  }

  public ReadyQueueScanPage scanReadySlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, int pageSize, String pageToken) {
    return readyQueueBackend.scanReadySlice(slice, pageSize, pageToken);
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
    return record != null
        && effective.matches(record.executionPolicy(), pinnedExecutorId, record.jobKind());
  }

  public long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  public String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  public String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  public String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue) {
    if (record == null) {
      return "";
    }
    String normalizedFilterValue = blankToEmpty(filterValue);
    return switch (indexType) {
      case GLOBAL -> readyPointerKeyFor(record, dueAtMs);
      case EXECUTION_CLASS ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionClassPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case EXECUTION_LANE ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionLanePointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case PINNED_EXECUTOR ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByPinnedExecutorPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case JOB_KIND ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByJobKindPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
    };
  }

  public List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    List<String> keys = new java.util.ArrayList<>();
    keys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassKey =
        readyPointerKeyFor(
            record,
            ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            record.executionPolicy().executionClass().name());
    if (!executionClassKey.isBlank()) {
      keys.add(executionClassKey);
    }
    String executionLaneKey =
        readyPointerKeyFor(
            record, ReadyIndexType.EXECUTION_LANE, dueAtMs, record.executionPolicy().lane());
    if (!executionLaneKey.isBlank()) {
      keys.add(executionLaneKey);
    }
    String pinnedExecutorKey =
        readyPointerKeyFor(
            record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId());
    if (!pinnedExecutorKey.isBlank()) {
      keys.add(pinnedExecutorKey);
    }
    String jobKindKey =
        readyPointerKeyFor(record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name());
    if (!jobKindKey.isBlank()) {
      keys.add(jobKindKey);
    }
    return List.copyOf(keys);
  }

  private Optional<LeasedJob> leaseReadyDueFromSelection(
      long nowMs, LeaseRequest request, ReadyIndexSelection selection, LeaseScanStats scanStats) {
    String token = "";
    int pages = 0;
    while (true) {
      if (scanBudgetExceeded(scanStats)) {
        return Optional.empty();
      }
      if (scanStats != null) {
        scanStats.scanCount++;
      }
      ReadyQueueScanPage page =
          readyQueueBackend.scanReadySlice(selection.slice(), readyScanLimit, token);
      if (page.entries().isEmpty()) {
        return Optional.empty();
      }

      for (ReadyQueueEntry candidate : page.entries()) {
        if (scanStats != null) {
          scanStats.candidateCount++;
        }
        if (candidate.dueAtMs() > nowMs) {
          return Optional.empty();
        }
        if (scanBudgetExceeded(scanStats)) {
          return Optional.empty();
        }
        CanonicalPointerSnapshot canonicalSnapshot =
            readyQueueBackend.loadCanonicalSnapshot(candidate.canonicalPointerKey()).orElse(null);
        if (canonicalSnapshot == null) {
          continue;
        }
        var recordOpt = jobIndexStore.readRecord(canonicalSnapshot);
        if (recordOpt.isEmpty()) {
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        if ("JS_WAITING".equals(record.state)) {
          continue;
        }
        if (!readyPointerMatchesRecord(candidate, record)) {
          continue;
        }
        if (!matchesLeaseRequest(record, request)) {
          continue;
        }
        var leased =
            leaseStore.leaseCanonical(
                candidate.canonicalPointerKey(),
                candidate.readyPointerKey(),
                nowMs,
                canonicalSnapshot,
                record);
        if (leased.isPresent()) {
          return leased;
        }
      }

      String nextToken = blankToEmpty(page.nextPageToken());
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
        return Optional.empty();
      }
    }
  }

  private List<ReadyIndexSelection> readyScanSelections(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    List<ReadyIndexSelection> pinnedSelections =
        effective.executorIds.stream()
            .map(
                executorId ->
                    new ReadyIndexSelection(
                        new ReconcileReadyQueueBackend.ReadyQueueSlice(
                            ReadyIndexType.PINNED_EXECUTOR, executorId)))
            .toList();
    if (!effective.executorIds.isEmpty()) {
      // Executor IDs identify workers that can accept pinned work; they must not exclude
      // ordinary unpinned jobs from leasing.
    }
    if (!effective.lanes.isEmpty() && !effective.lanes.contains("*")) {
      List<ReadyIndexSelection> selections = new java.util.ArrayList<>(pinnedSelections);
      selections.addAll(
          effective.lanes.stream()
              .map(
                  lane ->
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.EXECUTION_LANE, lane)))
              .toList());
      appendGlobalSelection(selections);
      return List.copyOf(selections);
    }
    if (!effective.jobKinds.isEmpty()) {
      List<ReadyIndexSelection> selections = new java.util.ArrayList<>(pinnedSelections);
      selections.addAll(
          effective.jobKinds.stream()
              .map(
                  jobKind ->
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.JOB_KIND, jobKind.name())))
              .toList());
      appendGlobalSelection(selections);
      return List.copyOf(selections);
    }
    if (!effective.executionClasses.isEmpty()) {
      List<ReadyIndexSelection> selections = new java.util.ArrayList<>(pinnedSelections);
      selections.addAll(
          effective.executionClasses.stream()
              .map(
                  executionClass ->
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.EXECUTION_CLASS, executionClass.name())))
              .toList());
      appendGlobalSelection(selections);
      return List.copyOf(selections);
    }
    List<ReadyIndexSelection> selections = new java.util.ArrayList<>(pinnedSelections);
    appendGlobalSelection(selections);
    return List.copyOf(selections);
  }

  private static void appendGlobalSelection(List<ReadyIndexSelection> selections) {
    selections.add(
        new ReadyIndexSelection(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(ReadyIndexType.GLOBAL, "")));
  }

  private boolean readyPointerMatchesRecord(ReadyQueueEntry candidate, StoredReconcileJob record) {
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
    if (record.nextAttemptAtMs != candidate.dueAtMs()) {
      return false;
    }
    if (!readyIndexFilterMatchesRecord(candidate, record)) {
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
      case EXECUTION_LANE -> candidate.filterValue().equals(policy.lane());
      case PINNED_EXECUTOR -> candidate.filterValue().equals(record.pinnedExecutorId());
      case JOB_KIND -> candidate.filterValue().equals(record.jobKind().name());
    };
  }

  private static boolean scanBudgetExceeded(LeaseScanStats scanStats) {
    if (scanStats == null || scanStats.deadlineAtMs <= 0L) {
      return false;
    }
    if (System.currentTimeMillis() < scanStats.deadlineAtMs) {
      return false;
    }
    scanStats.abortedByBudget = true;
    return true;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
