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

package ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory;

import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

/** Test-scope ready queue implementation for the in-memory durable store path. */
public final class InMemoryReconcileReadyQueueStore implements ReconcileReadyQueueStore {
  private static final Logger LOG = Logger.getLogger(InMemoryReconcileReadyQueueStore.class);

  private record ReadyIndexSelection(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {}

  private ReconcileReadyQueueBackend readyQueueBackend;
  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileLeaseStore leaseStore;
  private int readyScanLimit;
  private Predicate<StoredReconcileJob> requiresReadyPointer;

  @Override
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

  @Override
  public Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    return leaseReadyDue(nowMs, request, null);
  }

  @Override
  public ReadyQueueScanPage scanReadySlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, int pageSize, String pageToken) {
    return readyQueueBackend.scanReadySlice(slice, pageSize, pageToken);
  }

  @Override
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

  @Override
  public boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    String pinnedExecutorId = record == null ? "" : record.pinnedExecutorId();
    if (!blank(pinnedExecutorId) && !effective.executorIds.contains(pinnedExecutorId)) {
      return false;
    }
    return record != null
        && effective.matches(record.executionPolicy(), pinnedExecutorId, record.jobKind());
  }

  @Override
  public long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  @Override
  public String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  @Override
  public String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  @Override
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
      case BY_PRIORITY -> {
        if (normalizedFilterValue.isBlank()) {
          yield "";
        }
        try {
          yield Keys.reconcileReadyByPriorityPointerByDue(
              Integer.parseInt(normalizedFilterValue), dueAtMs, record.accountId, record.jobId);
        } catch (NumberFormatException ignored) {
          yield "";
        }
      }
    };
  }

  @Override
  public List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassReadyKey =
        readyPointerKeyFor(
            record,
            ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            executionPolicy.executionClass().name());
    if (!executionClassReadyKey.isBlank()) {
      readyKeys.add(executionClassReadyKey);
    }
    String executionLaneReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.EXECUTION_LANE, dueAtMs, executionPolicy.lane());
    if (!executionLaneReadyKey.isBlank()) {
      readyKeys.add(executionLaneReadyKey);
    }
    if (!blank(record.pinnedExecutorId())) {
      String pinnedReadyKey =
          readyPointerKeyFor(
              record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId());
      if (!pinnedReadyKey.isBlank()) {
        readyKeys.add(pinnedReadyKey);
      }
    }
    String kindReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name());
    if (!kindReadyKey.isBlank()) {
      readyKeys.add(kindReadyKey);
    }
    return readyKeys;
  }

  private Optional<LeasedJob> leaseReadyDueFromSelection(
      long nowMs, LeaseRequest request, ReadyIndexSelection selection, LeaseScanStats scanStats) {
    String token = "";
    int pages = 0;
    while (true) {
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

      String nextToken = page.nextPageToken();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid livelock");
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
    List<ReadyIndexSelection> selections = new ArrayList<>();
    List<String> executorIds =
        effective.executorIds.stream()
            .sorted()
            .filter(executorId -> !executorId.isBlank())
            .toList();
    for (String executorId : executorIds) {
      selections.add(
          new ReadyIndexSelection(
              new ReconcileReadyQueueBackend.ReadyQueueSlice(
                  ReadyIndexType.PINNED_EXECUTOR, executorId)));
    }
    if (!effective.lanes.isEmpty() && !effective.lanes.contains(LeaseRequest.anyLaneToken())) {
      effective.lanes.stream()
          .sorted()
          .filter(lane -> !lane.isBlank())
          .forEach(
              lane ->
                  selections.add(
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.EXECUTION_LANE, lane))));
    }
    if (!effective.jobKinds.isEmpty()) {
      effective.jobKinds.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              jobKind ->
                  selections.add(
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.JOB_KIND, jobKind))));
    }
    if (!effective.executionClasses.isEmpty()) {
      effective.executionClasses.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              executionClass ->
                  selections.add(
                      new ReadyIndexSelection(
                          new ReconcileReadyQueueBackend.ReadyQueueSlice(
                              ReadyIndexType.EXECUTION_CLASS, executionClass))));
    }
    selections.add(
        new ReadyIndexSelection(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(ReadyIndexType.GLOBAL, "")));
    return selections;
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
    ReconcileExecutionPolicy policy = record.executionPolicy();
    return switch (candidate.indexType()) {
      case GLOBAL -> true;
      case EXECUTION_CLASS -> candidate.filterValue().equals(policy.executionClass().name());
      case EXECUTION_LANE -> candidate.filterValue().equals(policy.lane());
      case PINNED_EXECUTOR -> candidate.filterValue().equals(record.pinnedExecutorId());
      case JOB_KIND -> candidate.filterValue().equals(record.jobKind().name());
      case BY_PRIORITY ->
          candidate.filterValue().equals(String.valueOf(record.priorityClass().order));
    };
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
