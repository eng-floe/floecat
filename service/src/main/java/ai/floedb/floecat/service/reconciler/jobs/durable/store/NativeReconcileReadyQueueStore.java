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
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
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
      // P0 guard: after attempting the P0 slice with no result, peek to distinguish
      // "empty P0" (fall-through is fine) from "blocked P0" (preserve executor capacity).
      // A single-entry head scan is cheap — the index is already in the read path.
      if (isP0Selection(selection)) {
        ReadyQueueScanPage peek = readyQueueBackend.scanReadySlice(selection.slice(), 1, "");
        if (!peek.entries().isEmpty()) {
          // P0 has ready entries but none were leasable by this executor. Return empty so
          // the executor retries instead of stealing a slot with lower-priority work.
          return Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Returns {@code true} iff {@code selection} targets the BY_PRIORITY/P0 slice — i.e. the slice
   * reserved for {@link ai.floedb.floecat.stats.spi.StatsPriorityClass#P0_SYNC} jobs.
   */
  private static boolean isP0Selection(ReadyIndexSelection selection) {
    return selection.slice().indexType() == ReconcileReadyQueueStore.ReadyIndexType.BY_PRIORITY
        && String.valueOf(ai.floedb.floecat.stats.spi.StatsPriorityClass.P0_SYNC.order)
            .equals(selection.slice().filterValue());
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

  public List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
    // BY_PRIORITY index: always added so the lease scanner can dispatch by priority class order.
    String priorityReadyKey =
        Keys.reconcileReadyByPriorityPointerByDue(
            record.priorityClass().order, dueAtMs, record.accountId, record.jobId);
    if (!priorityReadyKey.isBlank()) {
      readyKeys.add(priorityReadyKey);
    }
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
    // Keep lease attempts bounded even if the top-ranked candidate is CAS-raced by another
    // executor. A retry re-scans and picks the next best live candidate.
    for (int leaseAttempt = 0; leaseAttempt < 3; leaseAttempt++) {
      LeaseCandidate best = null;
      String token = "";
      int pages = 0;
      while (true) {
        if (scanStats != null) {
          scanStats.scanCount++;
        }
        ReadyQueueScanPage page =
            readyQueueBackend.scanReadySlice(selection.slice(), readyScanLimit, token);
        if (page.entries().isEmpty()) {
          break;
        }

        boolean hitFutureDue = false;
        for (ReadyQueueEntry candidate : page.entries()) {
          if (scanStats != null) {
            scanStats.candidateCount++;
          }
          if (candidate.dueAtMs() > nowMs) {
            // Keys are ordered by due within a slice, so once we hit future-due all remaining
            // entries in this and subsequent pages are also not ready yet.
            hitFutureDue = true;
            break;
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
          LeaseCandidate challenger = new LeaseCandidate(candidate, canonicalSnapshot, record);
          if (isHigherRanked(challenger, best)) {
            best = challenger;
          }
        }

        if (hitFutureDue) {
          break;
        }
        String nextToken = page.nextPageToken();
        if (nextToken.isBlank()) {
          break;
        }
        if (nextToken.equals(token)) {
          LOG.warn(
              "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                  + " livelock");
          break;
        }
        token = nextToken;
        pages++;
        if (pages >= 10_000) {
          LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
          break;
        }
      }

      if (best == null) {
        return Optional.empty();
      }
      var leased =
          leaseStore.leaseCanonical(
              best.entry().canonicalPointerKey(),
              best.entry().readyPointerKey(),
              nowMs,
              best.canonicalSnapshot(),
              best.record());
      if (leased.isPresent()) {
        return leased;
      }
    }
    return Optional.empty();
  }

  private static boolean isHigherRanked(LeaseCandidate challenger, LeaseCandidate currentBest) {
    if (challenger == null) {
      return false;
    }
    if (currentBest == null) {
      return true;
    }
    long challengerScore = challenger.record().executionPolicy().priorityScore();
    long currentScore = currentBest.record().executionPolicy().priorityScore();
    if (challengerScore != currentScore) {
      return challengerScore > currentScore;
    }
    if (challenger.entry().dueAtMs() != currentBest.entry().dueAtMs()) {
      return challenger.entry().dueAtMs() < currentBest.entry().dueAtMs();
    }
    return challenger.record().jobId.compareTo(currentBest.record().jobId) < 0;
  }

  private record LeaseCandidate(
      ReadyQueueEntry entry,
      CanonicalPointerSnapshot canonicalSnapshot,
      StoredReconcileJob record) {}

  private List<ReadyIndexSelection> readyScanSelections(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    List<ReadyIndexSelection> selections = new ArrayList<>();

    // 1. BY_PRIORITY/P0 — scanned unconditionally before all other slices.
    //    P0_SYNC is query-time bounded work; it must never be blocked by a pinned lower-priority
    //    job sitting ahead of it in the scan order. Non-pinned executors skip pinned P0 jobs via
    //    matchesLeaseRequest, so this does not break pinning semantics.
    selections.add(
        new ReadyIndexSelection(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(
                ReadyIndexType.BY_PRIORITY, String.valueOf(StatsPriorityClass.P0_SYNC.order))));

    // 2. Pinned-executor slices — jobs that can only run on this specific executor.
    //    P0 work is already covered above; these slices deliver pinned P1/P2/P3 jobs before the
    //    general priority queue so that pinned assignments are not starved by unpinned work.
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

    // 3. BY_PRIORITY/P1→P3 — remaining priority classes in order after pinned work.
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      if (cls == StatsPriorityClass.P0_SYNC) {
        continue; // already added at position 1
      }
      selections.add(
          new ReadyIndexSelection(
              new ReconcileReadyQueueBackend.ReadyQueueSlice(
                  ReadyIndexType.BY_PRIORITY, String.valueOf(cls.order))));
    }

    // 4. Legacy secondary indexes and GLOBAL fallback (handles jobs enqueued before BY_PRIORITY).
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
