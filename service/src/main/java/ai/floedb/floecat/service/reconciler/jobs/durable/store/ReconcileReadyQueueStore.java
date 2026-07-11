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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

public interface ReconcileReadyQueueStore {
  enum ReadyIndexType {
    GLOBAL,
    EXECUTION_CLASS,
    EXECUTION_LANE,
    PINNED_EXECUTOR,
    JOB_KIND
  }

  final class LeaseScanStats {
    public int scanCount;
    public int candidateCount;
    public int stalePointerSkipCount;
    public int missingRecordSkipCount;
    public int waitingSkipCount;
    public int cancellationBlockedSkipCount;
    public int pointerMismatchSkipCount;
    public int requestMismatchSkipCount;
    public int notDueSkipCount;
    public int leaseRaceSkipCount;
    public int leaseRaceRunningSkipCount;
    public int leaseRaceNotQueuedSkipCount;
    public int leaseRaceTerminalSkipCount;
    public int leaseRaceMissingSkipCount;
    public int leaseRacePointerMismatchSkipCount;
    public int leaseRaceOtherSkipCount;
    public long deadlineAtMs;
    public BooleanSupplier cancelled = () -> false;
    public boolean abortedByDeadline;
    public boolean abortedByCaller;
    private final Map<String, Integer> skipCounts = new LinkedHashMap<>();

    public void recordSkip(String reason) {
      String normalized = reason == null ? "" : reason.trim();
      if (normalized.isBlank()) {
        return;
      }
      skipCounts.merge(normalized, 1, Integer::sum);
      switch (normalized) {
        case "stale_pointer" -> stalePointerSkipCount++;
        case "missing_record" -> missingRecordSkipCount++;
        case "waiting" -> waitingSkipCount++;
        case "cancellation_blocked" -> cancellationBlockedSkipCount++;
        case "pointer_mismatch" -> pointerMismatchSkipCount++;
        case "request_mismatch" -> requestMismatchSkipCount++;
        case "not_due" -> notDueSkipCount++;
        case "lease_race" -> leaseRaceSkipCount++;
        case "lease_race_running" -> leaseRaceRunningSkipCount++;
        case "lease_race_not_queued" -> leaseRaceNotQueuedSkipCount++;
        case "lease_race_terminal" -> leaseRaceTerminalSkipCount++;
        case "lease_race_missing" -> leaseRaceMissingSkipCount++;
        case "lease_race_pointer_mismatch" -> leaseRacePointerMismatchSkipCount++;
        case "lease_race_other" -> leaseRaceOtherSkipCount++;
        default -> {}
      }
    }

    public Map<String, Integer> skipCounts() {
      return Map.copyOf(skipCounts);
    }

    public boolean shouldStop() {
      if (cancelled != null && cancelled.getAsBoolean()) {
        abortedByCaller = true;
        return true;
      }
      if (deadlineAtMs > 0L && System.currentTimeMillis() >= deadlineAtMs) {
        abortedByDeadline = true;
        return true;
      }
      return false;
    }
  }

  record ReadyQueueEntry(
      String readyPointerKey,
      String canonicalPointerKey,
      String accountId,
      String jobId,
      long dueAtMs,
      ReadyIndexType indexType,
      String filterValue) {}

  record ReadyQueueScanPage(List<ReadyQueueEntry> entries, String nextPageToken) {}

  void bind(
      ReconcileReadyQueueBackend readyQueueBackend,
      ReconcileJobIndexStore jobIndexStore,
      ReconcileLeaseStore leaseStore,
      int readyScanLimit,
      Predicate<StoredReconcileJob> requiresReadyPointer,
      Predicate<StoredReconcileJob> blockedByCancellation);

  ReadyQueueScanPage scanReadySlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, int pageSize, String pageToken);

  Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request);

  Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request, LeaseScanStats scanStats);

  boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request);

  boolean readyPointerMatchesRecord(ReadyQueueEntry candidate, StoredReconcileJob record);

  long readyPointerDueAt(StoredReconcileJob record);

  String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs);

  String readyPointerKeyForDue(String accountId, String laneKey, String jobId, long dueAtMs);

  String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue);

  List<String> readyPointerKeys(StoredReconcileJob record);
}
