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
import java.util.List;
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
    public long deadlineAtMs;
    public BooleanSupplier cancelled = () -> false;
    public boolean abortedByDeadline;
    public boolean abortedByCaller;
    public int prunedCount;

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
      Predicate<StoredReconcileJob> requiresReadyPointer);

  ReadyQueueScanPage scanReadySlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice, int pageSize, String pageToken);

  Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request);

  Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request, LeaseScanStats scanStats);

  boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request);

  long readyPointerDueAt(StoredReconcileJob record);

  String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs);

  String readyPointerKeyForDue(String accountId, String laneKey, String jobId, long dueAtMs);

  String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue);

  List<String> readyPointerKeys(StoredReconcileJob record);
}
