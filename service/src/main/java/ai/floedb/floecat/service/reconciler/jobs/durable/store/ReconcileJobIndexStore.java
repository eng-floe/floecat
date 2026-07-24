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

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public interface ReconcileJobIndexStore {
  record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  record StoredJobPage(List<StoredReconcileJob> records, String nextPageToken) {}

  record IndexBackfillResult(boolean updated, boolean retryable) {
    public static IndexBackfillResult unchanged() {
      return new IndexBackfillResult(false, false);
    }
  }

  record ReadyQueueWrite(
      String readyPointerKey, String canonicalPointerKey, PointerReferenceKind referenceKind) {}

  record ReadyQueueMutation(List<ReadyQueueWrite> upserts, List<String> deletes) {
    public static ReadyQueueMutation empty() {
      return new ReadyQueueMutation(List.of(), List.of());
    }

    public boolean isEmpty() {
      return upserts.isEmpty() && deletes.isEmpty();
    }
  }

  sealed interface JobIndexWriteOp
      permits JobIndexUpsert, JobIndexDelete, JobIndexCheck, JobIndexCheckAbsent {}

  record JobIndexUpsert(
      String pointerKey,
      long expectedVersion,
      String blobUri,
      PointerReferenceKind referenceKind,
      ReconcileJobIndexCleanupManifest cleanupManifest)
      implements JobIndexWriteOp {
    public JobIndexUpsert {
      cleanupManifest =
          cleanupManifest == null ? ReconcileJobIndexCleanupManifest.EMPTY : cleanupManifest;
    }

    public JobIndexUpsert(
        String pointerKey,
        long expectedVersion,
        String blobUri,
        PointerReferenceKind referenceKind) {
      this(
          pointerKey,
          expectedVersion,
          blobUri,
          referenceKind,
          ReconcileJobIndexCleanupManifest.EMPTY);
    }
  }

  record JobIndexDelete(
      String pointerKey,
      long expectedVersion,
      String expectedCanonicalPointerKey,
      String expectedLookupStoragePartitionKey,
      boolean requireCleanupLock,
      boolean allowAbsent)
      implements JobIndexWriteOp {
    public JobIndexDelete {
      expectedCanonicalPointerKey =
          expectedCanonicalPointerKey == null ? "" : expectedCanonicalPointerKey;
      expectedLookupStoragePartitionKey =
          expectedLookupStoragePartitionKey == null ? "" : expectedLookupStoragePartitionKey;
    }

    public JobIndexDelete(
        String pointerKey,
        long expectedVersion,
        String expectedCanonicalPointerKey,
        String expectedLookupStoragePartitionKey) {
      this(
          pointerKey,
          expectedVersion,
          expectedCanonicalPointerKey,
          expectedLookupStoragePartitionKey,
          false,
          false);
    }

    public JobIndexDelete(
        String pointerKey, long expectedVersion, String expectedCanonicalPointerKey) {
      this(pointerKey, expectedVersion, expectedCanonicalPointerKey, "", false, false);
    }

    public JobIndexDelete(String pointerKey, long expectedVersion) {
      this(pointerKey, expectedVersion, "", "", false, false);
    }
  }

  record JobIndexCheck(String pointerKey, long expectedVersion, boolean requireCleanupLock)
      implements JobIndexWriteOp {
    public JobIndexCheck(String pointerKey, long expectedVersion) {
      this(pointerKey, expectedVersion, false);
    }
  }

  record JobIndexCheckAbsent(String pointerKey) implements JobIndexWriteOp {}

  record JobIndexWriteBatch(List<JobIndexWriteOp> writes, ReadyQueueMutation readyMutation) {
    public static JobIndexWriteBatch empty() {
      return new JobIndexWriteBatch(List.of(), ReadyQueueMutation.empty());
    }
  }

  record JobWritePlan<T>(
      T subject, JobIndexWriteBatch indexBatch, List<PointerStore.CasOp> extraPointerOps) {
    public JobWritePlan {
      indexBatch = indexBatch == null ? JobIndexWriteBatch.empty() : indexBatch;
      extraPointerOps = extraPointerOps == null ? List.of() : List.copyOf(extraPointerOps);
    }
  }

  record JobWriteChunk<T>(
      List<JobWritePlan<T>> plans,
      JobIndexWriteBatch indexBatch,
      List<PointerStore.CasOp> extraPointerOps) {}

  record CanonicalRecordMutation(
      CanonicalPointerSnapshot snapshot, StoredReconcileJob previous, StoredReconcileJob current) {}

  record QueuedJobInsert(
      int index,
      String dedupePointerKey,
      String canonicalKey,
      String lookupKey,
      String parentKey,
      List<String> readyKeys,
      List<String> stateKeys,
      String connectorIndexKey,
      StoredReconcileJob record) {}

  final class BulkEnqueueCommitException extends RuntimeException {
    private final List<Integer> rollbackIndexes;

    public BulkEnqueueCommitException(Throwable cause, List<Integer> rollbackIndexes) {
      super(cause == null ? null : cause.getMessage(), cause);
      this.rollbackIndexes = rollbackIndexes == null ? List.of() : List.copyOf(rollbackIndexes);
    }

    public List<Integer> rollbackIndexes() {
      // Only entries from batches that were never attempted are safe to roll back. The batch that
      // raised the exception may have committed remotely before the client observed the failure.
      return rollbackIndexes;
    }
  }

  @FunctionalInterface
  interface TriConsumer<A, B, C> {
    void accept(A a, B b, C c);
  }

  void bind(
      ReconcileJobIndexBackend jobIndexBackend,
      ReconcilePayloadStore payloadStore,
      ReconcileJobIndexes indexes,
      int casMax,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
      TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition);

  Optional<CanonicalEnvelope> loadByAnyAccount(String jobId);

  Optional<CanonicalEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator);

  Optional<CanonicalEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator);

  Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey);

  Optional<StoredReconcileJob> readCanonicalRecordByKey(String canonicalPointerKey);

  Optional<StoredReconcileJob> readRecord(CanonicalPointerSnapshot canonicalPointer);

  Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey);

  BulkEnqueueItemResult commitQueuedJobInsert(QueuedJobInsert insert);

  BulkEnqueueItemResult commitQueuedJobInsert(
      QueuedJobInsert insert, Supplier<List<CanonicalRecordMutation>> ancestorMutationsSupplier);

  List<BulkEnqueueItemResult> commitQueuedJobInserts(
      List<QueuedJobInsert> inserts,
      Function<List<QueuedJobInsert>, List<CanonicalRecordMutation>> ancestorMutationsBuilder);

  StoredJobPage listStoredJobs(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states);

  StoredJobPage listStoredJobsInState(String state, int pageSize, String pageToken);

  StoredJobPage listStoredJobsPendingStatsCleanup(int pageSize, String pageToken);

  StoredJobPage listStoredChildJobs(
      String accountId, String parentJobId, int pageSize, String pageToken);

  long countStoredChildJobs(String accountId, String parentJobId);

  long countStoredJobsInState(String state);

  long oldestStoredJobTimestampInState(String state);

  // Returns the single authoritative description of canonical job-index writes for a state
  // transition. This is a native reconcile-domain write batch, not a read-repair path.
  JobIndexWriteBatch buildJobIndexWriteBatch(
      CanonicalPointerSnapshot currentSnapshot,
      StoredReconcileJob previous,
      StoredReconcileJob current);

  JobIndexWriteBatch buildJobDeleteBatch(CanonicalPointerSnapshot currentSnapshot);

  IndexBackfillResult backfillStoredJobIndexes(String canonicalPointerKey);

  IndexBackfillResult backfillStoredJobIndexes(
      CanonicalPointerSnapshot snapshot, StoredReconcileJob record);

  int writeItemCount(JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps);

  int maxWriteItemsPerBatch();

  <T> List<JobWriteChunk<T>> chunkJobWritePlans(List<JobWritePlan<T>> plans);

  <T> List<JobWriteChunk<T>> chunkOversizedJobDeletePlan(JobWritePlan<T> plan);

  JobIndexWriteBatch combineWriteBatches(List<JobIndexWriteBatch> batches);

  boolean commitReadyQueueRepairBatch(
      List<CanonicalRecordMutation> mutations, List<ReadyQueueWrite> readyWrites);

  boolean compareAndSetCanonicalMutations(List<CanonicalRecordMutation> mutations);

  default boolean compareAndSetBatchWithPointerOps(
      JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    if (extraPointerOps != null && !extraPointerOps.isEmpty()) {
      return false;
    }
    return compareAndSetCanonicalMutations(List.of());
  }

  StoredReconcileJob cloneStoredRecord(StoredReconcileJob source);

  // Contract: returned results are only non-created outcomes that the caller must resolve
  // explicitly, currently store-side dedupe or failure. Successfully created inserts are omitted
  // from the result list and are identified by their absence.
}
