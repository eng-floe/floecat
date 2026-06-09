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

  sealed interface JobIndexWriteOp permits JobIndexUpsert, JobIndexDelete {}

  record JobIndexUpsert(
      String pointerKey, long expectedVersion, String blobUri, PointerReferenceKind referenceKind)
      implements JobIndexWriteOp {}

  record JobIndexDelete(String pointerKey, long expectedVersion) implements JobIndexWriteOp {}

  record JobIndexWriteBatch(List<JobIndexWriteOp> writes, ReadyQueueMutation readyMutation) {
    public static JobIndexWriteBatch empty() {
      return new JobIndexWriteBatch(List.of(), ReadyQueueMutation.empty());
    }
  }

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
      String resultBlobUri,
      StoredReconcileJob record) {}

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

  // Contract: returned results are only non-created outcomes that the caller must resolve
  // explicitly, currently store-side dedupe or failure. Successfully created inserts are omitted
  // from the result list and are identified by their absence.

  StoredJobPage listStoredJobs(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states);

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

  JobIndexWriteBatch combineWriteBatches(List<JobIndexWriteBatch> batches);

  boolean compareAndSetCanonicalMutations(List<CanonicalRecordMutation> mutations);

  StoredReconcileJob cloneStoredRecord(StoredReconcileJob source);
}
