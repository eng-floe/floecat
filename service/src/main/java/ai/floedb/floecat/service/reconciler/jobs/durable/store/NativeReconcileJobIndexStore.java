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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NativeReconcileJobIndexStore implements ReconcileJobIndexStore {
  private static final Logger LOG = Logger.getLogger(NativeReconcileJobIndexStore.class);
  private static final long INVALID_ORDERED_POINTER_MS = -1L;
  private static final int LIST_SCAN_MAX_PAGES = 1_000;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";
  private static final String STATE_LIST_TOKEN_V1_PREFIX = "v1s:";

  private ReconcileJobIndexBackend jobIndexBackend;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobIndexes indexes;
  private int casMax;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;
  private TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition;

  public void bind(
      ReconcileJobIndexBackend jobIndexBackend,
      ReconcilePayloadStore payloadStore,
      ReconcileJobIndexes indexes,
      int casMax,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
      TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition) {
    this.jobIndexBackend = jobIndexBackend;
    this.payloadStore = payloadStore;
    this.indexes = indexes;
    this.casMax = casMax;
    this.assertImmutableJobIdentityPreserved = assertImmutableJobIdentityPreserved;
    this.logStateTransition = logStateTransition;
  }

  public Optional<CanonicalEnvelope> loadByAnyAccount(String jobId) {
    if (blank(jobId)) {
      return Optional.empty();
    }
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    JobIndexEntrySnapshot lookup = jobIndexBackend.loadIndexEntry(lookupKey).orElse(null);
    if (lookup == null || blank(lookup.blobUri())) {
      return Optional.empty();
    }
    String canonicalPointerKey = lookup.blobUri();
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      return Optional.empty();
    }
    var canonicalRec =
        readRecord(
            new CanonicalPointerSnapshot(
                canonicalPointer.pointerKey(),
                canonicalPointer.blobUri(),
                canonicalPointer.version()));
    if (canonicalRec.isEmpty()) {
      return Optional.empty();
    }
    StoredReconcileJob current = canonicalRec.get();
    if (!jobId.equals(current.jobId)) {
      LOG.warnf(
          "Reconcile lookup pointer mismatch jobId=%s canonicalKey=%s canonicalJobId=%s",
          jobId, canonicalPointerKey, current.jobId);
      return Optional.empty();
    }
    return Optional.of(new CanonicalEnvelope(canonicalPointerKey, current));
  }

  public Optional<CanonicalEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return mutateByCanonicalPointerReturningRecord(loaded.get().canonicalPointerKey(), mutator);
  }

  public Optional<CanonicalEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < casMax; i++) {
      JobIndexEntrySnapshot currentPointer =
          jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return Optional.empty();
      }
      var currentOpt =
          readRecord(
              new CanonicalPointerSnapshot(
                  currentPointer.pointerKey(), currentPointer.blobUri(), currentPointer.version()));
      if (currentOpt.isEmpty()) {
        return Optional.empty();
      }

      StoredReconcileJob baseline = cloneStoredRecord(currentOpt.get());
      StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
      StoredReconcileJob nextRecord = mutator.apply(current);
      if (nextRecord == null) {
        return Optional.empty();
      }
      assertImmutableJobIdentityPreserved.accept(baseline, nextRecord);

      nextRecord.updatedAtMs = System.currentTimeMillis();
      nextRecord.canonicalPointerKey = canonicalPointerKey;

      JobIndexWriteBatch writeBatch =
          buildJobIndexWriteBatch(
              new CanonicalPointerSnapshot(
                  currentPointer.pointerKey(), currentPointer.blobUri(), currentPointer.version()),
              baseline,
              nextRecord);
      if (jobIndexBackend.compareAndSetBatch(writeBatch)) {
        logStateTransition.accept(baseline, nextRecord, "mutate");
        return Optional.of(
            new CanonicalEnvelope(canonicalPointerKey, cloneStoredRecord(nextRecord)));
      }
    }
    return Optional.empty();
  }

  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      return Optional.empty();
    }
    return Optional.of(
        new CanonicalPointerSnapshot(
            canonicalPointer.pointerKey(), canonicalPointer.blobUri(), canonicalPointer.version()));
  }

  public Optional<StoredReconcileJob> readCanonicalRecordByKey(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    return loadCanonicalSnapshot(canonicalPointerKey).flatMap(this::readRecord);
  }

  public Optional<StoredReconcileJob> readRecord(CanonicalPointerSnapshot canonicalPointer) {
    var record = payloadStore.readInlineJobState(canonicalPointer.blobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.canonicalPointerKey();
    return record;
  }

  public Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey) {
    JobIndexEntrySnapshot dedupePointer =
        jobIndexBackend.loadIndexEntry(dedupePointerKey).orElse(null);
    if (dedupePointer == null || blank(dedupePointer.blobUri())) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(dedupePointer.blobUri()).orElse(null);
    if (canonicalPointer == null) {
      return Optional.empty();
    }
    Optional<StoredReconcileJob> record =
        readRecord(
            new CanonicalPointerSnapshot(
                canonicalPointer.pointerKey(),
                canonicalPointer.blobUri(),
                canonicalPointer.version()));
    if (record.isEmpty()) {
      return Optional.empty();
    }
    if (isTerminalState(record.get().state)) {
      jobIndexBackend.compareAndSetBatch(
          new JobIndexWriteBatch(
              List.of(new JobIndexDelete(dedupePointer.pointerKey(), dedupePointer.version())),
              ReadyQueueMutation.empty()));
      return Optional.empty();
    }
    return record;
  }

  public BulkEnqueueItemResult commitQueuedJobInsert(QueuedJobInsert insert) {
    return commitQueuedJobInsert(insert, List::of);
  }

  @Override
  public BulkEnqueueItemResult commitQueuedJobInsert(
      QueuedJobInsert insert, Supplier<List<CanonicalRecordMutation>> ancestorMutationsSupplier) {
    for (int attempt = 0; attempt < casMax; attempt++) {
      JobIndexEntrySnapshot existingDedupePointer =
          jobIndexBackend.loadIndexEntry(insert.dedupePointerKey()).orElse(null);
      var existing = loadActiveFromDedupe(insert.dedupePointerKey());
      if (existing.isPresent()) {
        return new BulkEnqueueItemResult(insert.index(), existing.get().jobId, false, "");
      }
      List<CanonicalRecordMutation> ancestorMutations =
          ancestorMutationsSupplier == null ? List.of() : ancestorMutationsSupplier.get();
      JobIndexWriteBatch batch =
          combineWriteBatches(
              prependInsertBatch(
                  queuedJobInsertOps(insert, existingDedupePointer), ancestorMutations));
      if (jobIndexBackend.compareAndSetBatch(batch)) {
        return null;
      }
    }
    return new BulkEnqueueItemResult(
        insert.index(), "", false, "Unable to enqueue reconcile job after CAS retries");
  }

  @Override
  public List<BulkEnqueueItemResult> commitQueuedJobInserts(
      List<QueuedJobInsert> inserts,
      Function<List<QueuedJobInsert>, List<CanonicalRecordMutation>> ancestorMutationsBuilder) {
    if (inserts == null || inserts.isEmpty()) {
      return List.of();
    }
    List<QueuedJobInsert> pending = new ArrayList<>(inserts);
    List<BulkEnqueueItemResult> results = new ArrayList<>();
    for (int attempt = 0; attempt < casMax; attempt++) {
      if (pending.isEmpty()) {
        return results;
      }
      List<QueuedJobInsert> nextPending = new ArrayList<>(pending.size());
      List<JobIndexWriteBatch> insertBatches = new ArrayList<>(pending.size());
      for (QueuedJobInsert insert : pending) {
        var existing = loadActiveFromDedupe(insert.dedupePointerKey());
        if (existing.isPresent()) {
          results.add(new BulkEnqueueItemResult(insert.index(), existing.get().jobId, false, ""));
          continue;
        }
        JobIndexEntrySnapshot existingDedupePointer =
            jobIndexBackend.loadIndexEntry(insert.dedupePointerKey()).orElse(null);
        insertBatches.add(queuedJobInsertOps(insert, existingDedupePointer));
        nextPending.add(insert);
      }
      if (nextPending.isEmpty()) {
        return results;
      }
      List<CanonicalRecordMutation> ancestorMutations =
          ancestorMutationsBuilder == null
              ? List.of()
              : ancestorMutationsBuilder.apply(nextPending);
      JobIndexWriteBatch batch =
          combineWriteBatches(prependInsertBatches(insertBatches, ancestorMutations));
      if (jobIndexBackend.compareAndSetBatch(batch)) {
        return results;
      }
      pending = nextPending;
    }
    for (QueuedJobInsert insert : pending) {
      results.add(
          new BulkEnqueueItemResult(
              insert.index(), "", false, "Unable to enqueue reconcile job after CAS retries"));
    }
    return results;
  }

  @Override
  public JobIndexWriteBatch combineWriteBatches(List<JobIndexWriteBatch> batches) {
    if (batches == null || batches.isEmpty()) {
      return JobIndexWriteBatch.empty();
    }
    List<JobIndexWriteOp> writes = new ArrayList<>();
    List<ReadyQueueWrite> readyUpserts = new ArrayList<>();
    List<String> readyDeletes = new ArrayList<>();
    for (JobIndexWriteBatch batch : batches) {
      if (batch == null) {
        continue;
      }
      writes.addAll(batch.writes());
      readyUpserts.addAll(batch.readyMutation().upserts());
      readyDeletes.addAll(batch.readyMutation().deletes());
    }
    return new JobIndexWriteBatch(
        List.copyOf(writes),
        new ReadyQueueMutation(List.copyOf(readyUpserts), List.copyOf(readyDeletes)));
  }

  @Override
  public boolean compareAndSetCanonicalMutations(List<CanonicalRecordMutation> mutations) {
    if (mutations == null || mutations.isEmpty()) {
      return true;
    }
    List<JobIndexWriteBatch> batches = new ArrayList<>(mutations.size());
    for (CanonicalRecordMutation mutation : mutations) {
      if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
        continue;
      }
      batches.add(
          buildJobIndexWriteBatch(mutation.snapshot(), mutation.previous(), mutation.current()));
    }
    return jobIndexBackend.compareAndSetBatch(combineWriteBatches(batches));
  }

  public StoredJobPage listStoredJobs(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    Set<String> normalizedStates = normalizeStateFilter(states);
    if (!normalizedStates.isEmpty()) {
      List<String> orderedStates = orderedStateFilter(normalizedStates);
      if (!blank(connectorId)) {
        return listByConnectorStateIndexes(
            accountId, pageSize, pageToken, connectorId, orderedStates);
      }
      return listByAccountStateIndexes(accountId, pageSize, pageToken, orderedStates);
    }
    if (!blank(connectorId)) {
      return listByConnectorIndex(accountId, pageSize, pageToken, connectorId, normalizedStates);
    }
    return listAccountWide(accountId, pageSize, pageToken, normalizedStates);
  }

  public StoredJobPage listStoredChildJobs(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (blank(accountId) || blank(parentJobId)) {
      return new StoredJobPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    List<StoredReconcileJob> out = new ArrayList<>();
    String token = pageToken == null ? "" : pageToken;
    var page = jobIndexBackend.listParentEntries(accountId, parentJobId, limit, token);
    List<JobIndexEntrySnapshot> pointers = page.entries();
    for (JobIndexEntrySnapshot ptr : pointers) {
      readCurrentRecordFromIndexPointer(ptr).ifPresent(out::add);
    }
    return new StoredJobPage(out, page.nextPageToken());
  }

  public long countStoredChildJobs(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return 0L;
    }
    return countPages(
        token -> jobIndexBackend.listParentEntries(accountId, parentJobId, 256, token));
  }

  public long countStoredJobsInState(String state) {
    if (blank(state)) {
      return 0L;
    }
    return countPages(token -> jobIndexBackend.listGlobalStateEntries(state, 256, token));
  }

  public long oldestStoredJobTimestampInState(String state) {
    if (blank(state)) {
      return 0L;
    }
    List<JobIndexEntrySnapshot> pointers =
        jobIndexBackend.listGlobalStateEntries(state, 1, "").entries();
    if (pointers.isEmpty()) {
      return 0L;
    }
    return Math.max(0L, parseStatePointerMillis(pointers.get(0).pointerKey(), state));
  }

  // Job-index pointers are derived from canonical job state and are only mutated from
  // canonical job transitions.
  public JobIndexWriteBatch buildJobIndexWriteBatch(
      CanonicalPointerSnapshot currentSnapshot,
      StoredReconcileJob previous,
      StoredReconcileJob current) {
    String canonicalPointerKey = currentSnapshot.canonicalPointerKey();
    List<JobIndexWriteOp> ops = new ArrayList<>();
    ops.add(
        new JobIndexUpsert(
            currentSnapshot.canonicalPointerKey(),
            currentSnapshot.version(),
            payloadStore.encodeInlineJobState(current)));

    String previousLookupKey =
        previous == null || blank(previous.jobId)
            ? ""
            : Keys.reconcileJobLookupPointerById(previous.jobId);
    String currentLookupKey =
        blank(current.jobId) ? "" : Keys.reconcileJobLookupPointerById(current.jobId);
    if (!currentLookupKey.equals(previousLookupKey)) {
      appendReferenceUpsert(ops, currentLookupKey, canonicalPointerKey);
      if (!previousLookupKey.isBlank()) {
        appendOwnedDelete(ops, previousLookupKey, canonicalPointerKey);
      }
    }
    String previousConnectorIndexPointerKey =
        previous == null ? "" : blankToEmpty(previous.connectorIndexPointerKey);
    String currentConnectorIndexPointerKey = blankToEmpty(current.connectorIndexPointerKey);
    if (!currentConnectorIndexPointerKey.equals(previousConnectorIndexPointerKey)
        && !currentConnectorIndexPointerKey.isBlank()) {
      appendReferenceUpsert(ops, currentConnectorIndexPointerKey, canonicalPointerKey);
    }
    if (!previousConnectorIndexPointerKey.isBlank()
        && !previousConnectorIndexPointerKey.equals(currentConnectorIndexPointerKey)) {
      appendOwnedDelete(ops, previousConnectorIndexPointerKey, canonicalPointerKey);
    }

    List<String> previousStatePointerKeys =
        previous == null ? List.of() : indexes.statePointerKeys(previous);
    List<String> currentStatePointerKeys = indexes.statePointerKeys(current);
    if (!currentStatePointerKeys.equals(previousStatePointerKeys)) {
      for (String currentStatePointerKey : currentStatePointerKeys) {
        appendReferenceUpsert(ops, currentStatePointerKey, canonicalPointerKey);
      }
      for (String previousStatePointerKey : previousStatePointerKeys) {
        if (!currentStatePointerKeys.contains(previousStatePointerKey)) {
          appendOwnedDelete(ops, previousStatePointerKey, canonicalPointerKey);
        }
      }
    }

    String previousDedupePointerKey = previous == null ? "" : indexes.dedupePointerKey(previous);
    String currentDedupePointerKey = indexes.dedupePointerKey(current);
    boolean previousTerminal = previous != null && isTerminalState(previous.state);
    boolean currentTerminal = isTerminalState(current.state);
    if (currentTerminal) {
      String dedupePointerToDelete =
          previousDedupePointerKey.isBlank() ? currentDedupePointerKey : previousDedupePointerKey;
      if (!dedupePointerToDelete.isBlank()
          && (!previousTerminal
              || !Objects.equals(previousDedupePointerKey, currentDedupePointerKey))) {
        appendOwnedDelete(ops, dedupePointerToDelete, canonicalPointerKey);
      }
    } else if (!currentDedupePointerKey.isBlank()
        && (previousTerminal
            || !Objects.equals(previousDedupePointerKey, currentDedupePointerKey))) {
      appendReferenceUpsert(ops, currentDedupePointerKey, canonicalPointerKey);
      if (!previousDedupePointerKey.isBlank()
          && !Objects.equals(previousDedupePointerKey, currentDedupePointerKey)) {
        appendOwnedDelete(ops, previousDedupePointerKey, canonicalPointerKey);
      }
    }
    return new JobIndexWriteBatch(
        ops, buildReadyQueueMutation(previous, current, canonicalPointerKey));
  }

  public StoredReconcileJob cloneStoredRecord(StoredReconcileJob source) {
    if (source == null) {
      return null;
    }
    StoredReconcileJob copy = new StoredReconcileJob();
    copy.jobId = source.jobId;
    copy.accountId = source.accountId;
    copy.connectorId = source.connectorId;
    copy.jobKind = source.jobKind;
    copy.parentJobId = source.parentJobId;
    copy.fullRescan = source.fullRescan;
    copy.captureMode = source.captureMode;
    copy.executionClass = source.executionClass;
    copy.executionLane = source.executionLane;
    copy.executionAttributes =
        source.executionAttributes == null
            ? java.util.Map.of()
            : java.util.Map.copyOf(source.executionAttributes);
    copy.pinnedExecutorId = source.pinnedExecutorId;
    copy.executorId = source.executorId;
    copy.snapshotTaskTableId = source.snapshotTaskTableId;
    copy.snapshotTaskSnapshotId = source.snapshotTaskSnapshotId;
    copy.snapshotTaskSourceNamespace = source.snapshotTaskSourceNamespace;
    copy.snapshotTaskSourceTable = source.snapshotTaskSourceTable;
    copy.snapshotTaskFileGroupPlanRecorded = source.snapshotTaskFileGroupPlanRecorded;
    copy.snapshotTaskCompletionMode = source.snapshotTaskCompletionMode;
    copy.snapshotTaskDirectStatsBlobUri = source.snapshotTaskDirectStatsBlobUri;
    copy.snapshotTaskDirectStatsRecordCount = source.snapshotTaskDirectStatsRecordCount;
    copy.fileGroupPlanId = source.fileGroupPlanId;
    copy.fileGroupGroupId = source.fileGroupGroupId;
    copy.fileGroupTableId = source.fileGroupTableId;
    copy.fileGroupSnapshotId = source.fileGroupSnapshotId;
    copy.fileGroupFileCount = source.fileGroupFileCount;
    copy.fileGroupResultBlobUri = source.fileGroupResultBlobUri;
    copy.definition = cloneDefinition(source.definition);
    copy.snapshotPlanBlobUri = source.snapshotPlanBlobUri;
    copy.state = source.state;
    copy.message = source.message;
    copy.startedAtMs = source.startedAtMs;
    copy.finishedAtMs = source.finishedAtMs;
    copy.tablesScanned = source.tablesScanned;
    copy.tablesChanged = source.tablesChanged;
    copy.viewsScanned = source.viewsScanned;
    copy.viewsChanged = source.viewsChanged;
    copy.errors = source.errors;
    copy.snapshotsProcessed = source.snapshotsProcessed;
    copy.statsProcessed = source.statsProcessed;
    copy.indexesProcessed = source.indexesProcessed;
    copy.plannedFileGroups = source.plannedFileGroups;
    copy.plannedFiles = source.plannedFiles;
    copy.completedFileGroups = source.completedFileGroups;
    copy.failedFileGroups = source.failedFileGroups;
    copy.completedFiles = source.completedFiles;
    copy.failedFiles = source.failedFiles;
    copy.expectedDirectChildren = source.expectedDirectChildren;
    copy.childrenFinalized = source.childrenFinalized;
    copy.projectionRequestedGeneration = source.projectionRequestedGeneration;
    copy.projectionAppliedGeneration = source.projectionAppliedGeneration;
    copy.attempt = source.attempt;
    copy.nextAttemptAtMs = source.nextAttemptAtMs;
    copy.lastError = source.lastError;
    copy.laneKey = source.laneKey;
    copy.dedupeKeyHash = source.dedupeKeyHash;
    copy.readyPointerKey = source.readyPointerKey;
    copy.connectorIndexPointerKey = source.connectorIndexPointerKey;
    copy.canonicalPointerKey = source.canonicalPointerKey;
    copy.createdAtMs = source.createdAtMs;
    copy.updatedAtMs = source.updatedAtMs;
    return copy;
  }

  private void appendReferenceUpsert(
      List<JobIndexWriteOp> ops, String pointerKey, String reference) {
    if (blank(pointerKey) || blank(reference)) {
      return;
    }
    JobIndexEntrySnapshot existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing != null && reference.equals(existing.blobUri())) {
      return;
    }
    long expectedVersion = existing == null ? 0L : existing.version();
    ops.add(new JobIndexUpsert(pointerKey, expectedVersion, reference));
  }

  private static StoredJobDefinition cloneDefinition(StoredJobDefinition source) {
    if (source == null) {
      return null;
    }
    StoredJobDefinition copy = new StoredJobDefinition();
    copy.sourceNamespace = source.sourceNamespace;
    copy.sourceTable = source.sourceTable;
    copy.taskMode = source.taskMode;
    copy.taskDestinationNamespaceId = source.taskDestinationNamespaceId;
    copy.taskDestinationTableId = source.taskDestinationTableId;
    copy.taskDestinationTableDisplayName = source.taskDestinationTableDisplayName;
    copy.sourceView = source.sourceView;
    copy.taskDestinationViewId = source.taskDestinationViewId;
    copy.taskDestinationViewDisplayName = source.taskDestinationViewDisplayName;
    copy.destinationTableId = source.destinationTableId;
    copy.destinationViewId = source.destinationViewId;
    copy.destinationNamespaceIds =
        source.destinationNamespaceIds == null
            ? List.of()
            : List.copyOf(source.destinationNamespaceIds);
    copy.destinationCaptureRequests =
        source.destinationCaptureRequests == null
            ? List.of()
            : List.copyOf(source.destinationCaptureRequests);
    copy.capturePolicyColumns =
        source.capturePolicyColumns == null ? List.of() : List.copyOf(source.capturePolicyColumns);
    copy.capturePolicyOutputs =
        source.capturePolicyOutputs == null ? List.of() : List.copyOf(source.capturePolicyOutputs);
    copy.capturePolicyDefaultColumnScope = source.capturePolicyDefaultColumnScope;
    copy.capturePolicyMaxDefaultColumns = source.capturePolicyMaxDefaultColumns;
    copy.snapshotSelectionKind = source.snapshotSelectionKind;
    copy.snapshotSelectionSnapshotIds =
        source.snapshotSelectionSnapshotIds == null
            ? List.of()
            : List.copyOf(source.snapshotSelectionSnapshotIds);
    copy.snapshotSelectionLatestN = source.snapshotSelectionLatestN;
    return copy;
  }

  private void appendOwnedDelete(
      List<JobIndexWriteOp> ops, String pointerKey, String canonicalPointerKey) {
    if (blank(pointerKey) || blank(canonicalPointerKey)) {
      return;
    }
    JobIndexEntrySnapshot existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (canonicalPointerKey.equals(existing.blobUri())) {
      ops.add(new JobIndexDelete(pointerKey, existing.version()));
    }
  }

  private List<String> readyPointerKeysForCleanup(StoredReconcileJob record) {
    if (record == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> readyKeys =
        new java.util.LinkedHashSet<>(readyPointerKeys(record));
    boolean hasStoredReadyPointer = !blank(record.readyPointerKey);
    if (hasStoredReadyPointer) {
      readyKeys.add(record.readyPointerKey);
    }
    boolean shouldReconstructHistoricalReadyKeys =
        hasStoredReadyPointer || !isTerminalState(record.state);
    if (shouldReconstructHistoricalReadyKeys) {
      long dueAtMs =
          record.nextAttemptAtMs > 0L
              ? record.nextAttemptAtMs
              : parseDueMillis(blankToEmpty(record.readyPointerKey));
      if (dueAtMs != INVALID_ORDERED_POINTER_MS && dueAtMs > 0L) {
        ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
        readyKeys.add(readyPointerKeyFor(record, dueAtMs));
        // BY_PRIORITY key must be included in cleanup to prevent stale key accumulation.
        // When a job retries, its dueAtMs changes; the old BY_PRIORITY key (with old dueAtMs)
        // must be deleted so the KV store doesn't accumulate unreachable entries.
        readyKeys.add(
            Keys.reconcileReadyByPriorityPointerByDue(
                record.priorityClass().order, dueAtMs, record.accountId, record.jobId));
        readyKeys.add(
            readyPointerKeyFor(
                record,
                ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
                dueAtMs,
                executionPolicy.executionClass().name()));
        readyKeys.add(
            readyPointerKeyFor(
                record,
                ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE,
                dueAtMs,
                executionPolicy.lane()));
        if (!blank(record.pinnedExecutorId())) {
          readyKeys.add(
              readyPointerKeyFor(
                  record,
                  ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
                  dueAtMs,
                  record.pinnedExecutorId()));
        }
        readyKeys.add(
            readyPointerKeyFor(
                record,
                ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
                dueAtMs,
                record.jobKind().name()));
      }
    }
    readyKeys.removeIf(NativeReconcileJobIndexStore::blank);
    return List.copyOf(readyKeys);
  }

  private ReadyQueueMutation buildReadyQueueMutation(
      StoredReconcileJob previous, StoredReconcileJob current, String canonicalPointerKey) {
    List<String> previousReadyPointerKeys =
        previous == null ? List.of() : readyPointerKeysForCleanup(previous);
    List<String> currentReadyPointerKeys = readyPointerKeys(current);
    if (currentReadyPointerKeys.equals(previousReadyPointerKeys)) {
      return ReadyQueueMutation.empty();
    }
    List<ReadyQueueWrite> upserts =
        currentReadyPointerKeys.stream()
            .map(readyKey -> new ReadyQueueWrite(readyKey, canonicalPointerKey))
            .toList();
    List<String> deletes =
        previousReadyPointerKeys.stream()
            .filter(
                previousReadyPointerKey ->
                    !currentReadyPointerKeys.contains(previousReadyPointerKey))
            .toList();
    return new ReadyQueueMutation(upserts, deletes);
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
  }

  private long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String normalizedKey = normalizePointerKey(pointerKey);
    String normalizedPrefix = normalizePointerKey(prefix);
    if (!normalizedKey.startsWith(normalizedPrefix)) {
      return INVALID_ORDERED_POINTER_MS;
    }
    int slash = normalizedKey.indexOf('/', normalizedPrefix.length());
    if (slash < 0) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String token = normalizedKey.substring(normalizedPrefix.length(), slash);
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException nfe) {
      return INVALID_ORDERED_POINTER_MS;
    }
  }

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean isTerminalState(String state) {
    if (state == null) {
      return false;
    }
    return switch (state) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }

  private JobIndexWriteBatch queuedJobInsertOps(
      QueuedJobInsert insert, JobIndexEntrySnapshot existingDedupePointer) {
    List<JobIndexWriteOp> ops = new ArrayList<>();
    long dedupeExpectedVersion =
        existingDedupePointer == null ? 0L : existingDedupePointer.version();
    ops.add(
        new JobIndexUpsert(
            insert.dedupePointerKey(), dedupeExpectedVersion, insert.canonicalKey()));
    ops.add(
        new JobIndexUpsert(
            insert.canonicalKey(), 0L, payloadStore.encodeInlineJobState(insert.record())));
    ops.add(new JobIndexUpsert(insert.lookupKey(), 0L, insert.canonicalKey()));
    if (!insert.parentKey().isBlank()) {
      ops.add(new JobIndexUpsert(insert.parentKey(), 0L, insert.canonicalKey()));
    }
    if (!insert.connectorIndexKey().isBlank()) {
      ops.add(new JobIndexUpsert(insert.connectorIndexKey(), 0L, insert.canonicalKey()));
    }
    for (String stateKey : insert.stateKeys()) {
      ops.add(new JobIndexUpsert(stateKey, 0L, insert.canonicalKey()));
    }
    return new JobIndexWriteBatch(
        ops,
        new ReadyQueueMutation(
            insert.readyKeys().stream()
                .map(readyKey -> new ReadyQueueWrite(readyKey, insert.canonicalKey()))
                .toList(),
            List.of()));
  }

  private List<JobIndexWriteBatch> prependInsertBatch(
      JobIndexWriteBatch insertBatch, List<CanonicalRecordMutation> ancestorMutations) {
    List<JobIndexWriteBatch> batches =
        new ArrayList<>(1 + (ancestorMutations == null ? 0 : ancestorMutations.size()));
    batches.add(insertBatch);
    if (ancestorMutations != null) {
      for (CanonicalRecordMutation mutation : ancestorMutations) {
        if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
          continue;
        }
        batches.add(
            buildJobIndexWriteBatch(mutation.snapshot(), mutation.previous(), mutation.current()));
      }
    }
    return batches;
  }

  private List<JobIndexWriteBatch> prependInsertBatches(
      List<JobIndexWriteBatch> insertBatches, List<CanonicalRecordMutation> ancestorMutations) {
    List<JobIndexWriteBatch> batches =
        new ArrayList<>(
            (insertBatches == null ? 0 : insertBatches.size())
                + (ancestorMutations == null ? 0 : ancestorMutations.size()));
    if (insertBatches != null) {
      for (JobIndexWriteBatch insertBatch : insertBatches) {
        if (insertBatch != null) {
          batches.add(insertBatch);
        }
      }
    }
    if (ancestorMutations != null) {
      for (CanonicalRecordMutation mutation : ancestorMutations) {
        if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
          continue;
        }
        batches.add(
            buildJobIndexWriteBatch(mutation.snapshot(), mutation.previous(), mutation.current()));
      }
    }
    return batches;
  }

  private StoredJobPage listAccountWide(
      String accountId, int pageSize, String pageToken, Set<String> states) {
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<StoredReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    while (out.size() < limit) {
      ReconcileJobIndexBackend.JobIndexQueryPage page =
          jobIndexBackend.listCanonicalEntries(accountId, Math.max(limit * 2, 64), token);
      List<JobIndexEntrySnapshot> pointers = page.entries();
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        JobIndexEntrySnapshot ptr = pointers.get(i);
        if (!isCanonicalJobPointerKey(accountId, ptr.pointerKey())) {
          continue;
        }
        var rec =
            readRecord(
                new CanonicalPointerSnapshot(ptr.pointerKey(), ptr.blobUri(), ptr.version()));
        if (rec.isEmpty()) {
          continue;
        }
        StoredReconcileJob stored = rec.get();
        if (states != null && !states.isEmpty() && !states.contains(stored.state)) {
          continue;
        }
        out.add(stored);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || !page.nextPageToken().isBlank();
          nextToken =
              !hasMore
                  ? ""
                  : (i + 1 < pointers.size()
                      ? encodeListCursor(token, i + 1)
                      : page.nextPageToken());
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = page.nextPageToken();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Account-wide reconcile job list hit page cap accountId=%s out=%d",
            accountId, out.size());
        break;
      }
      token = nextToken;
    }
    return new StoredJobPage(out, nextToken);
  }

  private StoredJobPage listByConnectorIndex(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<StoredReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    while (out.size() < limit) {
      var page =
          jobIndexBackend.listConnectorEntries(
              accountId, connectorId, Math.max(limit * 2, 64), token);
      List<JobIndexEntrySnapshot> pointers = page.entries();
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        JobIndexEntrySnapshot ptr = pointers.get(i);
        var rec = readCurrentRecordFromIndexPointer(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        StoredReconcileJob stored = rec.get();
        if (!connectorId.equals(stored.connectorId)) {
          continue;
        }
        if (states != null && !states.isEmpty() && !states.contains(stored.state)) {
          continue;
        }
        out.add(stored);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || !page.nextPageToken().isBlank();
          nextToken =
              !hasMore
                  ? ""
                  : (i + 1 < pointers.size()
                      ? encodeListCursor(token, i + 1)
                      : page.nextPageToken());
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = page.nextPageToken();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Connector reconcile job list hit page cap accountId=%s connectorId=%s out=%d",
            accountId, connectorId, out.size());
        break;
      }
      token = nextToken;
    }
    return new StoredJobPage(out, nextToken);
  }

  private StoredJobPage listByAccountStateIndexes(
      String accountId, int pageSize, String pageToken, List<String> states) {
    if (blank(accountId) || states == null || states.isEmpty()) {
      return new StoredJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByStatePage(
                jobIndexBackend.listAccountStateEntries(
                    accountId, request.state(), request.pageSize(), request.pageToken()),
                stored ->
                    accountId.equals(stored.accountId) && request.state().equals(stored.state)));
  }

  private StoredJobPage listByConnectorStateIndexes(
      String accountId, int pageSize, String pageToken, String connectorId, List<String> states) {
    if (blank(accountId) || blank(connectorId) || states == null || states.isEmpty()) {
      return new StoredJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByStatePage(
                jobIndexBackend.listConnectorStateEntries(
                    accountId,
                    connectorId,
                    request.state(),
                    request.pageSize(),
                    request.pageToken()),
                stored ->
                    accountId.equals(stored.accountId)
                        && connectorId.equals(stored.connectorId)
                        && request.state().equals(stored.state)));
  }

  private StoredJobPage listByStateIndexes(
      int pageSize,
      String pageToken,
      List<String> states,
      Function<StateListRequest, StoredJobPage> fetchPage) {
    int limit = Math.max(1, pageSize);
    StateListCursor cursor = decodeStateListCursor(pageToken);
    if (states == null || states.isEmpty() || cursor.stateIndex() >= states.size()) {
      return new StoredJobPage(List.of(), "");
    }

    List<StoredReconcileJob> out = new ArrayList<>(limit);
    int stateIndex = Math.max(0, cursor.stateIndex());
    String nestedPageToken = cursor.pageToken();

    while (stateIndex < states.size() && out.size() < limit) {
      StoredJobPage page =
          fetchPage.apply(
              new StateListRequest(
                  states.get(stateIndex), Math.max(1, limit - out.size()), nestedPageToken));
      if (page != null && page.records() != null && !page.records().isEmpty()) {
        out.addAll(page.records());
      }
      String nextNestedToken = page == null ? "" : page.nextPageToken();
      boolean currentStateExhausted = blank(nextNestedToken);

      if (out.size() >= limit) {
        if (!currentStateExhausted) {
          return new StoredJobPage(out, encodeStateListCursor(stateIndex, nextNestedToken));
        }
        if (stateIndex + 1 < states.size()) {
          return new StoredJobPage(out, encodeStateListCursor(stateIndex + 1, ""));
        }
        return new StoredJobPage(out, "");
      }

      if (!currentStateExhausted) {
        nestedPageToken = nextNestedToken;
        continue;
      }
      stateIndex++;
      nestedPageToken = "";
    }
    return new StoredJobPage(out, "");
  }

  private StoredJobPage listByStatePage(
      ReconcileJobIndexBackend.JobIndexQueryPage initialPage,
      Predicate<StoredReconcileJob> filter) {
    List<StoredReconcileJob> out = new ArrayList<>(initialPage.entries().size());
    for (JobIndexEntrySnapshot ptr : initialPage.entries()) {
      var rec = readCurrentRecordFromStateIndexPointer(ptr, filter);
      rec.ifPresent(out::add);
    }
    return new StoredJobPage(out, initialPage.nextPageToken());
  }

  private boolean isCanonicalJobPointerKey(String accountId, String key) {
    return key != null
        && key.equals(Keys.reconcileJobPointerById(accountId, jobIdFromCanonicalKey(key)));
  }

  private long countPages(
      java.util.function.Function<String, ReconcileJobIndexBackend.JobIndexQueryPage> fetchPage) {
    long count = 0L;
    String token = "";
    int pages = 0;
    while (true) {
      ReconcileJobIndexBackend.JobIndexQueryPage page = fetchPage.apply(token);
      List<JobIndexEntrySnapshot> pointers = page.entries();
      if (pointers.isEmpty()) {
        return count;
      }
      count += pointers.size();
      token = page.nextPageToken();
      if (token.isBlank()) {
        return count;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf("Reconcile job index count hit page cap count=%d", count);
        return count;
      }
    }
  }

  private long parseStatePointerMillis(String statePointerKey, String state) {
    return parseTimestampFromOrderedPointer(
        statePointerKey, Keys.reconcileJobByStatePointerPrefix(state));
  }

  private String jobIdFromCanonicalKey(String key) {
    int slash = key == null ? -1 : key.lastIndexOf('/');
    return slash < 0 ? "" : key.substring(slash + 1);
  }

  private static ListCursor decodeListCursor(String pageToken) {
    if (blank(pageToken)) {
      return new ListCursor("", 0);
    }
    if (!pageToken.startsWith(LIST_TOKEN_V1_PREFIX)) {
      return new ListCursor(pageToken, 0);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(pageToken.substring(LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new ListCursor(decoded, 0);
      }
      return new ListCursor(
          decoded.substring(0, separator),
          Math.max(0, Integer.parseInt(decoded.substring(separator + 1))));
    } catch (RuntimeException e) {
      return new ListCursor(pageToken, 0);
    }
  }

  private static String encodeListCursor(String storeToken, int skip) {
    if (skip <= 0) {
      return storeToken == null ? "" : storeToken;
    }
    String payload = (storeToken == null ? "" : storeToken) + "\n" + skip;
    return LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static StateListCursor decodeStateListCursor(String pageToken) {
    if (blank(pageToken)) {
      return new StateListCursor(0, "");
    }
    if (!pageToken.startsWith(STATE_LIST_TOKEN_V1_PREFIX)) {
      return new StateListCursor(0, pageToken);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder()
                  .decode(pageToken.substring(STATE_LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new StateListCursor(Math.max(0, Integer.parseInt(decoded)), "");
      }
      return new StateListCursor(
          Math.max(0, Integer.parseInt(decoded.substring(0, separator))),
          decoded.substring(separator + 1));
    } catch (RuntimeException e) {
      return new StateListCursor(0, "");
    }
  }

  private Optional<StoredReconcileJob> readCurrentRecordFromIndexPointer(
      JobIndexEntrySnapshot indexPointer) {
    if (indexPointer == null || blank(indexPointer.blobUri())) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(indexPointer.blobUri()).orElse(null);
    if (canonicalPointer == null) {
      return Optional.empty();
    }
    return readRecord(
        new CanonicalPointerSnapshot(
            canonicalPointer.pointerKey(), canonicalPointer.blobUri(), canonicalPointer.version()));
  }

  private Optional<StoredReconcileJob> readCurrentRecordFromStateIndexPointer(
      JobIndexEntrySnapshot indexPointer, Predicate<StoredReconcileJob> filter) {
    var current = readCurrentRecordFromIndexPointer(indexPointer);
    if (current.isEmpty()) {
      return Optional.empty();
    }
    if (filter == null || filter.test(current.get())) {
      return current;
    }
    return Optional.empty();
  }

  private long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  private String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  private String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  private String readyPointerKeyFor(
      StoredReconcileJob record,
      ReconcileReadyQueueStore.ReadyIndexType indexType,
      long dueAtMs,
      String filterValue) {
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

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !requiresReadyPointer(record)) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
    // BY_PRIORITY index: mirrors NativeReconcileReadyQueueStore.readyPointerKeys().
    // Must be kept in sync so that the mutation path (re-enqueue on retry, cleanup) produces
    // the same key set as the initial enqueue path. Within-class sort order is by dueAtMs
    // (earliest-due-first) — priorityScore ordering is only enforced in the in-memory store.
    String priorityReadyKey =
        Keys.reconcileReadyByPriorityPointerByDue(
            record.priorityClass().order, dueAtMs, record.accountId, record.jobId);
    if (!priorityReadyKey.isBlank()) {
      readyKeys.add(priorityReadyKey);
    }
    String executionClassReadyKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            executionPolicy.executionClass().name());
    if (!executionClassReadyKey.isBlank()) {
      readyKeys.add(executionClassReadyKey);
    }
    String executionLaneReadyKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE,
            dueAtMs,
            executionPolicy.lane());
    if (!executionLaneReadyKey.isBlank()) {
      readyKeys.add(executionLaneReadyKey);
    }
    if (!blank(record.pinnedExecutorId())) {
      String pinnedReadyKey =
          readyPointerKeyFor(
              record,
              ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
              dueAtMs,
              record.pinnedExecutorId());
      if (!pinnedReadyKey.isBlank()) {
        readyKeys.add(pinnedReadyKey);
      }
    }
    String kindReadyKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
            dueAtMs,
            record.jobKind().name());
    if (!kindReadyKey.isBlank()) {
      readyKeys.add(kindReadyKey);
    }
    return readyKeys;
  }

  private static boolean requiresReadyPointer(StoredReconcileJob record) {
    return record != null && "JS_QUEUED".equals(record.state);
  }

  private static String encodeStateListCursor(int stateIndex, String nestedPageToken) {
    if (stateIndex < 0) {
      return "";
    }
    String payload = stateIndex + "\n" + (nestedPageToken == null ? "" : nestedPageToken);
    return STATE_LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static Set<String> normalizeStateFilter(Set<String> states) {
    if (states == null || states.isEmpty()) {
      return Set.of();
    }
    return states.stream()
        .filter(state -> state != null && !state.isBlank())
        .map(String::trim)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static List<String> orderedStateFilter(Set<String> states) {
    return states == null || states.isEmpty() ? List.of() : states.stream().sorted().toList();
  }

  private record ListCursor(String storeToken, int skip) {}

  private record StateListCursor(int stateIndex, String pageToken) {}

  private record StateListRequest(String state, int pageSize, String pageToken) {}
}
