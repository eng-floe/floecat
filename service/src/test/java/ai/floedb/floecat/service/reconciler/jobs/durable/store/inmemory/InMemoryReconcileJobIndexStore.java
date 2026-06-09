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

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

/** Test-scope in-memory job index store with explicit canonical job state. */
public final class InMemoryReconcileJobIndexStore implements ReconcileJobIndexStore {
  private static final Logger LOG = Logger.getLogger(InMemoryReconcileJobIndexStore.class);
  private static final long INVALID_ORDERED_POINTER_MS = -1L;
  private static final int LIST_SCAN_MAX_PAGES = 1_000;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";
  private static final String STATE_LIST_TOKEN_V1_PREFIX = "v1s:";

  private final InMemoryReconcileJobIndexState state = new InMemoryReconcileJobIndexState();

  private ReconcileJobIndexBackend jobIndexBackend;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobIndexes indexes;
  private int casMax;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;
  private TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition;

  @Override
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

  @Override
  public Optional<CanonicalEnvelope> loadByAnyAccount(String jobId) {
    if (blank(jobId)) {
      return Optional.empty();
    }
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    JobIndexEntrySnapshot lookup = jobIndexBackend.loadIndexEntry(lookupKey).orElse(null);
    if (lookup == null || blank(lookup.blobUri())) {
      synchronized (state) {
        String canonicalPointerKey = state.canonicalKeyByJobId.get(jobId);
        StoredReconcileJob cached =
            canonicalPointerKey == null ? null : state.canonicalByKey.get(canonicalPointerKey);
        return cached == null
            ? Optional.empty()
            : Optional.of(new CanonicalEnvelope(canonicalPointerKey, cloneStoredRecord(cached)));
      }
    }
    String canonicalPointerKey = lookup.blobUri();
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      synchronized (state) {
        state.remove(canonicalPointerKey, jobId);
      }
      return Optional.empty();
    }
    var canonicalRec =
        readRecord(
            new CanonicalPointerSnapshot(
                canonicalPointer.pointerKey(),
                canonicalPointer.blobUri(),
                canonicalPointer.version()));
    if (canonicalRec.isEmpty()) {
      synchronized (state) {
        state.remove(canonicalPointerKey, jobId);
      }
      return Optional.empty();
    }
    StoredReconcileJob current = canonicalRec.get();
    if (!jobId.equals(current.jobId)) {
      LOG.warnf(
          "Reconcile lookup pointer mismatch jobId=%s canonicalKey=%s canonicalJobId=%s",
          jobId, canonicalPointerKey, current.jobId);
      return Optional.empty();
    }
    synchronized (state) {
      state.put(canonicalPointerKey, cloneStoredRecord(current));
    }
    return Optional.of(new CanonicalEnvelope(canonicalPointerKey, current));
  }

  @Override
  public Optional<CanonicalEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return mutateByCanonicalPointerReturningRecord(loaded.get().canonicalPointerKey(), mutator);
  }

  @Override
  public Optional<CanonicalEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < casMax; i++) {
      JobIndexEntrySnapshot currentPointer =
          jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        synchronized (state) {
          StoredReconcileJob removed = state.canonicalByKey.remove(canonicalPointerKey);
          if (removed != null) {
            state.canonicalKeyByJobId.remove(removed.jobId);
          }
        }
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
        synchronized (state) {
          state.put(canonicalPointerKey, cloneStoredRecord(nextRecord));
        }
        logStateTransition.accept(baseline, nextRecord, "mutate");
        return Optional.of(
            new CanonicalEnvelope(canonicalPointerKey, cloneStoredRecord(nextRecord)));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      synchronized (state) {
        StoredReconcileJob removed = state.canonicalByKey.remove(canonicalPointerKey);
        if (removed != null) {
          state.canonicalKeyByJobId.remove(removed.jobId);
        }
      }
      return Optional.empty();
    }
    return Optional.of(
        new CanonicalPointerSnapshot(
            canonicalPointer.pointerKey(), canonicalPointer.blobUri(), canonicalPointer.version()));
  }

  @Override
  public Optional<StoredReconcileJob> readCanonicalRecordByKey(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    var record = loadCanonicalSnapshot(canonicalPointerKey).flatMap(this::readRecord);
    record.ifPresent(
        stored -> {
          synchronized (state) {
            state.put(canonicalPointerKey, cloneStoredRecord(stored));
          }
        });
    return record;
  }

  @Override
  public Optional<StoredReconcileJob> readRecord(CanonicalPointerSnapshot canonicalPointer) {
    var record = payloadStore.readInlineJobState(canonicalPointer.blobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.canonicalPointerKey();
    return record;
  }

  @Override
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
    try {
      return readRecord(
          new CanonicalPointerSnapshot(
              canonicalPointer.pointerKey(),
              canonicalPointer.blobUri(),
              canonicalPointer.version()));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  @Override
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
        synchronized (state) {
          state.put(insert.canonicalKey(), cloneStoredRecord(insert.record()));
        }
        if (ancestorMutations != null) {
          for (CanonicalRecordMutation mutation : ancestorMutations) {
            if (mutation == null || mutation.current() == null) {
              continue;
            }
            state.put(
                mutation.snapshot().canonicalPointerKey(), cloneStoredRecord(mutation.current()));
          }
        }
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
        synchronized (state) {
          for (QueuedJobInsert insert : nextPending) {
            state.put(insert.canonicalKey(), cloneStoredRecord(insert.record()));
          }
          for (CanonicalRecordMutation mutation : ancestorMutations) {
            if (mutation == null || mutation.current() == null) {
              continue;
            }
            state.put(
                mutation.snapshot().canonicalPointerKey(), cloneStoredRecord(mutation.current()));
          }
        }
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
    boolean applied = jobIndexBackend.compareAndSetBatch(combineWriteBatches(batches));
    if (applied) {
      synchronized (state) {
        for (CanonicalRecordMutation mutation : mutations) {
          if (mutation == null || mutation.current() == null) {
            continue;
          }
          state.put(
              mutation.snapshot().canonicalPointerKey(), cloneStoredRecord(mutation.current()));
        }
      }
    }
    return applied;
  }

  @Override
  public StoredJobPage listStoredJobs(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    return listAccountWide(
        accountId, pageSize, pageToken, connectorId, normalizeStateFilter(states));
  }

  @Override
  public StoredJobPage listStoredChildJobs(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (blank(accountId) || blank(parentJobId)) {
      return new StoredJobPage(List.of(), "");
    }
    var page =
        jobIndexBackend.listParentEntries(
            accountId, parentJobId, Math.max(1, pageSize), pageToken == null ? "" : pageToken);
    List<StoredReconcileJob> out = new ArrayList<>(page.entries().size());
    for (JobIndexEntrySnapshot ptr : page.entries()) {
      readCurrentRecordFromIndexPointer(ptr).ifPresent(out::add);
    }
    return new StoredJobPage(out, page.nextPageToken());
  }

  @Override
  public long countStoredChildJobs(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return 0L;
    }
    return countPages(
        token -> jobIndexBackend.listParentEntries(accountId, parentJobId, 256, token));
  }

  @Override
  public long countStoredJobsInState(String state) {
    return 0L;
  }

  @Override
  public long oldestStoredJobTimestampInState(String state) {
    return 0L;
  }

  @Override
  public JobIndexWriteBatch buildJobIndexWriteBatch(
      CanonicalPointerSnapshot currentSnapshot,
      StoredReconcileJob previous,
      StoredReconcileJob current) {
    String canonicalPointerKey = currentSnapshot.canonicalPointerKey();
    List<JobIndexWriteOp> ops = new ArrayList<>();
    ops.add(
        new JobIndexUpsert(
            canonicalPointerKey,
            currentSnapshot.version(),
            payloadStore.encodeInlineJobState(current),
            PointerReferenceKind.PRK_INLINE_JSON));

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
    List<String> previousReadyPointerKeys =
        previous == null ? List.of() : readyPointerKeysForCleanup(previous);
    List<String> currentReadyPointerKeys = readyPointerKeys(current);
    if (!currentReadyPointerKeys.equals(previousReadyPointerKeys)) {
      for (String currentReadyPointerKey : currentReadyPointerKeys) {
        appendReferenceUpsert(ops, currentReadyPointerKey, canonicalPointerKey);
      }
      for (String previousReadyPointerKey : previousReadyPointerKeys) {
        if (!currentReadyPointerKeys.contains(previousReadyPointerKey)) {
          appendOwnedDelete(ops, previousReadyPointerKey, canonicalPointerKey);
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

  @Override
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

  private Optional<StoredReconcileJob> readCurrentRecordFromIndexPointer(
      JobIndexEntrySnapshot indexPointer) {
    if (indexPointer == null || blank(indexPointer.blobUri())) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot canonicalPointer =
        jobIndexBackend.loadIndexEntry(indexPointer.blobUri()).orElse(null);
    return canonicalPointer == null
        ? Optional.empty()
        : readRecord(
            new CanonicalPointerSnapshot(
                canonicalPointer.pointerKey(),
                canonicalPointer.blobUri(),
                canonicalPointer.version()));
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

  private JobIndexWriteBatch queuedJobInsertOps(
      QueuedJobInsert insert, JobIndexEntrySnapshot existingDedupePointer) {
    List<JobIndexWriteOp> ops = new ArrayList<>();
    long dedupeExpectedVersion =
        existingDedupePointer == null ? 0L : existingDedupePointer.version();
    ops.add(
        new JobIndexUpsert(
            insert.dedupePointerKey(),
            dedupeExpectedVersion,
            insert.canonicalKey(),
            PointerReferenceKind.PRK_POINTER_KEY));
    ops.add(
        new JobIndexUpsert(
            insert.canonicalKey(),
            0L,
            payloadStore.encodeInlineJobState(insert.record()),
            PointerReferenceKind.PRK_INLINE_JSON));
    ops.add(
        new JobIndexUpsert(
            insert.lookupKey(), 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
    if (!insert.parentKey().isBlank()) {
      ops.add(
          new JobIndexUpsert(
              insert.parentKey(), 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
    }
    return new JobIndexWriteBatch(
        ops,
        new ReadyQueueMutation(
            insert.readyKeys().stream()
                .map(
                    readyKey ->
                        new ReadyQueueWrite(
                            readyKey, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY))
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
    ops.add(
        new JobIndexUpsert(
            pointerKey, expectedVersion, reference, PointerReferenceKind.PRK_POINTER_KEY));
  }

  private void appendOwnedDelete(
      List<JobIndexWriteOp> ops, String pointerKey, String canonicalPointerKey) {
    if (blank(pointerKey) || blank(canonicalPointerKey)) {
      return;
    }
    JobIndexEntrySnapshot existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing != null && canonicalPointerKey.equals(existing.blobUri())) {
      ops.add(new JobIndexDelete(pointerKey, existing.version()));
    }
  }

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !requiresReadyPointer(record)) {
      return List.of();
    }
    return readyPointerKeys(record, readyPointerDueAt(record));
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
              : parseTimestampFromOrderedPointer(
                  blankToEmpty(record.readyPointerKey), Keys.reconcileReadyPointerPrefix());
      if (dueAtMs != INVALID_ORDERED_POINTER_MS && dueAtMs > 0L) {
        readyKeys.addAll(readyPointerKeys(record, dueAtMs));
      }
    }
    readyKeys.removeIf(InMemoryReconcileJobIndexStore::blank);
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
            .map(
                readyKey ->
                    new ReadyQueueWrite(
                        readyKey, canonicalPointerKey, PointerReferenceKind.PRK_POINTER_KEY))
            .toList();
    List<String> deletes =
        previousReadyPointerKeys.stream()
            .filter(
                previousReadyPointerKey ->
                    !currentReadyPointerKeys.contains(previousReadyPointerKey))
            .toList();
    return new ReadyQueueMutation(upserts, deletes);
  }

  private String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, record.accountId, record.laneKey, record.jobId);
  }

  private String readyPointerKeyFor(
      StoredReconcileJob record,
      ReconcileReadyQueueStore.ReadyIndexType indexType,
      long dueAtMs,
      String filterValue) {
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

  private List<String> readyPointerKeys(StoredReconcileJob record, long dueAtMs) {
    if (record == null || !requiresReadyPointer(record) || dueAtMs <= 0L) {
      return List.of();
    }
    List<String> keys = new ArrayList<>();
    keys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            record.executionPolicy().executionClass().name());
    if (!executionClassKey.isBlank()) {
      keys.add(executionClassKey);
    }
    String executionLaneKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE,
            dueAtMs,
            record.executionPolicy().lane());
    if (!executionLaneKey.isBlank()) {
      keys.add(executionLaneKey);
    }
    String pinnedExecutorKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
            dueAtMs,
            record.pinnedExecutorId());
    if (!pinnedExecutorKey.isBlank()) {
      keys.add(pinnedExecutorKey);
    }
    String jobKindKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
            dueAtMs,
            record.jobKind().name());
    if (!jobKindKey.isBlank()) {
      keys.add(jobKindKey);
    }
    return List.copyOf(keys);
  }

  private long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  private boolean requiresReadyPointer(StoredReconcileJob record) {
    return record != null && "JS_QUEUED".equals(record.state);
  }

  private StoredJobPage listAccountWide(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
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
        if (!blank(connectorId) && !connectorId.equals(stored.connectorId)) {
          continue;
        }
        if (states != null && !states.isEmpty() && !states.contains(stored.state)) {
          continue;
        }
        out.add(stored);
        synchronized (state) {
          state.put(ptr.pointerKey(), cloneStoredRecord(stored));
        }
        if (out.size() >= limit) {
          boolean hasMore =
              i + 1 < pointers.size() || !blankToEmpty(page.nextPageToken()).isBlank();
          nextToken =
              !hasMore
                  ? ""
                  : (i + 1 < pointers.size()
                      ? encodeListCursor(token, i + 1)
                      : blankToEmpty(page.nextPageToken()));
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

  private StoredJobPage listByStatePage(
      ReconcileJobIndexBackend.JobIndexQueryPage initialPage,
      java.util.function.Predicate<StoredReconcileJob> filter) {
    List<StoredReconcileJob> out = new ArrayList<>(initialPage.entries().size());
    for (JobIndexEntrySnapshot ptr : initialPage.entries()) {
      var rec = readCurrentRecordFromIndexPointer(ptr);
      if (rec.isPresent() && (filter == null || filter.test(rec.get()))) {
        out.add(rec.get());
      }
    }
    return new StoredJobPage(out, initialPage.nextPageToken());
  }

  private static Set<String> normalizeStateFilter(Set<String> states) {
    return states == null
        ? Set.of()
        : states.stream()
            .filter(state -> state != null && !state.isBlank())
            .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
  }

  private static List<String> orderedStateFilter(Set<String> states) {
    return states.stream().sorted().toList();
  }

  private static boolean isCanonicalJobPointerKey(String accountId, String key) {
    return key != null
        && key.startsWith(Keys.reconcileJobPointerByIdPrefix(accountId))
        && !key.contains("/lookup/")
        && !key.contains("/by-state/")
        && !key.contains("/by-connector/")
        && !key.contains("/by-parent/");
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

  private record ListCursor(String storeToken, int skip) {}

  private record StateListCursor(int stateIndex, String storeToken) {}

  private record StateListRequest(String state, int pageSize, String pageToken) {}

  private static ListCursor decodeListCursor(String token) {
    if (token == null || token.isBlank() || !token.startsWith(LIST_TOKEN_V1_PREFIX)) {
      return new ListCursor(token == null ? "" : token, 0);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(token.substring(LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int split = decoded.lastIndexOf('|');
      if (split < 0) {
        return new ListCursor(token, 0);
      }
      return new ListCursor(
          decoded.substring(0, split), Integer.parseInt(decoded.substring(split + 1)));
    } catch (RuntimeException e) {
      return new ListCursor("", 0);
    }
  }

  private static String encodeListCursor(String storeToken, int skip) {
    String raw = blankToEmpty(storeToken) + "|" + Math.max(0, skip);
    return LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  private static StateListCursor decodeStateListCursor(String token, List<String> orderedStates) {
    if (token == null || token.isBlank() || !token.startsWith(STATE_LIST_TOKEN_V1_PREFIX)) {
      return new StateListCursor(0, token == null ? "" : token);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(token.substring(STATE_LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int split = decoded.indexOf('|');
      if (split < 0) {
        return new StateListCursor(0, "");
      }
      int stateIndex = Integer.parseInt(decoded.substring(0, split));
      return new StateListCursor(
          Math.max(0, Math.min(stateIndex, orderedStates.size())), decoded.substring(split + 1));
    } catch (RuntimeException e) {
      return new StateListCursor(0, "");
    }
  }

  private static String encodeStateListCursor(int stateIndex, String storeToken) {
    String raw = Math.max(0, stateIndex) + "|" + blankToEmpty(storeToken);
    return STATE_LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
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
    return switch (blankToEmpty(state)) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }
}
