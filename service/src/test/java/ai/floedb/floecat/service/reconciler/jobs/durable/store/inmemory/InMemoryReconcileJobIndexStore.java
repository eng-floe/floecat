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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileProjectionBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.StoredPointerSnapshot;
import ai.floedb.floecat.service.repo.model.Keys;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

/**
 * Test-scope in-memory job index store with explicit canonical job state. Pointer rows are mirrored
 * for compatibility with durable tests that inspect storage keys directly.
 */
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
    StoredPointerSnapshot lookup = jobIndexBackend.loadStoredPointer(lookupKey).orElse(null);
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
    StoredPointerSnapshot canonicalPointer =
        jobIndexBackend.loadStoredPointer(canonicalPointerKey).orElse(null);
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
      StoredPointerSnapshot currentPointer =
          jobIndexBackend.loadStoredPointer(canonicalPointerKey).orElse(null);
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
    StoredPointerSnapshot canonicalPointer =
        jobIndexBackend.loadStoredPointer(canonicalPointerKey).orElse(null);
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
  public Optional<StoredPointerSnapshot> loadStoredPointer(String pointerKey) {
    if (blank(pointerKey)) {
      return Optional.empty();
    }
    return jobIndexBackend.loadStoredPointer(pointerKey);
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
    StoredPointerSnapshot dedupePointer =
        jobIndexBackend.loadStoredPointer(dedupePointerKey).orElse(null);
    if (dedupePointer == null || blank(dedupePointer.blobUri())) {
      return Optional.empty();
    }
    StoredPointerSnapshot canonicalPointer =
        jobIndexBackend.loadStoredPointer(dedupePointer.blobUri()).orElse(null);
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
    for (int attempt = 0; attempt < casMax; attempt++) {
      StoredPointerSnapshot existingDedupePointer =
          jobIndexBackend.loadStoredPointer(insert.dedupePointerKey()).orElse(null);
      var existing = loadActiveFromDedupe(insert.dedupePointerKey());
      if (existing.isPresent()) {
        return new BulkEnqueueItemResult(insert.index(), existing.get().jobId, false, "");
      }
      if (jobIndexBackend.compareAndSetBatch(queuedJobInsertOps(insert, existingDedupePointer))) {
        synchronized (state) {
          state.put(insert.canonicalKey(), cloneStoredRecord(insert.record()));
        }
        return null;
      }
    }
    return new BulkEnqueueItemResult(
        insert.index(), "", false, "Unable to enqueue reconcile job after CAS retries");
  }

  @Override
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

  @Override
  public StoredJobPage listStoredChildJobs(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (blank(accountId) || blank(parentJobId)) {
      return new StoredJobPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    List<StoredReconcileJob> out = new ArrayList<>();
    String token = pageToken == null ? "" : pageToken;
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    StringBuilder next = new StringBuilder();
    List<StoredPointerSnapshot> pointers =
        jobIndexBackend.listStoredPointersByPrefix(prefix, limit, token, next);
    for (StoredPointerSnapshot ptr : pointers) {
      readCurrentRecordFromIndexPointer(ptr).ifPresent(out::add);
    }
    return new StoredJobPage(out, next.toString());
  }

  @Override
  public long countStoredChildJobs(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return 0L;
    }
    return countPointers(Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId));
  }

  @Override
  public long countStoredJobsInState(String state) {
    if (blank(state)) {
      return 0L;
    }
    return countPointers(Keys.reconcileJobByStatePointerPrefix(state));
  }

  @Override
  public long oldestStoredJobTimestampInState(String state) {
    if (blank(state)) {
      return 0L;
    }
    String prefix = Keys.reconcileJobByStatePointerPrefix(state);
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<StoredPointerSnapshot> pointers =
          jobIndexBackend.listStoredPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        return 0L;
      }
      long oldest = Long.MAX_VALUE;
      for (StoredPointerSnapshot pointer : pointers) {
        long candidate = parseTimestampFromOrderedPointer(pointer.pointerKey(), prefix);
        if (candidate > 0L && candidate < oldest) {
          oldest = candidate;
        }
      }
      if (oldest != Long.MAX_VALUE) {
        return oldest;
      }
      token = next.toString();
      if (token.isBlank()) {
        return 0L;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf("Reconcile job index oldest-state scan hit page cap prefix=%s", prefix);
        return 0L;
      }
    }
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
        ops,
        buildReadyQueueMutation(previous, current, canonicalPointerKey),
        new ReconcileProjectionBackend.ProjectionWriteBatch(List.of()));
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
    copy.definitionBlobUri = source.definitionBlobUri;
    copy.snapshotPlanBlobUri = source.snapshotPlanBlobUri;
    copy.fileGroupPlanBlobUri = source.fileGroupPlanBlobUri;
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
    copy.expectedChildJobs = source.expectedChildJobs;
    copy.completedChildJobs = source.completedChildJobs;
    copy.failedChildJobs = source.failedChildJobs;
    copy.cancelledChildJobs = source.cancelledChildJobs;
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
      StoredPointerSnapshot indexPointer) {
    if (indexPointer == null || blank(indexPointer.blobUri())) {
      return Optional.empty();
    }
    StoredPointerSnapshot canonicalPointer =
        jobIndexBackend.loadStoredPointer(indexPointer.blobUri()).orElse(null);
    return canonicalPointer == null
        ? Optional.empty()
        : readRecord(
            new CanonicalPointerSnapshot(
                canonicalPointer.pointerKey(),
                canonicalPointer.blobUri(),
                canonicalPointer.version()));
  }

  private JobIndexWriteBatch queuedJobInsertOps(
      QueuedJobInsert insert, StoredPointerSnapshot existingDedupePointer) {
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
            List.of()),
        blank(insert.resultBlobUri())
            ? new ReconcileProjectionBackend.ProjectionWriteBatch(List.of())
            : new ReconcileProjectionBackend.ProjectionWriteBatch(
                List.of(
                    new ReconcileProjectionBackend.ResultReferenceUpsert(
                        insert.record().accountId,
                        insert.record().jobId,
                        0L,
                        insert.resultBlobUri()))));
  }

  private void appendReferenceUpsert(
      List<JobIndexWriteOp> ops, String pointerKey, String reference) {
    if (blank(pointerKey) || blank(reference)) {
      return;
    }
    StoredPointerSnapshot existing = jobIndexBackend.loadStoredPointer(pointerKey).orElse(null);
    if (existing != null && reference.equals(existing.blobUri())) {
      return;
    }
    long expectedVersion = existing == null ? 0L : existing.version();
    ops.add(new JobIndexUpsert(pointerKey, expectedVersion, reference));
  }

  private void appendOwnedDelete(
      List<JobIndexWriteOp> ops, String pointerKey, String canonicalPointerKey) {
    if (blank(pointerKey) || blank(canonicalPointerKey)) {
      return;
    }
    StoredPointerSnapshot existing = jobIndexBackend.loadStoredPointer(pointerKey).orElse(null);
    if (existing != null && canonicalPointerKey.equals(existing.blobUri())) {
      ops.add(new JobIndexDelete(pointerKey, existing.version()));
    }
  }

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !requiresReadyPointer(record)) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
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
        ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
        readyKeys.add(readyPointerKeyFor(record, dueAtMs));
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

  private long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  private boolean requiresReadyPointer(StoredReconcileJob record) {
    return record != null && "JS_QUEUED".equals(record.state);
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
      StringBuilder next = new StringBuilder();
      List<StoredPointerSnapshot> pointers =
          jobIndexBackend.listStoredPointersByPrefix(
              Keys.reconcileJobPointerByIdPrefix(accountId), Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        StoredPointerSnapshot ptr = pointers.get(i);
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
        synchronized (state) {
          state.put(ptr.pointerKey(), cloneStoredRecord(stored));
        }
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          nextToken =
              !hasMore
                  ? ""
                  : (i + 1 < pointers.size() ? encodeListCursor(token, i + 1) : next.toString());
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
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
    return listByPrefixPage(
        Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId),
        pageSize,
        pageToken,
        record -> states == null || states.isEmpty() || states.contains(record.state));
  }

  private StoredJobPage listByAccountStateIndexes(
      String accountId, int pageSize, String pageToken, List<String> orderedStates) {
    StateListCursor cursor = decodeStateListCursor(pageToken, orderedStates);
    return listByStatePrefixes(
        orderedStates.stream()
            .map(state -> Keys.reconcileJobByAccountStatePointerPrefix(accountId, state))
            .toList(),
        pageSize,
        cursor);
  }

  private StoredJobPage listByConnectorStateIndexes(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      List<String> orderedStates) {
    StateListCursor cursor = decodeStateListCursor(pageToken, orderedStates);
    return listByStatePrefixes(
        orderedStates.stream()
            .map(
                state ->
                    Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state))
            .toList(),
        pageSize,
        cursor);
  }

  private StoredJobPage listByStatePrefixes(
      List<String> prefixes, int pageSize, StateListCursor cursor) {
    int limit = Math.max(1, pageSize);
    int stateIndex = Math.min(cursor.stateIndex(), prefixes.size());
    String token = cursor.storeToken();
    List<StoredReconcileJob> out = new ArrayList<>(limit);
    while (stateIndex < prefixes.size() && out.size() < limit) {
      StoredJobPage page =
          listByPrefixPage(prefixes.get(stateIndex), limit - out.size(), token, record -> true);
      out.addAll(page.records());
      if (!page.nextPageToken().isBlank()) {
        return new StoredJobPage(out, encodeStateListCursor(stateIndex, page.nextPageToken()));
      }
      if (out.size() >= limit) {
        String nextToken =
            stateIndex + 1 < prefixes.size() ? encodeStateListCursor(stateIndex + 1, "") : "";
        return new StoredJobPage(out, nextToken);
      }
      stateIndex++;
      token = "";
    }
    return new StoredJobPage(out, "");
  }

  private StoredJobPage listByPrefixPage(
      String prefix,
      int pageSize,
      String pageToken,
      java.util.function.Predicate<StoredReconcileJob> filter) {
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<StoredReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<StoredPointerSnapshot> pointers =
          jobIndexBackend.listStoredPointersByPrefix(prefix, Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        var rec = readCurrentRecordFromIndexPointer(pointers.get(i));
        if (rec.isEmpty() || !filter.test(rec.get())) {
          continue;
        }
        out.add(rec.get());
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          nextToken =
              !hasMore
                  ? ""
                  : (i + 1 < pointers.size() ? encodeListCursor(token, i + 1) : next.toString());
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf("Reconcile job index list hit page cap prefix=%s out=%d", prefix, out.size());
        break;
      }
      token = nextToken;
    }
    return new StoredJobPage(out, nextToken);
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

  private long countPointers(String prefix) {
    long count = 0L;
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<StoredPointerSnapshot> pointers =
          jobIndexBackend.listStoredPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        return count;
      }
      count += pointers.size();
      token = next.toString();
      if (token.isBlank()) {
        return count;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf("Reconcile job index count hit page cap prefix=%s count=%d", prefix, count);
        return count;
      }
    }
  }

  private record ListCursor(String storeToken, int skip) {}

  private record StateListCursor(int stateIndex, String storeToken) {}

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
