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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
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
      if (compareAndSetBatchWithPointerOps(writeBatch, List.of())) {
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
    return loadFromDedupe(dedupePointerKey, "");
  }

  private Optional<StoredReconcileJob> loadFromDedupe(
      String dedupePointerKey, String acceptedTerminalParentJobId) {
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
    if (!isDedupeActiveState(record.get().state)
        && !blank(acceptedTerminalParentJobId)
        && acceptedTerminalParentJobId.equals(
            record.get().parentJobId == null ? "" : record.get().parentJobId)) {
      return record;
    }
    if (!isDedupeActiveState(record.get().state)) {
      compareAndSetBatchWithPointerOps(
          new JobIndexWriteBatch(
              List.of(
                  new JobIndexDelete(
                      dedupePointer.pointerKey(),
                      dedupePointer.version(),
                      dedupePointer.blobUri())),
              ReadyQueueMutation.empty()),
          List.of());
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
      JobWriteChunk<QueuedJobInsert> chunk =
          requireSingleAtomicWrite(new JobWritePlan<>(insert, batch, List.of()));
      if (jobIndexBackend.compareAndSetBatch(chunk.indexBatch(), chunk.extraPointerOps())) {
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
    java.util.LinkedHashMap<Integer, BulkEnqueueItemResult> resultsByIndex =
        new java.util.LinkedHashMap<>();
    try {
      for (int attempt = 0; attempt < casMax; attempt++) {
        if (pending.isEmpty()) {
          return new ArrayList<>(resultsByIndex.values());
        }
        List<QueuedJobInsert> nextPending = new ArrayList<>(pending.size());
        List<JobIndexWriteBatch> insertBatches = new ArrayList<>(pending.size());
        for (QueuedJobInsert insert : pending) {
          String acceptedTerminalParentJobId =
              ancestorMutationsBuilder != null && insert.record() != null
                  ? (insert.record().parentJobId == null ? "" : insert.record().parentJobId)
                  : "";
          var existing = loadFromDedupe(insert.dedupePointerKey(), acceptedTerminalParentJobId);
          if (existing.isPresent()) {
            resultsByIndex.putIfAbsent(
                insert.index(),
                new BulkEnqueueItemResult(insert.index(), existing.get().jobId, false, ""));
            continue;
          }
          JobIndexEntrySnapshot existingDedupePointer =
              jobIndexBackend.loadIndexEntry(insert.dedupePointerKey()).orElse(null);
          insertBatches.add(queuedJobInsertOps(insert, existingDedupePointer));
          nextPending.add(insert);
        }
        List<CanonicalRecordMutation> ancestorMutations =
            ancestorMutationsBuilder == null ? List.of() : ancestorMutationsBuilder.apply(inserts);
        if (nextPending.isEmpty()) {
          if (ancestorMutations.isEmpty()) {
            return new ArrayList<>(resultsByIndex.values());
          }
          if (compareAndSetCanonicalMutations(ancestorMutations)) {
            return new ArrayList<>(resultsByIndex.values());
          }
          continue;
        }
        List<CommitBatch> commitBatches =
            buildCommitBatches(nextPending, insertBatches, ancestorMutations);
        boolean committed = true;
        int firstFailedBatch = -1;
        for (int batchIndex = 0; batchIndex < commitBatches.size(); batchIndex++) {
          CommitBatch commitBatch = commitBatches.get(batchIndex);
          final boolean batchCommitted;
          try {
            batchCommitted = jobIndexBackend.compareAndSetBatch(commitBatch.batch());
          } catch (RuntimeException e) {
            // Exclude the ambiguous failing batch: retaining payloads may leak blobs, but deleting
            // them could leave a transaction that committed remotely pointing at missing data.
            throw new BulkEnqueueCommitException(
                e, queuedInsertIndexes(remainingInserts(commitBatches, batchIndex + 1)));
          }
          if (batchCommitted) {
            continue;
          }
          committed = false;
          firstFailedBatch = batchIndex;
          break;
        }
        if (committed) {
          return new ArrayList<>(resultsByIndex.values());
        }
        pending = remainingInserts(commitBatches, firstFailedBatch);
      }
    } catch (RuntimeException e) {
      if (e instanceof BulkEnqueueCommitException) {
        throw e;
      }
      throw new BulkEnqueueCommitException(e, queuedInsertIndexes(pending));
    }
    for (QueuedJobInsert insert : pending) {
      resultsByIndex.putIfAbsent(
          insert.index(),
          new BulkEnqueueItemResult(
              insert.index(), "", false, "Unable to enqueue reconcile job after CAS retries"));
    }
    return new ArrayList<>(resultsByIndex.values());
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
    if (batches.isEmpty()) {
      return true;
    }
    JobWriteChunk<List<CanonicalRecordMutation>> chunk =
        requireSingleAtomicWrite(
            new JobWritePlan<>(List.copyOf(mutations), combineWriteBatches(batches), List.of()));
    return jobIndexBackend.compareAndSetBatch(chunk.indexBatch(), chunk.extraPointerOps());
  }

  @Override
  public boolean compareAndSetBatchWithPointerOps(
      JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    if (writeItemCount(batch, extraPointerOps) == 0) {
      return true;
    }
    JobWriteChunk<Void> chunk =
        requireSingleAtomicWrite(new JobWritePlan<>(null, batch, extraPointerOps));
    return jobIndexBackend.compareAndSetBatch(chunk.indexBatch(), chunk.extraPointerOps());
  }

  @Override
  public boolean commitReadyQueueRepairBatch(
      List<CanonicalRecordMutation> mutations, List<ReadyQueueWrite> readyWrites) {
    boolean hasMutations = mutations != null && !mutations.isEmpty();
    boolean hasReadyWrites = readyWrites != null && !readyWrites.isEmpty();
    if (!hasMutations && !hasReadyWrites) {
      return true;
    }
    List<JobIndexWriteBatch> batches = new ArrayList<>();
    if (hasMutations) {
      for (CanonicalRecordMutation mutation : mutations) {
        if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
          continue;
        }
        batches.add(
            buildJobIndexWriteBatch(mutation.snapshot(), mutation.previous(), mutation.current()));
      }
    }
    if (hasReadyWrites) {
      batches.add(
          new JobIndexWriteBatch(
              List.of(), new ReadyQueueMutation(List.copyOf(readyWrites), List.of())));
    }
    if (batches.isEmpty()) {
      return true;
    }
    JobWriteChunk<Void> chunk =
        requireSingleAtomicWrite(new JobWritePlan<>(null, combineWriteBatches(batches), List.of()));
    return jobIndexBackend.compareAndSetBatch(chunk.indexBatch(), chunk.extraPointerOps());
  }

  private <T> JobWriteChunk<T> requireSingleAtomicWrite(JobWritePlan<T> plan) {
    return chunkJobWritePlans(List.of(plan)).getFirst();
  }

  public StoredJobPage listStoredJobs(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    return listAccountWide(
        accountId, pageSize, pageToken, connectorId, normalizeStateFilter(states));
  }

  @Override
  public StoredJobPage listStoredJobsInState(String state, int pageSize, String pageToken) {
    String effectiveState = blankToEmpty(state);
    if (effectiveState.isBlank()) {
      return new StoredJobPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    String token = pageToken == null ? "" : pageToken;
    var page = jobIndexBackend.listGlobalStateEntries(effectiveState, limit, token);
    List<StoredReconcileJob> out = new ArrayList<>(page.entries().size());
    for (JobIndexEntrySnapshot ptr : page.entries()) {
      readCurrentRecordFromStateIndexPointer(
              ptr, record -> effectiveState.equals(blankToEmpty(record.state)))
          .ifPresent(out::add);
    }
    return new StoredJobPage(List.copyOf(out), page.nextPageToken());
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

  @Override
  public long countStoredChildJobs(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return 0L;
    }
    return countPages(
        token -> jobIndexBackend.listParentEntries(accountId, parentJobId, 256, token));
  }

  public long countStoredJobsInState(String state) {
    return 0L;
  }

  public long oldestStoredJobTimestampInState(String state) {
    return 0L;
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
            payloadStore.encodeInlineJobState(current),
            PointerReferenceKind.PRK_INLINE_JSON,
            cleanupManifest(current)));

    appendReferenceTransition(
        ops,
        previous == null || blank(previous.jobId)
            ? ""
            : Keys.reconcileJobLookupPointerById(previous.jobId),
        blank(current.jobId) ? "" : Keys.reconcileJobLookupPointerById(current.jobId),
        canonicalPointerKey);
    appendReferenceTransition(
        ops,
        previous == null || blank(previous.parentJobId)
            ? ""
            : indexes.parentPointerKey(previous.accountId, previous.parentJobId, previous.jobId),
        blank(current.parentJobId)
            ? ""
            : indexes.parentPointerKey(current.accountId, current.parentJobId, current.jobId),
        canonicalPointerKey);
    appendReferenceTransition(
        ops,
        previous == null ? "" : connectorIndexPointerKey(previous),
        connectorIndexPointerKey(current),
        canonicalPointerKey);
    appendReferenceSetTransition(
        ops,
        previous == null ? List.of() : indexes.statePointerKeys(previous),
        indexes.statePointerKeys(current),
        canonicalPointerKey);
    String previousDedupePointerKey = previous == null ? "" : indexes.dedupePointerKey(previous);
    String currentDedupePointerKey = indexes.dedupePointerKey(current);
    // A direct child keeps its dedupe ownership through terminal state. Parent-first planner
    // publication can leave later child chunks to a retry; without this stable identity, a child
    // that finishes before the retry would lose its dedupe row and be recreated as an N+1 child.
    // GC removes the retained child dedupe row with the child's canonical record.
    boolean previousDedupeActive = previous != null && retainsDedupeOwnership(previous);
    boolean currentDedupeActive = retainsDedupeOwnership(current);
    if (!currentDedupeActive) {
      String dedupePointerToDelete =
          previousDedupePointerKey.isBlank() ? currentDedupePointerKey : previousDedupePointerKey;
      if (!dedupePointerToDelete.isBlank()
          && (previousDedupeActive
              || !Objects.equals(previousDedupePointerKey, currentDedupePointerKey))) {
        appendOwnedDelete(ops, dedupePointerToDelete, canonicalPointerKey);
      }
    } else if (!currentDedupePointerKey.isBlank()
        && (!previousDedupeActive
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
  public JobIndexWriteBatch buildJobDeleteBatch(CanonicalPointerSnapshot currentSnapshot) {
    if (currentSnapshot == null || blank(currentSnapshot.canonicalPointerKey())) {
      return JobIndexWriteBatch.empty();
    }
    return buildJobDeleteBatch(currentSnapshot, ReconcileJobIndexCleanupManifest.EMPTY);
  }

  @Override
  public JobIndexWriteBatch buildReadableLegacyJobDeleteBatch(
      CanonicalPointerSnapshot currentSnapshot, StoredReconcileJob readableRecord) {
    if (currentSnapshot == null
        || readableRecord == null
        || blank(currentSnapshot.canonicalPointerKey())
        || blank(readableRecord.accountId)
        || blank(readableRecord.jobId)
        || !currentSnapshot
            .canonicalPointerKey()
            .equals(Keys.reconcileJobPointerById(readableRecord.accountId, readableRecord.jobId))) {
      return JobIndexWriteBatch.empty();
    }
    return buildJobDeleteBatch(currentSnapshot, cleanupManifest(readableRecord));
  }

  @Override
  public JobIndexWriteBatch buildDiscoveredLegacyJobDeleteBatch(
      CanonicalPointerSnapshot currentSnapshot, ReconcileJobIndexCleanupManifest manifest) {
    if (currentSnapshot == null || blank(currentSnapshot.canonicalPointerKey())) {
      return JobIndexWriteBatch.empty();
    }
    return buildJobDeleteBatch(currentSnapshot, manifest);
  }

  private JobIndexWriteBatch buildJobDeleteBatch(
      CanonicalPointerSnapshot currentSnapshot, ReconcileJobIndexCleanupManifest fallbackManifest) {
    var session = jobIndexBackend.beginJobCleanup(currentSnapshot, fallbackManifest).orElse(null);
    if (session == null || (session.manifest().isEmpty() && !session.footprintDrained())) {
      return JobIndexWriteBatch.empty();
    }
    CanonicalPointerSnapshot lockedSnapshot = session.snapshot();
    ReconcileJobIndexCleanupManifest manifest = session.manifest();
    String canonicalPointerKey = lockedSnapshot.canonicalPointerKey();
    List<JobIndexWriteOp> deletes = new ArrayList<>();
    deletes.add(
        new JobIndexDelete(
            canonicalPointerKey, lockedSnapshot.version(), "", "", session.cleanupLocked()));
    for (String pointerKey : manifest.indexPointerKeys()) {
      appendOwnedDelete(deletes, pointerKey, canonicalPointerKey);
    }
    return new JobIndexWriteBatch(
        List.copyOf(deletes), new ReadyQueueMutation(List.of(), manifest.readyPointerKeys()));
  }

  @Override
  public IndexBackfillResult backfillStoredJobIndexes(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return IndexBackfillResult.unchanged();
    }
    CanonicalPointerSnapshot snapshot = loadCanonicalSnapshot(canonicalPointerKey).orElse(null);
    if (snapshot == null) {
      return IndexBackfillResult.unchanged();
    }
    StoredReconcileJob record = readRecord(snapshot).orElse(null);
    if (record == null) {
      return IndexBackfillResult.unchanged();
    }
    List<String> desiredKeys = new ArrayList<>();
    try {
      desiredKeys.add(connectorIndexPointerKey(record));
      desiredKeys.addAll(indexes.statePointerKeys(record));
    } catch (RuntimeException ignored) {
      return new IndexBackfillResult(false, true);
    }
    desiredKeys.removeIf(NativeReconcileJobIndexStore::blank);
    List<JobIndexWriteOp> writes = new ArrayList<>();
    for (String pointerKey : new java.util.LinkedHashSet<>(desiredKeys)) {
      JobIndexEntrySnapshot existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
      if (existing != null) {
        if (!canonicalPointerKey.equals(existing.blobUri())) {
          return new IndexBackfillResult(false, true);
        }
        continue;
      }
      writes.add(
          new JobIndexUpsert(
              pointerKey, 0L, canonicalPointerKey, PointerReferenceKind.PRK_POINTER_KEY));
    }
    if (writes.isEmpty()) {
      return IndexBackfillResult.unchanged();
    }
    ReconcileJobIndexCleanupManifest manifest;
    try {
      manifest = cleanupManifest(record);
    } catch (RuntimeException ignored) {
      try {
        manifest =
            new ReconcileJobIndexCleanupManifest(indexPointerKeysForCleanup(record), List.of());
      } catch (RuntimeException invalidRecord) {
        return new IndexBackfillResult(false, true);
      }
    }
    writes.add(
        0,
        new JobIndexUpsert(
            canonicalPointerKey,
            snapshot.version(),
            snapshot.blobUri(),
            PointerReferenceKind.PRK_INLINE_JSON,
            manifest));
    boolean committed =
        jobIndexBackend.compareAndSetBatch(
            new JobIndexWriteBatch(List.copyOf(writes), ReadyQueueMutation.empty()));
    return committed ? new IndexBackfillResult(true, false) : new IndexBackfillResult(false, true);
  }

  @Override
  public int writeItemCount(JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    return physicalWriteItemCount(batch) + (extraPointerOps == null ? 0 : extraPointerOps.size());
  }

  @Override
  public int maxWriteItemsPerBatch() {
    return ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS;
  }

  @Override
  public <T> List<JobWriteChunk<T>> chunkJobWritePlans(List<JobWritePlan<T>> plans) {
    if (plans == null || plans.isEmpty()) {
      return List.of();
    }
    List<JobWriteChunk<T>> chunks = new ArrayList<>();
    List<JobWritePlan<T>> chunkPlans = new ArrayList<>();
    int chunkItems = 0;
    for (JobWritePlan<T> plan : plans) {
      int planItems = writeItemCount(plan.indexBatch(), plan.extraPointerOps());
      if (planItems > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
        throw new IllegalStateException(
            "single atomic job write requires more than "
                + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
                + " write items");
      }
      if (chunkItems > 0
          && chunkItems + planItems > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
        chunks.add(buildJobWriteChunk(chunkPlans));
        chunkPlans = new ArrayList<>();
        chunkItems = 0;
      }
      if (planItems > 0) {
        chunkPlans.add(plan);
        chunkItems += planItems;
      }
    }
    if (!chunkPlans.isEmpty()) {
      chunks.add(buildJobWriteChunk(chunkPlans));
    }
    return List.copyOf(chunks);
  }

  @Override
  public <T> List<JobWriteChunk<T>> chunkOversizedJobDeletePlan(JobWritePlan<T> plan) {
    if (plan == null) {
      return List.of();
    }
    int totalItems = writeItemCount(plan.indexBatch(), plan.extraPointerOps());
    if (totalItems <= ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      return List.of(buildJobWriteChunk(List.of(plan)));
    }
    JobIndexDelete canonicalDelete =
        plan.indexBatch().writes().stream()
            .filter(JobIndexDelete.class::isInstance)
            .map(JobIndexDelete.class::cast)
            .filter(
                delete -> JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey()) != null)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "oversized job cleanup is missing its canonical delete"));
    if (!canonicalDelete.requireCleanupLock()) {
      throw new IllegalStateException("oversized job cleanup requires a canonical cleanup lock");
    }
    if (!plan.indexBatch().readyMutation().upserts().isEmpty()) {
      throw new IllegalStateException("job cleanup cannot contain ready-queue upserts");
    }

    List<JobWriteChunk<T>> chunks = new ArrayList<>();
    List<JobIndexWriteOp> referenceWrites = new ArrayList<>();
    List<String> readyDeletes = new ArrayList<>();
    int referenceItems = 0;
    int referenceCapacity = ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS - 1;
    for (JobIndexWriteOp write : plan.indexBatch().writes()) {
      if (write == canonicalDelete) {
        continue;
      }
      int itemCount = physicalWriteItemCount(write);
      if (referenceItems > 0 && referenceItems + itemCount > referenceCapacity) {
        chunks.add(buildCleanupReferenceChunk(canonicalDelete, referenceWrites, readyDeletes));
        referenceWrites = new ArrayList<>();
        readyDeletes = new ArrayList<>();
        referenceItems = 0;
      }
      referenceWrites.add(write);
      referenceItems += itemCount;
    }
    for (String readyDelete : plan.indexBatch().readyMutation().deletes()) {
      if (referenceItems >= referenceCapacity) {
        chunks.add(buildCleanupReferenceChunk(canonicalDelete, referenceWrites, readyDeletes));
        referenceWrites = new ArrayList<>();
        readyDeletes = new ArrayList<>();
        referenceItems = 0;
      }
      readyDeletes.add(readyDelete);
      referenceItems++;
    }
    if (referenceItems > 0) {
      chunks.add(buildCleanupReferenceChunk(canonicalDelete, referenceWrites, readyDeletes));
    }

    JobIndexWriteBatch finalBatch =
        new JobIndexWriteBatch(List.of(canonicalDelete), ReadyQueueMutation.empty());
    JobWritePlan<T> finalPlan =
        new JobWritePlan<>(plan.subject(), finalBatch, plan.extraPointerOps());
    if (writeItemCount(finalBatch, plan.extraPointerOps())
        > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      throw new IllegalStateException(
          "final job cleanup transaction exceeds "
              + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
              + " write items");
    }
    chunks.add(buildJobWriteChunk(List.of(finalPlan)));
    return List.copyOf(chunks);
  }

  private <T> JobWriteChunk<T> buildCleanupReferenceChunk(
      JobIndexDelete canonicalDelete,
      List<JobIndexWriteOp> referenceWrites,
      List<String> readyDeletes) {
    List<JobIndexWriteOp> writes = new ArrayList<>(referenceWrites.size() + 1);
    writes.add(
        new JobIndexCheck(canonicalDelete.pointerKey(), canonicalDelete.expectedVersion(), true));
    writes.addAll(referenceWrites);
    JobWritePlan<T> chunkPlan =
        new JobWritePlan<>(
            null,
            new JobIndexWriteBatch(
                List.copyOf(writes), new ReadyQueueMutation(List.of(), List.copyOf(readyDeletes))),
            List.of());
    return buildJobWriteChunk(List.of(chunkPlan));
  }

  private <T> JobWriteChunk<T> buildJobWriteChunk(List<JobWritePlan<T>> plans) {
    List<JobIndexWriteBatch> indexBatches = new ArrayList<>(plans.size());
    List<PointerStore.CasOp> pointerOps = new ArrayList<>();
    for (JobWritePlan<T> plan : plans) {
      indexBatches.add(plan.indexBatch());
      pointerOps.addAll(plan.extraPointerOps());
    }
    return new JobWriteChunk<>(
        List.copyOf(plans), combineWriteBatches(indexBatches), List.copyOf(pointerOps));
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
    copy.snapshotTaskSourceFileCount = source.snapshotTaskSourceFileCount;
    copy.snapshotTaskDirectStatsBlobUri = source.snapshotTaskDirectStatsBlobUri;
    copy.snapshotTaskDirectStatsRecordCount = source.snapshotTaskDirectStatsRecordCount;
    copy.snapshotTaskDirectStatsPersistedRecordCountsByChunk =
        source.snapshotTaskDirectStatsPersistedRecordCountsByChunk == null
            ? java.util.Map.of()
            : java.util.Map.copyOf(source.snapshotTaskDirectStatsPersistedRecordCountsByChunk);
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
    copy.plannerOutcomeFingerprint = source.plannerOutcomeFingerprint;
    copy.plannerOutcomeLeaseEpoch = source.plannerOutcomeLeaseEpoch;
    copy.childrenFinalized = source.childrenFinalized;
    copy.statsCleanupState = source.statsCleanupState;
    copy.statsCleanupUpdatedAtMs = source.statsCleanupUpdatedAtMs;
    copy.projectionRequestedGeneration = source.projectionRequestedGeneration;
    copy.projectionAppliedGeneration = source.projectionAppliedGeneration;
    copy.attempt = source.attempt;
    copy.nextAttemptAtMs = source.nextAttemptAtMs;
    copy.lastError = source.lastError;
    copy.laneKey = source.laneKey;
    copy.dedupeKeyHash = source.dedupeKeyHash;
    copy.readyPointerKey = source.readyPointerKey;
    copy.readyIndexVersion = source.readyIndexVersion;
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
    ops.add(
        new JobIndexUpsert(
            pointerKey, expectedVersion, reference, PointerReferenceKind.PRK_POINTER_KEY));
  }

  private void appendReferenceTransition(
      List<JobIndexWriteOp> ops,
      String previousPointerKey,
      String currentPointerKey,
      String canonicalPointerKey) {
    String previousKey = blankToEmpty(previousPointerKey);
    String currentKey = blankToEmpty(currentPointerKey);
    if (Objects.equals(previousKey, currentKey)) {
      return;
    }
    appendReferenceUpsert(ops, currentKey, canonicalPointerKey);
    appendOwnedDelete(ops, previousKey, canonicalPointerKey);
  }

  private void appendReferenceSetTransition(
      List<JobIndexWriteOp> ops,
      List<String> previousPointerKeys,
      List<String> currentPointerKeys,
      String canonicalPointerKey) {
    java.util.LinkedHashSet<String> previous =
        new java.util.LinkedHashSet<>(
            previousPointerKeys == null ? List.of() : previousPointerKeys);
    java.util.LinkedHashSet<String> current =
        new java.util.LinkedHashSet<>(currentPointerKeys == null ? List.of() : currentPointerKeys);
    for (String pointerKey : current) {
      if (!previous.contains(pointerKey)) {
        appendReferenceUpsert(ops, pointerKey, canonicalPointerKey);
      }
    }
    for (String pointerKey : previous) {
      if (!current.contains(pointerKey)) {
        appendOwnedDelete(ops, pointerKey, canonicalPointerKey);
      }
    }
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
      ops.add(
          new JobIndexDelete(
              pointerKey,
              existing.version(),
              canonicalPointerKey,
              existing.lookupStoragePartitionKey()));
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
        readyKeys.addAll(
            ReadyQueueKeys.readyPointerKeys(
                record, dueAtMs, NativeReconcileJobIndexStore::requiresReadyPointer));
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

  private ReconcileJobIndexCleanupManifest cleanupManifest(StoredReconcileJob record) {
    return new ReconcileJobIndexCleanupManifest(
        indexPointerKeysForCleanup(record), readyPointerKeysForCleanup(record));
  }

  private ReconcileJobIndexCleanupManifest cleanupManifest(QueuedJobInsert insert) {
    if (insert == null) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    java.util.LinkedHashSet<String> indexKeys = new java.util.LinkedHashSet<>();
    indexKeys.add(insert.lookupKey());
    indexKeys.add(insert.parentKey());
    indexKeys.add(queuedConnectorIndexPointerKey(insert));
    indexKeys.addAll(insert.stateKeys());
    indexKeys.add(insert.dedupePointerKey());
    indexKeys.removeIf(NativeReconcileJobIndexStore::blank);
    return new ReconcileJobIndexCleanupManifest(
        List.copyOf(indexKeys), insert.readyKeys() == null ? List.of() : insert.readyKeys());
  }

  private List<String> indexPointerKeysForCleanup(StoredReconcileJob record) {
    if (record == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> indexKeys = new java.util.LinkedHashSet<>();
    if (!blank(record.jobId)) {
      indexKeys.add(Keys.reconcileJobLookupPointerById(record.jobId));
    }
    if (!blank(record.parentJobId)) {
      indexKeys.add(indexes.parentPointerKey(record.accountId, record.parentJobId, record.jobId));
    }
    indexKeys.add(connectorIndexPointerKey(record));
    indexKeys.addAll(indexes.statePointerKeys(record));
    indexKeys.add(indexes.dedupePointerKey(record));
    indexKeys.removeIf(NativeReconcileJobIndexStore::blank);
    return List.copyOf(indexKeys);
  }

  private String connectorIndexPointerKey(StoredReconcileJob record) {
    if (record == null) {
      return "";
    }
    return blank(record.connectorIndexPointerKey)
        ? indexes.connectorIndexPointerKey(
            record.accountId, record.connectorId, record.createdAtMs, record.jobId)
        : record.connectorIndexPointerKey;
  }

  private String queuedConnectorIndexPointerKey(QueuedJobInsert insert) {
    if (insert == null) {
      return "";
    }
    return blank(insert.connectorIndexKey())
        ? connectorIndexPointerKey(insert.record())
        : insert.connectorIndexKey();
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

  private static boolean isDedupeActiveState(String state) {
    return !isTerminalState(state) && !"JS_CANCELLING".equals(state);
  }

  private static boolean retainsDedupeOwnership(StoredReconcileJob record) {
    return record != null && (isDedupeActiveState(record.state) || !blank(record.parentJobId));
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
            PointerReferenceKind.PRK_INLINE_JSON,
            cleanupManifest(insert)));
    ops.add(
        new JobIndexUpsert(
            insert.lookupKey(), 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
    if (!insert.parentKey().isBlank()) {
      ops.add(
          new JobIndexUpsert(
              insert.parentKey(), 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
    }
    String connectorIndexKey = queuedConnectorIndexPointerKey(insert);
    if (!blank(connectorIndexKey)) {
      ops.add(
          new JobIndexUpsert(
              connectorIndexKey, 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
    }
    for (String stateKey : insert.stateKeys()) {
      if (!blank(stateKey)) {
        ops.add(
            new JobIndexUpsert(
                stateKey, 0L, insert.canonicalKey(), PointerReferenceKind.PRK_POINTER_KEY));
      }
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

  private List<CommitBatch> buildCommitBatches(
      List<QueuedJobInsert> inserts,
      List<JobIndexWriteBatch> insertBatches,
      List<CanonicalRecordMutation> ancestorMutations) {
    List<JobIndexWriteBatch> ancestorBatches = ancestorWriteBatches(ancestorMutations);
    int ancestorItemCount = totalWriteItems(ancestorBatches);
    if (ancestorItemCount > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      throw new IllegalStateException(
          "ancestor reconcile mutation requires more than "
              + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
              + " write items");
    }

    int trailingInsertStart = trailingInsertStart(insertBatches, ancestorItemCount);
    List<CommitBatch> leadingCommitBatches = new ArrayList<>();
    List<JobWritePlan<QueuedJobInsert>> leadingPlans = new ArrayList<>();
    for (int i = 0; i < trailingInsertStart; i++) {
      leadingPlans.add(new JobWritePlan<>(inserts.get(i), insertBatches.get(i), List.of()));
    }
    JobIndexWriteBatch postAncestorGuard = ancestorPostCommitGuard(ancestorMutations);
    if (postAncestorGuard.writes().isEmpty()) {
      for (JobWriteChunk<QueuedJobInsert> chunk : chunkJobWritePlans(leadingPlans)) {
        leadingCommitBatches.add(
            new CommitBatch(
                chunk.plans().stream().map(JobWritePlan::subject).toList(), chunk.indexBatch()));
      }
    } else {
      leadingCommitBatches.addAll(
          guardedPlannerChildCommitBatches(leadingPlans, postAncestorGuard));
    }

    List<QueuedJobInsert> trailingInserts = inserts.subList(trailingInsertStart, inserts.size());
    List<JobIndexWriteBatch> trailingInsertBatches =
        insertBatches.subList(trailingInsertStart, insertBatches.size());
    if (trailingInserts.isEmpty() && !ancestorBatches.isEmpty() && !inserts.isEmpty()) {
      throw new IllegalStateException(
          "unable to reserve space for ancestor reconcile mutation within a "
              + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
              + "-item batch");
    }
    List<JobIndexWriteBatch> finalBatches =
        new ArrayList<>(trailingInsertBatches.size() + ancestorBatches.size());
    finalBatches.addAll(trailingInsertBatches);
    finalBatches.addAll(ancestorBatches);
    JobIndexWriteBatch finalBatch = combineWriteBatches(finalBatches);
    JobWriteChunk<List<QueuedJobInsert>> finalChunk =
        chunkJobWritePlans(
                List.of(new JobWritePlan<>(List.copyOf(trailingInserts), finalBatch, List.of())))
            .getFirst();
    CommitBatch finalCommitBatch =
        new CommitBatch(finalChunk.plans().getFirst().subject(), finalChunk.indexBatch());
    List<CommitBatch> commitBatches = new ArrayList<>(leadingCommitBatches.size() + 1);
    if (!ancestorBatches.isEmpty()) {
      // Publish the parent outcome before any child-only chunk can make runnable work visible.
      // If a later child chunk fails, a retry observes the committed parent outcome and fills in
      // only the missing children through the normal dedupe path.
      commitBatches.add(finalCommitBatch);
      commitBatches.addAll(leadingCommitBatches);
    } else {
      commitBatches.addAll(leadingCommitBatches);
      commitBatches.add(finalCommitBatch);
    }
    return commitBatches;
  }

  private JobIndexWriteBatch ancestorPostCommitGuard(
      List<CanonicalRecordMutation> ancestorMutations) {
    if (ancestorMutations == null || ancestorMutations.isEmpty()) {
      return JobIndexWriteBatch.empty();
    }
    List<JobIndexWriteOp> checks = new ArrayList<>(ancestorMutations.size());
    for (CanonicalRecordMutation mutation : ancestorMutations) {
      if (mutation == null || mutation.snapshot() == null) {
        continue;
      }
      checks.add(
          new JobIndexCheck(
              mutation.snapshot().canonicalPointerKey(),
              mutation.snapshot().version() + 1L,
              false));
    }
    return new JobIndexWriteBatch(List.copyOf(checks), ReadyQueueMutation.empty());
  }

  private List<CommitBatch> guardedPlannerChildCommitBatches(
      List<JobWritePlan<QueuedJobInsert>> plans, JobIndexWriteBatch guard) {
    if (plans == null || plans.isEmpty()) {
      return List.of();
    }
    int guardItems = physicalWriteItemCount(guard);
    if (guardItems <= 0 || guardItems >= ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      throw new IllegalStateException("invalid planner parent guard write count: " + guardItems);
    }
    List<CommitBatch> chunks = new ArrayList<>();
    List<JobWritePlan<QueuedJobInsert>> pending = new ArrayList<>();
    int pendingItems = guardItems;
    for (JobWritePlan<QueuedJobInsert> plan : plans) {
      int planItems = writeItemCount(plan.indexBatch(), plan.extraPointerOps());
      if (planItems + guardItems > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
        throw new IllegalStateException(
            "planner child insert cannot fit with its parent guard: " + planItems);
      }
      if (!pending.isEmpty()
          && pendingItems + planItems > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
        chunks.add(guardedPlannerChildCommitBatch(pending, guard));
        pending = new ArrayList<>();
        pendingItems = guardItems;
      }
      pending.add(plan);
      pendingItems += planItems;
    }
    if (!pending.isEmpty()) {
      chunks.add(guardedPlannerChildCommitBatch(pending, guard));
    }
    return List.copyOf(chunks);
  }

  private CommitBatch guardedPlannerChildCommitBatch(
      List<JobWritePlan<QueuedJobInsert>> plans, JobIndexWriteBatch guard) {
    List<JobIndexWriteBatch> batches = new ArrayList<>(plans.size() + 1);
    batches.add(guard);
    plans.stream().map(JobWritePlan::indexBatch).forEach(batches::add);
    return new CommitBatch(
        plans.stream().map(JobWritePlan::subject).toList(), combineWriteBatches(batches));
  }

  private int trailingInsertStart(List<JobIndexWriteBatch> insertBatches, int reservedWriteItems) {
    if (insertBatches == null || insertBatches.isEmpty()) {
      return 0;
    }
    int finalCapacity = ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS - reservedWriteItems;
    if (finalCapacity <= 0) {
      return insertBatches.size();
    }
    int used = 0;
    int split = insertBatches.size();
    while (split > 0) {
      int batchItems = writeItemCount(insertBatches.get(split - 1), List.of());
      if (batchItems > finalCapacity) {
        return insertBatches.size();
      }
      if (used > 0 && used + batchItems > finalCapacity) {
        break;
      }
      used += batchItems;
      split--;
    }
    return split;
  }

  private List<JobIndexWriteBatch> ancestorWriteBatches(
      List<CanonicalRecordMutation> ancestorMutations) {
    if (ancestorMutations == null || ancestorMutations.isEmpty()) {
      return List.of();
    }
    List<JobIndexWriteBatch> batches = new ArrayList<>(ancestorMutations.size());
    for (CanonicalRecordMutation mutation : ancestorMutations) {
      if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
        continue;
      }
      batches.add(
          buildJobIndexWriteBatch(mutation.snapshot(), mutation.previous(), mutation.current()));
    }
    return batches;
  }

  private int totalWriteItems(List<JobIndexWriteBatch> batches) {
    int total = 0;
    if (batches == null) {
      return 0;
    }
    for (JobIndexWriteBatch batch : batches) {
      total += writeItemCount(batch, List.of());
    }
    return total;
  }

  static int physicalWriteItemCount(JobIndexWriteBatch batch) {
    if (batch == null) {
      return 0;
    }
    int count = batch.readyMutation().upserts().size() + batch.readyMutation().deletes().size();
    for (JobIndexWriteOp write : batch.writes()) {
      count += physicalWriteItemCount(write);
    }
    return count;
  }

  private static int physicalWriteItemCount(JobIndexWriteOp write) {
    String pointerKey = null;
    if (write instanceof JobIndexUpsert upsert) {
      pointerKey = upsert.pointerKey();
    } else if (write instanceof JobIndexDelete delete) {
      pointerKey = delete.pointerKey();
    } else if (write instanceof JobIndexCheck check) {
      pointerKey = check.pointerKey();
    } else if (write instanceof JobIndexCheckAbsent check) {
      pointerKey = check.pointerKey();
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pointerKey);
    if (lookupKey != null) {
      return JobIndexBackendSupport.lookupReadStorageKeys(lookupKey).size();
    }
    return 1;
  }

  private List<QueuedJobInsert> remainingInserts(
      List<CommitBatch> commitBatches, int firstFailedBatch) {
    if (commitBatches == null
        || commitBatches.isEmpty()
        || firstFailedBatch < 0
        || firstFailedBatch >= commitBatches.size()) {
      return List.of();
    }
    List<QueuedJobInsert> remaining = new ArrayList<>();
    for (int i = firstFailedBatch; i < commitBatches.size(); i++) {
      remaining.addAll(commitBatches.get(i).inserts());
    }
    return remaining;
  }

  private List<Integer> queuedInsertIndexes(List<QueuedJobInsert> inserts) {
    if (inserts == null || inserts.isEmpty()) {
      return List.of();
    }
    List<Integer> indexes = new ArrayList<>(inserts.size());
    for (QueuedJobInsert insert : inserts) {
      if (insert != null) {
        indexes.add(insert.index());
      }
    }
    return List.copyOf(indexes);
  }

  private record CommitBatch(List<QueuedJobInsert> inserts, JobIndexWriteBatch batch) {}

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

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    return ReadyQueueKeys.readyPointerKeys(
        record, NativeReconcileJobIndexStore::requiresReadyPointer);
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
