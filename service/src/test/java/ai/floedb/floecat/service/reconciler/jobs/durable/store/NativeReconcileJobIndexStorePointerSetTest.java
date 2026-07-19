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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeReconcileJobIndexStorePointerSetTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "connector-1";
  private static final String JOB_ID = "job-1";

  private InMemoryPointerStore pointers;
  private MemoryReconcileJobIndexBackend backend;
  private ReconcileJobIndexes indexes;
  private NativeReconcileJobIndexStore store;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    backend = new MemoryReconcileJobIndexBackend(pointers);
    indexes = new ReconcileJobIndexes();
    indexes.bind(pointers, ignored -> false, ignored -> List.of());
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), pointers, new ObjectMapper());
    store = new NativeReconcileJobIndexStore();
    store.bind(backend, payloadStore, indexes, 4, (previous, current) -> {}, (a, b, op) -> {});
  }

  @Test
  void createAndDeleteUseTheSameCompletePointerSet() {
    StoredReconcileJob record = record("JS_QUEUED");
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);
    String lookupKey = Keys.reconcileJobLookupPointerById(JOB_ID);
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    record.connectorIndexPointerKey = "";
    String connectorKey =
        indexes.connectorIndexPointerKey(
            ACCOUNT_ID, CONNECTOR_ID, record.createdAtMs, record.jobId);
    List<String> stateKeys = indexes.statePointerKeys(record);
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, record.dedupeKeyHash);

    assertNull(
        store.commitQueuedJobInsert(
            new ReconcileJobIndexStore.QueuedJobInsert(
                0,
                dedupeKey,
                canonicalKey,
                lookupKey,
                parentKey,
                List.of(),
                stateKeys,
                "",
                "",
                record)));

    List<String> expectedIndexes = new ArrayList<>();
    expectedIndexes.add(dedupeKey);
    expectedIndexes.add(lookupKey);
    expectedIndexes.add(parentKey);
    expectedIndexes.add(connectorKey);
    expectedIndexes.addAll(stateKeys);
    for (String pointerKey : expectedIndexes) {
      assertTrue(backend.loadIndexEntry(pointerKey).isPresent(), pointerKey);
    }

    JobIndexEntrySnapshot canonical = backend.loadIndexEntry(canonicalKey).orElseThrow();
    assertEquals(
        List.of(Keys.reconcileJobProjectionPointer(ACCOUNT_ID, JOB_ID)),
        backend.loadCleanupManifest(canonicalKey).pointerKeys());
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        store.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
    assertEquals(1 + expectedIndexes.size(), deleteBatch.writes().size());
    assertTrue(
        deleteBatch.writes().stream()
            .filter(ReconcileJobIndexStore.JobIndexDelete.class::isInstance)
            .map(ReconcileJobIndexStore.JobIndexDelete.class::cast)
            .filter(delete -> !canonicalKey.equals(delete.pointerKey()))
            .allMatch(delete -> canonicalKey.equals(delete.expectedCanonicalPointerKey())));
    assertTrue(backend.compareAndSetBatch(deleteBatch));
    assertTrue(backend.loadIndexEntry(canonicalKey).isEmpty());
    for (String pointerKey : expectedIndexes) {
      assertTrue(backend.loadIndexEntry(pointerKey).isEmpty(), pointerKey);
    }
  }

  @Test
  void rootJobManifestIncludesProjectionAndBothSummaryPointers() {
    StoredReconcileJob record = record("JS_SUCCEEDED");
    record.parentJobId = "";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);
    String sortableToken = String.format("%019d-%s", Long.MAX_VALUE - record.createdAtMs, JOB_ID);

    ReconcileJobIndexStore.JobIndexWriteBatch batch =
        store.buildJobIndexWriteBatch(
            new CanonicalPointerSnapshot(canonicalKey, "inline:reconcile-job:e30", 1L),
            null,
            record);

    ReconcileJobIndexStore.JobIndexUpsert canonicalUpsert =
        batch.writes().stream()
            .filter(ReconcileJobIndexStore.JobIndexUpsert.class::isInstance)
            .map(ReconcileJobIndexStore.JobIndexUpsert.class::cast)
            .filter(upsert -> canonicalKey.equals(upsert.pointerKey()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        List.of(
            Keys.reconcileJobProjectionPointer(ACCOUNT_ID, JOB_ID),
            Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, sortableToken),
            Keys.reconcileRootJobSummaryByConnectorPointer(
                ACCOUNT_ID, CONNECTOR_ID, sortableToken)),
        canonicalUpsert.cleanupManifest().pointerKeys());
  }

  @Test
  void drainedLegacyFootprintBuildsCanonicalOnlyDelete() {
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, "legacy-drained");
    MemoryReconcileJobIndexBackend drainedBackend =
        new MemoryReconcileJobIndexBackend(pointers) {
          @Override
          public Optional<ReconcileJobIndexBackend.JobCleanupSession> beginJobCleanup(
              CanonicalPointerSnapshot expected,
              ReconcileJobIndexCleanupManifest fallbackManifest) {
            return Optional.of(
                new ReconcileJobIndexBackend.JobCleanupSession(
                    expected, ReconcileJobIndexCleanupManifest.EMPTY, true, true));
          }
        };
    NativeReconcileJobIndexStore drainedStore = new NativeReconcileJobIndexStore();
    drainedStore.bind(
        drainedBackend, payloadStore(), indexes, 4, (previous, current) -> {}, (a, b, op) -> {});

    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        drainedStore.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(canonicalKey, "inline:reconcile-job:e30", 7L));

    assertEquals(1, deleteBatch.writes().size());
    var delete = (ReconcileJobIndexStore.JobIndexDelete) deleteBatch.writes().getFirst();
    assertEquals(canonicalKey, delete.pointerKey());
    assertEquals(7L, delete.expectedVersion());
    assertTrue(delete.requireCleanupLock());
  }

  @Test
  void stateTransitionReplacesAllStateIndexesInOneBatch() {
    StoredReconcileJob previous = record("JS_SUCCEEDED");
    StoredReconcileJob current = record("JS_FAILED");
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);
    List<ReconcileJobIndexStore.JobIndexWriteOp> seed = new ArrayList<>();
    for (String stateKey : indexes.statePointerKeys(previous)) {
      seed.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              stateKey,
              0L,
              canonicalKey,
              ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_POINTER_KEY));
    }
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                seed, ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ReconcileJobIndexStore.JobIndexWriteBatch transition =
        store.buildJobIndexWriteBatch(
            new CanonicalPointerSnapshot(canonicalKey, "inline:reconcile-job:e30", 1L),
            previous,
            current);

    for (String oldStateKey : indexes.statePointerKeys(previous)) {
      assertTrue(
          transition.writes().stream()
              .anyMatch(
                  op ->
                      op instanceof ReconcileJobIndexStore.JobIndexDelete delete
                          && oldStateKey.equals(delete.pointerKey())));
    }
    for (String newStateKey : indexes.statePointerKeys(current)) {
      assertTrue(
          transition.writes().stream()
              .anyMatch(
                  op ->
                      op instanceof ReconcileJobIndexStore.JobIndexUpsert upsert
                          && newStateKey.equals(upsert.pointerKey())));
    }
  }

  @Test
  void chunkingPacksWholeJobsToOneHundredItemsAndSplitsOnlyBetweenJobs() {
    ReconcileJobIndexStore.JobWritePlan<String> sixty =
        new ReconcileJobIndexStore.JobWritePlan<>("a", batchWithDeletes("a", 60), List.of());
    ReconcileJobIndexStore.JobWritePlan<String> forty =
        new ReconcileJobIndexStore.JobWritePlan<>("b", batchWithDeletes("b", 40), List.of());
    ReconcileJobIndexStore.JobWritePlan<String> one =
        new ReconcileJobIndexStore.JobWritePlan<>("c", batchWithDeletes("c", 1), List.of());

    List<ReconcileJobIndexStore.JobWriteChunk<String>> chunks =
        store.chunkJobWritePlans(List.of(sixty, forty, one));

    assertEquals(2, chunks.size());
    assertEquals(List.of(sixty, forty), chunks.get(0).plans());
    assertEquals(100, store.writeItemCount(chunks.get(0).indexBatch(), List.of()));
    assertEquals(List.of(one), chunks.get(1).plans());
    assertEquals(1, store.writeItemCount(chunks.get(1).indexBatch(), List.of()));
    assertThrows(
        IllegalStateException.class,
        () ->
            store.chunkJobWritePlans(
                List.of(
                    new ReconcileJobIndexStore.JobWritePlan<>(
                        "oversized", batchWithDeletes("oversized", 101), List.of()))));
  }

  @Test
  void cleanupChunkingCountsPointerOpsAndPreservesWholeJobPlans() {
    ReconcileJobIndexStore.JobWritePlan<String> first =
        new ReconcileJobIndexStore.JobWritePlan<>(
            "job-a",
            batchWithDeletes("a", 60),
            java.util.stream.IntStream.range(0, 40)
                .mapToObj(
                    i -> (PointerStore.CasOp) new PointerStore.CasDelete("pointer-a-" + i, 1L))
                .toList());
    ReconcileJobIndexStore.JobWritePlan<String> second =
        new ReconcileJobIndexStore.JobWritePlan<>("job-b", batchWithDeletes("b", 1), List.of());

    List<ReconcileJobIndexStore.JobWriteChunk<String>> chunks =
        store.chunkJobWritePlans(List.of(first, second));

    assertEquals(2, chunks.size());
    assertEquals(List.of(first), chunks.get(0).plans());
    assertEquals(
        100, store.writeItemCount(chunks.get(0).indexBatch(), chunks.get(0).extraPointerOps()));
    assertEquals(List.of(second), chunks.get(1).plans());
    assertThrows(
        IllegalStateException.class,
        () ->
            store.chunkJobWritePlans(
                List.of(
                    new ReconcileJobIndexStore.JobWritePlan<>(
                        "oversized", batchWithDeletes("oversized", 101), List.of()))));
  }

  @Test
  void oversizedCleanupLocksCanonicalAndDeletesReferencesInBoundedPhases() {
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, "oversized-cleanup");
    List<String> referenceKeys = new ArrayList<>();
    List<ReconcileJobIndexStore.JobIndexWriteOp> seedWrites = new ArrayList<>();
    for (int i = 0; i < 125; i++) {
      String referenceKey =
          Keys.reconcileJobByParentPointer(ACCOUNT_ID, "cleanup-parent-" + i, "oversized-cleanup");
      referenceKeys.add(referenceKey);
      seedWrites.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              referenceKey,
              0L,
              canonicalKey,
              ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_POINTER_KEY));
    }
    seedWrites.add(
        0,
        new ReconcileJobIndexStore.JobIndexUpsert(
            canonicalKey,
            0L,
            "inline:reconcile-job:e30",
            ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_INLINE_JSON,
            new ReconcileJobIndexCleanupManifest(referenceKeys, List.of())));
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                seedWrites, ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    JobIndexEntrySnapshot canonical = backend.loadIndexEntry(canonicalKey).orElseThrow();
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        store.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
    ReconcileJobIndexStore.JobWritePlan<String> plan =
        new ReconcileJobIndexStore.JobWritePlan<>("oversized-cleanup", deleteBatch, List.of());
    List<ReconcileJobIndexStore.JobWriteChunk<String>> phases =
        store.chunkOversizedJobDeletePlan(plan);

    assertTrue(phases.size() > 1);
    assertTrue(
        phases.stream()
            .allMatch(
                phase -> store.writeItemCount(phase.indexBatch(), phase.extraPointerOps()) <= 100));
    assertFalse(
        backend.compareAndSetBatch(
            store.buildJobIndexWriteBatch(
                new CanonicalPointerSnapshot(
                    canonical.pointerKey(), canonical.blobUri(), canonical.version()),
                record("JS_SUCCEEDED"),
                record("JS_FAILED"))));
    assertTrue(
        backend.compareAndSetBatch(
            phases.getFirst().indexBatch(), phases.getFirst().extraPointerOps()));
    assertTrue(
        backend.compareAndSetBatch(
            phases.getFirst().indexBatch(), phases.getFirst().extraPointerOps()));
    for (int i = 1; i < phases.size(); i++) {
      assertTrue(
          backend.compareAndSetBatch(phases.get(i).indexBatch(), phases.get(i).extraPointerOps()));
      if (i + 1 < phases.size()) {
        assertTrue(backend.loadIndexEntry(canonicalKey).isPresent());
      }
    }
    assertTrue(backend.loadIndexEntry(canonicalKey).isEmpty());
    for (String referenceKey : referenceKeys) {
      assertTrue(backend.loadIndexEntry(referenceKey).isEmpty(), referenceKey);
    }
  }

  @Test
  void migrationBackfillInstallsMissingConnectorAndStateIndexes() {
    StoredReconcileJob legacy = record("JS_QUEUED");
    legacy.jobId = "legacy-backfill";
    legacy.connectorIndexPointerKey = "";
    legacy.laneKey = "lane-backfill";
    legacy.nextAttemptAtMs = 1_000L;
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, legacy.jobId);
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey,
                        0L,
                        payloadStore().encodeInlineJobState(legacy),
                        ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_INLINE_JSON,
                        ReconcileJobIndexCleanupManifest.EMPTY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ReconcileJobIndexStore.IndexBackfillResult first = store.backfillStoredJobIndexes(canonicalKey);

    assertTrue(first.updated());
    assertFalse(first.retryable());
    String connectorKey =
        indexes.connectorIndexPointerKey(
            ACCOUNT_ID, CONNECTOR_ID, legacy.createdAtMs, legacy.jobId);
    assertEquals(canonicalKey, backend.loadIndexEntry(connectorKey).orElseThrow().blobUri());
    for (String stateKey : indexes.statePointerKeys(legacy)) {
      assertEquals(canonicalKey, backend.loadIndexEntry(stateKey).orElseThrow().blobUri());
    }
    assertFalse(store.backfillStoredJobIndexes(canonicalKey).updated());
  }

  @Test
  void bulkEnqueueDoesNotCommitChildOnlyChunksBeforeAncestorOutcome() {
    String parentJobId = "parent-gate";
    String parentCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId);
    RejectAncestorBatchBackend rejectingBackend =
        new RejectAncestorBatchBackend(pointers, parentCanonicalKey);
    store.bind(
        rejectingBackend, payloadStore(), indexes, 4, (previous, current) -> {}, (a, b, op) -> {});
    List<ReconcileJobIndexStore.QueuedJobInsert> inserts = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      String jobId = "gated-child-" + i;
      StoredReconcileJob child = record("JS_QUEUED");
      child.jobId = jobId;
      child.parentJobId = parentJobId;
      child.dedupeKeyHash = "gated-hash-" + i;
      child.connectorIndexPointerKey =
          Keys.reconcileJobByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, "gated-child-token-" + i);
      inserts.add(
          new ReconcileJobIndexStore.QueuedJobInsert(
              i,
              Keys.reconcileDedupePointer(ACCOUNT_ID, child.dedupeKeyHash),
              Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
              Keys.reconcileJobLookupPointerById(jobId),
              Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId),
              List.of(),
              List.of(),
              child.connectorIndexPointerKey,
              "",
              child));
    }
    StoredReconcileJob previousParent = record("JS_RUNNING");
    previousParent.jobId = parentJobId;
    previousParent.parentJobId = "";
    previousParent.dedupeKeyHash = "parent-gate-hash";
    StoredReconcileJob waitingParent = store.cloneStoredRecord(previousParent);
    waitingParent.state = "JS_WAITING";
    ReconcileJobIndexStore.CanonicalRecordMutation parentOutcome =
        new ReconcileJobIndexStore.CanonicalRecordMutation(
            new CanonicalPointerSnapshot(parentCanonicalKey, "inline:reconcile-job:e30", 1L),
            previousParent,
            waitingParent);

    List<ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult> results =
        store.commitQueuedJobInserts(inserts, ignored -> List.of(parentOutcome));

    assertEquals(inserts.size(), results.size());
    assertTrue(rejectingBackend.ancestorRejections.get() > 0);
    for (ReconcileJobIndexStore.QueuedJobInsert insert : inserts) {
      assertFalse(rejectingBackend.loadIndexEntry(insert.canonicalKey()).isPresent());
    }
  }

  @Test
  void bulkCommitExceptionRollsBackOnlyDefinitelyUnattemptedChunks() {
    ThrowOnSecondBatchBackend throwingBackend = new ThrowOnSecondBatchBackend(pointers);
    store.bind(
        throwingBackend, payloadStore(), indexes, 4, (previous, current) -> {}, (a, b, op) -> {});
    List<ReconcileJobIndexStore.QueuedJobInsert> inserts = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      String jobId = "job-" + i;
      StoredReconcileJob record = record("JS_QUEUED");
      record.jobId = jobId;
      record.dedupeKeyHash = "hash-" + i;
      inserts.add(
          new ReconcileJobIndexStore.QueuedJobInsert(
              i,
              Keys.reconcileDedupePointer(ACCOUNT_ID, record.dedupeKeyHash),
              Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
              Keys.reconcileJobLookupPointerById(jobId),
              "",
              List.of(),
              List.of(),
              "",
              "",
              record));
    }

    ReconcileJobIndexStore.BulkEnqueueCommitException failure =
        assertThrows(
            ReconcileJobIndexStore.BulkEnqueueCommitException.class,
            () -> store.commitQueuedJobInserts(inserts, ignored -> List.of()));

    Set<Integer> rollbackIndexes = new HashSet<>(failure.rollbackIndexes());
    assertTrue(java.util.Collections.disjoint(throwingBackend.committedIndexes, rollbackIndexes));
    assertTrue(
        java.util.Collections.disjoint(throwingBackend.indeterminateIndexes, rollbackIndexes));
    assertTrue(
        java.util.Collections.disjoint(
            throwingBackend.committedIndexes, throwingBackend.indeterminateIndexes));
    assertEquals(
        inserts.size(),
        throwingBackend.committedIndexes.size()
            + throwingBackend.indeterminateIndexes.size()
            + rollbackIndexes.size());
    assertEquals(2, throwingBackend.calls.get());
  }

  private ReconcilePayloadStore payloadStore() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), pointers, new ObjectMapper());
    return payloadStore;
  }

  private StoredReconcileJob record(String state) {
    StoredReconcileJob record = new StoredReconcileJob();
    record.accountId = ACCOUNT_ID;
    record.connectorId = CONNECTOR_ID;
    record.jobId = JOB_ID;
    record.parentJobId = "parent-1";
    record.state = state;
    record.createdAtMs = 100L;
    record.updatedAtMs = 100L;
    record.dedupeKeyHash = "hash-1";
    record.connectorIndexPointerKey =
        Keys.reconcileJobByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, "token-1");
    return record;
  }

  private static ReconcileJobIndexStore.JobIndexWriteBatch batchWithDeletes(
      String prefix, int count) {
    List<ReconcileJobIndexStore.JobIndexWriteOp> writes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      writes.add(new ReconcileJobIndexStore.JobIndexDelete(prefix + "-" + i, 1L));
    }
    return new ReconcileJobIndexStore.JobIndexWriteBatch(
        writes, ReconcileJobIndexStore.ReadyQueueMutation.empty());
  }

  private static final class ThrowOnSecondBatchBackend extends MemoryReconcileJobIndexBackend {
    private final AtomicInteger calls = new AtomicInteger();
    private final Set<Integer> committedIndexes = new HashSet<>();
    private final Set<Integer> indeterminateIndexes = new HashSet<>();

    private ThrowOnSecondBatchBackend(PointerStore pointerStore) {
      super(pointerStore);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      int call = calls.incrementAndGet();
      Set<Integer> batchIndexes = new HashSet<>();
      for (var write : batch.writes()) {
        if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
          var canonical = JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey());
          if (canonical != null) {
            batchIndexes.add(Integer.parseInt(canonical.jobSegment().substring("job-".length())));
          }
        }
      }
      if (call == 2) {
        indeterminateIndexes.addAll(batchIndexes);
        throw new RuntimeException("injected second chunk failure");
      }
      committedIndexes.addAll(batchIndexes);
      return super.compareAndSetBatch(batch);
    }
  }

  private static final class RejectAncestorBatchBackend extends MemoryReconcileJobIndexBackend {
    private final String ancestorCanonicalKey;
    private final AtomicInteger ancestorRejections = new AtomicInteger();

    private RejectAncestorBatchBackend(PointerStore pointerStore, String ancestorCanonicalKey) {
      super(pointerStore);
      this.ancestorCanonicalKey = ancestorCanonicalKey;
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      boolean containsAncestor =
          batch.writes().stream()
              .anyMatch(
                  write ->
                      write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert
                          && ancestorCanonicalKey.equals(upsert.pointerKey()));
      if (containsAncestor) {
        ancestorRejections.incrementAndGet();
        return false;
      }
      return super.compareAndSetBatch(batch);
    }
  }
}
