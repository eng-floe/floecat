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
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        store.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
    assertEquals(1 + expectedIndexes.size(), deleteBatch.writes().size());
    assertTrue(backend.compareAndSetBatch(deleteBatch));
    assertTrue(backend.loadIndexEntry(canonicalKey).isEmpty());
    for (String pointerKey : expectedIndexes) {
      assertTrue(backend.loadIndexEntry(pointerKey).isEmpty(), pointerKey);
    }
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
}
