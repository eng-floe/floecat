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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeReconcileJobIndexStorePointerSetTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "connector-1";
  private static final String JOB_ID = "job-1";

  private MemoryReconcileJobIndexBackend backend;
  private ReconcileJobIndexes indexes;
  private NativeReconcileJobIndexStore store;

  @BeforeEach
  void setUp() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
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
    String connectorKey = Keys.reconcileJobByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, "token-1");
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
                connectorKey,
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
    ReconcileJobIndexStore.JobIndexWriteBatch sixty = batchWithDeletes("a", 60);
    ReconcileJobIndexStore.JobIndexWriteBatch forty = batchWithDeletes("b", 40);
    ReconcileJobIndexStore.JobIndexWriteBatch one = batchWithDeletes("c", 1);

    List<ReconcileJobIndexStore.JobIndexWriteBatch> chunks =
        store.chunkJobWriteBatches(List.of(sixty, forty, one));

    assertEquals(2, chunks.size());
    assertEquals(100, NativeReconcileJobIndexStore.physicalWriteItemCount(chunks.get(0)));
    assertEquals(1, NativeReconcileJobIndexStore.physicalWriteItemCount(chunks.get(1)));
    assertThrows(
        IllegalStateException.class,
        () -> store.chunkJobWriteBatches(List.of(batchWithDeletes("oversized", 101))));
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
}
