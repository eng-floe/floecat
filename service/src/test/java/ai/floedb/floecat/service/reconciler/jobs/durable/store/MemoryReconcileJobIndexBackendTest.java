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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemoryReconcileJobIndexBackendTest {

  @Test
  void accountDeletionFenceRejectsUpsertsButAllowsCleanupDeletes() {
    String accountId = "acct-1";
    String canonicalKey = Keys.reconcileJobPointerById(accountId, "job-1");
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend backend = new MemoryReconcileJobIndexBackend(pointers);
    var create =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexUpsert(
                    canonicalKey,
                    0L,
                    "inline:reconcile-job:e30",
                    PointerReferenceKind.PRK_INLINE_JSON,
                    new ReconcileJobIndexCleanupManifest(
                        List.of(Keys.reconcileJobLookupPointerById("job-1")), List.of()))),
            ReconcileJobIndexStore.ReadyQueueMutation.empty());
    assertTrue(backend.compareAndSetBatch(create));
    String fence = Keys.accountDeletionMarker(accountId);
    List<PointerStore.CasOp> fenceCreates = new ArrayList<>();
    fenceCreates.add(
        new PointerStore.CasUpsert(
            fence, 0L, PointerReferences.opaqueMarkerPointer(fence, "deleting", 1L)));
    for (String shardKey : Keys.accountDeletionFenceShards(accountId)) {
      fenceCreates.add(
          new PointerStore.CasUpsert(
              shardKey, 0L, PointerReferences.opaqueMarkerPointer(shardKey, "deleting", 1L)));
    }
    assertTrue(pointers.compareAndSetBatch(fenceCreates));

    assertFalse(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobPointerById(accountId, "job-2"),
                        0L,
                        "inline:reconcile-job:e30",
                        PointerReferenceKind.PRK_INLINE_JSON)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(new ReconcileJobIndexStore.JobIndexDelete(canonicalKey, 1L)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
  }

  @Test
  void clearInMemoryStateDropsCleanupMetadata() {
    String canonicalKey = Keys.reconcileJobPointerById("acct", "job");
    String lookupKey = Keys.reconcileJobLookupPointerById("job");
    MemoryReconcileJobIndexBackend backend =
        new MemoryReconcileJobIndexBackend(new InMemoryPointerStore());
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey,
                        0L,
                        "inline:reconcile-job:e30",
                        PointerReferenceKind.PRK_INLINE_JSON,
                        new ReconcileJobIndexCleanupManifest(List.of(lookupKey), List.of()))),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    backend.clearInMemoryState();

    assertTrue(backend.loadCleanupManifest(canonicalKey).isEmpty());
  }

  @Test
  void rejectsTransactionOverDynamoPhysicalItemLimit() {
    MemoryReconcileJobIndexBackend backend =
        new MemoryReconcileJobIndexBackend(new InMemoryPointerStore());
    List<ReconcileJobIndexStore.JobIndexWriteOp> writes = new ArrayList<>();
    for (int index = 0; index < 101; index++) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexCheckAbsent(
              Keys.reconcileJobLookupPointerById("job-" + index)));
    }
    var batch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.copyOf(writes), ReconcileJobIndexStore.ReadyQueueMutation.empty());

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> backend.compareAndSetBatch(batch));

    assertEquals("DynamoDB transaction exceeds 100 items: 101", thrown.getMessage());
  }

  @Test
  void rejectsVersionCheckForNonCanonicalKey() {
    String lookupKey = Keys.reconcileJobLookupPointerById("job-1");
    MemoryReconcileJobIndexBackend backend =
        new MemoryReconcileJobIndexBackend(new InMemoryPointerStore());
    var batch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(new ReconcileJobIndexStore.JobIndexCheck(lookupKey, 1L, false)),
            ReconcileJobIndexStore.ReadyQueueMutation.empty());

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> backend.compareAndSetBatch(batch));

    assertEquals(
        "Unsupported reconcile job index version check key: " + lookupKey, thrown.getMessage());
  }

  @Test
  void canonicalLoadAndListExposeAcquiredCleanupLock() {
    String accountId = "acct-1";
    String jobId = "job-1";
    String canonicalKey = Keys.reconcileJobPointerById(accountId, jobId);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    String blob = "inline:reconcile-job:e30";
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend backend = new MemoryReconcileJobIndexBackend(pointers);
    var manifest = new ReconcileJobIndexCleanupManifest(List.of(lookupKey), List.of());
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey, 0L, blob, PointerReferenceKind.PRK_INLINE_JSON, manifest)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    JobIndexEntrySnapshot before = backend.loadIndexEntry(canonicalKey).orElseThrow();
    assertFalse(before.cleanupLocked());

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(before.pointerKey(), before.blobUri(), before.version()),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isPresent());
    assertEquals(before.version() + 1L, session.orElseThrow().snapshot().version());
    JobIndexEntrySnapshot locked = backend.loadIndexEntry(canonicalKey).orElseThrow();
    assertEquals(before.version() + 1L, locked.version());
    assertTrue(locked.cleanupLocked());
    var listed = backend.listCanonicalEntries(accountId, 10, "");
    assertEquals(1, listed.entries().size());
    assertTrue(listed.entries().getFirst().cleanupLocked());
  }
}
