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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemoryReconcileJobIndexBackendTest {

  @Test
  void rejectsTransactionOverDynamoPhysicalItemLimit() {
    MemoryReconcileJobIndexBackend backend =
        new MemoryReconcileJobIndexBackend(new InMemoryPointerStore());
    List<ReconcileJobIndexStore.JobIndexWriteOp> writes = new ArrayList<>();
    for (int index = 0; index < 51; index++) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexCheckAbsent(
              Keys.reconcileJobLookupPointerById("job-" + index)));
    }
    var batch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.copyOf(writes), ReconcileJobIndexStore.ReadyQueueMutation.empty());

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> backend.compareAndSetBatch(batch));

    assertEquals("DynamoDB transaction exceeds 100 items: 102", thrown.getMessage());
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

  @Test
  void legacyMigrationCheckpointSurvivesLeaseTakeoverAndFencesOldOwner() {
    MemoryReconcileJobIndexBackend backend = new MemoryReconcileJobIndexBackend();
    assertFalse(backend.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.CLEANUP));
    var first =
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "first", 1_000L, 100L)
            .orElseThrow();
    assertTrue(
        backend.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "first",
            first.fence(),
            new ReconcileJobIndexBackend.LegacyMigrationProgress("page-2", 3, 0, 0, 0, false),
            1_050L,
            100L));

    assertTrue(
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "second", 1_149L, 100L)
            .isEmpty());
    var replacement =
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "second", 1_151L, 100L)
            .orElseThrow();
    assertEquals("page-2", replacement.progress().pageToken());
    assertEquals(3, replacement.progress().changed());
    assertFalse(
        backend.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "first",
            first.fence(),
            ReconcileJobIndexBackend.LegacyMigrationProgress.empty(),
            1_151L,
            100L));
    assertFalse(
        backend.completeLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "second",
            replacement.fence(),
            1_151L));

    var quiet = new ReconcileJobIndexBackend.LegacyMigrationProgress("", 0, 4, 5, 0, true);
    assertTrue(
        backend.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "second",
            replacement.fence(),
            quiet,
            1_151L,
            100L));
    assertTrue(
        backend.completeLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "second",
            replacement.fence(),
            1_151L));
    assertTrue(backend.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.CLEANUP));
    assertTrue(
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "third", 1_300L, 100L)
            .isEmpty());
  }

  @Test
  void legacyCleanupDiscoveryExcludesLeasePointers() {
    String accountId = "acct-1";
    String jobId = "job-1";
    String canonicalKey = Keys.reconcileJobPointerById(accountId, jobId);
    String parentKey = Keys.reconcileJobByParentPointer(accountId, "parent-1", jobId);
    String leaseExpiryKey = Keys.reconcileJobLeaseExpiryPointer(1_000L, accountId, jobId);
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    assertTrue(
        pointers.compareAndSet(
            parentKey, 0L, PointerReferences.pointerKeyPointer(parentKey, canonicalKey, 1L)));
    assertTrue(
        pointers.compareAndSet(
            leaseExpiryKey,
            0L,
            PointerReferences.pointerKeyPointer(leaseExpiryKey, canonicalKey, 1L)));
    MemoryReconcileJobIndexBackend backend = new MemoryReconcileJobIndexBackend(pointers);

    ReconcileJobIndexCleanupManifest discovered =
        backend.discoverLegacyCleanupManifest(canonicalKey);

    assertTrue(discovered.indexPointerKeys().contains(parentKey));
    assertFalse(discovered.indexPointerKeys().contains(leaseExpiryKey));
  }

  @Test
  void beginCleanupRepairsInvalidStoredManifestFromOwnedReferences() {
    String accountId = "acct-1";
    String jobId = "job-1";
    String canonicalKey = Keys.reconcileJobPointerById(accountId, jobId);
    String parentKey = Keys.reconcileJobByParentPointer(accountId, "parent-1", jobId);
    String readyKey = Keys.reconcileReadyPointerByDue(1_000L, accountId, "lane-1", jobId);
    String invalidIndexKey = Keys.reconcileJobLeaseExpiryPointer(1_000L, accountId, jobId);
    String projectionKey = Keys.reconcileJobProjectionPointer(accountId, jobId);
    String blob = "inline:reconcile-job:e30";
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend backend = new MemoryReconcileJobIndexBackend(pointers);
    var malformed =
        new ReconcileJobIndexCleanupManifest(
            List.of(invalidIndexKey), List.of(), List.of(projectionKey));
    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey, 0L, blob, PointerReferenceKind.PRK_INLINE_JSON, malformed)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    assertTrue(
        pointers.compareAndSet(
            parentKey, 0L, PointerReferences.pointerKeyPointer(parentKey, canonicalKey, 1L)));
    assertTrue(
        pointers.compareAndSet(
            readyKey, 0L, PointerReferences.pointerKeyPointer(readyKey, canonicalKey, 1L)));
    JobIndexEntrySnapshot canonical = backend.loadIndexEntry(canonicalKey).orElseThrow();
    var fallback =
        new ReconcileJobIndexCleanupManifest(List.of(), List.of(), List.of(projectionKey));

    ReconcileJobIndexBackend.JobCleanupSession session =
        backend
            .beginJobCleanup(
                new CanonicalPointerSnapshot(
                    canonical.pointerKey(), canonical.blobUri(), canonical.version()),
                fallback)
            .orElseThrow();

    assertTrue(session.manifest().indexPointerKeys().contains(parentKey));
    assertTrue(session.manifest().readyPointerKeys().contains(readyKey));
    assertTrue(session.manifest().pointerKeys().contains(projectionKey));
    assertFalse(session.manifest().indexPointerKeys().contains(invalidIndexKey));
    assertTrue(backend.loadIndexEntry(canonicalKey).orElseThrow().cleanupLocked());
  }
}
