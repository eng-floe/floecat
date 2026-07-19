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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemoryReconcileJobIndexBackendTest {

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
    assertTrue(backend.loadIndexEntry(canonicalKey).orElseThrow().cleanupLocked());
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

    var quiet = new ReconcileJobIndexBackend.LegacyMigrationProgress("", 0, 0, 0, 0, true);
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
}
