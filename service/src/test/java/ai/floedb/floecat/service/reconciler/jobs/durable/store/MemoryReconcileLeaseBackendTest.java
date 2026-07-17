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

class MemoryReconcileLeaseBackendTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String JOB_ID = "job-1";
  private static final String CANONICAL_KEY = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);

  @Test
  void leaseTransactionUpdatesCanonicalCleanupManifestThroughJobIndexBackend() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend jobIndexBackend = new MemoryReconcileJobIndexBackend(pointers);
    MemoryReconcileLeaseBackend leaseBackend =
        new MemoryReconcileLeaseBackend(pointers, jobIndexBackend);
    ReconcileJobIndexCleanupManifest manifest =
        new ReconcileJobIndexCleanupManifest(List.of("index-1"), List.of("ready-1"));

    boolean committed =
        leaseBackend.compareAndSetBatch(
            canonicalUpsert(0L, manifest), ReconcileLeaseBackend.LeaseWriteBatch.empty());

    assertTrue(committed);
    assertEquals(manifest, jobIndexBackend.loadCleanupManifest(CANONICAL_KEY));
  }

  @Test
  void failedLeaseTransactionDoesNotPublishCanonicalCleanupManifest() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend jobIndexBackend = new MemoryReconcileJobIndexBackend(pointers);
    MemoryReconcileLeaseBackend leaseBackend =
        new MemoryReconcileLeaseBackend(pointers, jobIndexBackend);
    ReconcileJobIndexCleanupManifest manifest =
        new ReconcileJobIndexCleanupManifest(List.of("index-1"), List.of("ready-1"));

    boolean committed =
        leaseBackend.compareAndSetBatch(
            canonicalUpsert(0L, manifest),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseRecordUpsert(
                        ACCOUNT_ID, JOB_ID, 1L, "inline:lease:e30"))));

    assertFalse(committed);
    assertTrue(jobIndexBackend.loadIndexEntry(CANONICAL_KEY).isEmpty());
    assertTrue(jobIndexBackend.loadCleanupManifest(CANONICAL_KEY).isEmpty());
  }

  private static ReconcileJobIndexStore.JobIndexWriteBatch canonicalUpsert(
      long expectedVersion, ReconcileJobIndexCleanupManifest manifest) {
    return new ReconcileJobIndexStore.JobIndexWriteBatch(
        List.of(
            new ReconcileJobIndexStore.JobIndexUpsert(
                CANONICAL_KEY,
                expectedVersion,
                "inline:reconcile-job:e30",
                PointerReferenceKind.PRK_INLINE_JSON,
                manifest)),
        ReconcileJobIndexStore.ReadyQueueMutation.empty());
  }
}
