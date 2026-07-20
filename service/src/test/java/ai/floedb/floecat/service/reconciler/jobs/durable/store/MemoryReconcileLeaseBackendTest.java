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
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemoryReconcileLeaseBackendTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String JOB_ID = "job-1";
  private static final String CANONICAL_KEY = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);

  @Test
  void leaseRecordKeyPreservesAndParsesLegacyRawSegments() {
    String pointerKey = LeaseBackendSupport.leasePointerKey("acct+legacy", "job%legacy");

    assertEquals("/accounts/acct+legacy/reconcile/job-leases/by-id/job%legacy", pointerKey);
    var parsed = LeaseBackendSupport.parseLeasePointerKey(pointerKey);
    assertEquals("acct+legacy", parsed.accountSegment());
    assertEquals("job%legacy", parsed.jobSegment());
  }

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

  @Test
  void leaseRecordConditionIsIncludedInAtomicPointerBatch() {
    RecordingPointerStore pointers = new RecordingPointerStore();
    String leaseKey = LeaseBackendSupport.leasePointerKey(ACCOUNT_ID, JOB_ID);
    assertTrue(
        pointers.compareAndSet(
            leaseKey, 0L, PointerReferences.inlineJsonPointer(leaseKey, "inline:lease:e30", 1L)));
    MemoryReconcileJobIndexBackend jobIndexBackend = new MemoryReconcileJobIndexBackend(pointers);
    MemoryReconcileLeaseBackend leaseBackend =
        new MemoryReconcileLeaseBackend(pointers, jobIndexBackend);

    assertTrue(
        leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseRecordCondition(ACCOUNT_ID, JOB_ID, 1L),
                    new ReconcileLeaseBackend.LeaseOwnerUpsert(
                        "/lease-owner/test", 0L, CANONICAL_KEY)))));

    assertTrue(
        pointers.lastOps.stream()
            .anyMatch(
                op ->
                    op instanceof PointerStore.CasCheck check
                        && leaseKey.equals(check.key())
                        && check.expectedVersion() == 1L));
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

  private static final class RecordingPointerStore extends InMemoryPointerStore {
    private List<PointerStore.CasOp> lastOps = List.of();

    @Override
    public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
      lastOps = List.copyOf(ops);
      return super.compareAndSetBatch(ops);
    }
  }
}
