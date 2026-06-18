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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore.LeaseScanStats;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the lease-scan guards: the wall-clock budget (so a scan a caller has already
 * abandoned yields its concurrency slot instead of running to completion) and inline pruning of
 * stale ready pointers (so leaked pointers drain instead of starving leasable work behind them).
 */
class NativeReconcileReadyQueueStoreBudgetTest {

  @Test
  void scanAbortsImmediatelyWhenBudgetAlreadyExpired() {
    CountingBackend backend = new CountingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats();
    stats.deadlineAtMs = System.currentTimeMillis() - 1; // already past

    long now = System.currentTimeMillis();
    Optional<LeasedJob> leased = store.leaseReadyDue(now, LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty(), "expired-budget scan must yield no work");
    assertTrue(stats.abortedByBudget, "scan must record that it aborted on the budget");
    assertEquals(
        0, backend.scanReadySliceCalls, "scan must short-circuit before issuing any DynamoDB scan");
  }

  @Test
  void scanProceedsWhenNoBudgetSet() {
    CountingBackend backend = new CountingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats(); // deadlineAtMs == 0 -> no budget

    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty(), "empty ready queue yields no work");
    assertFalse(stats.abortedByBudget, "no budget means no budget-abort");
    assertTrue(
        backend.scanReadySliceCalls >= 1, "scan must reach the backend when not budget-gated");
  }

  @Test
  void scanPrunesOrphanedReadyPointer() {
    PruningBackend backend = new PruningBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty(), "an orphaned pointer cannot be leased");
    assertEquals(1, stats.prunedCount, "the orphaned (canonical-missing) pointer must be pruned");
    assertEquals(
        List.of("rp-orphan"),
        backend.deleted,
        "deleteReadyEntry called with the stale pointer key");
  }

  /** Records ready-scan calls and returns an empty page so the scan terminates cleanly. */
  private static final class CountingBackend implements ReconcileReadyQueueBackend {
    int scanReadySliceCalls;

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice, int pageSize, String pageToken) {
      scanReadySliceCalls++;
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      throw new UnsupportedOperationException("not used in this test");
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      throw new UnsupportedOperationException("not used in this test");
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
      throw new UnsupportedOperationException("not used in this test");
    }
  }

  /** Serves one due candidate whose canonical snapshot is missing, and records prune deletes. */
  private static final class PruningBackend implements ReconcileReadyQueueBackend {
    final List<String> deleted = new ArrayList<>();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice, int pageSize, String pageToken) {
      ReconcileReadyQueueStore.ReadyQueueEntry orphan =
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              "rp-orphan",
              "cp-orphan",
              "acct",
              "job",
              1L,
              ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
              "");
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(orphan), "");
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      throw new UnsupportedOperationException("not used in this test");
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      deleted.add(readyPointerKey);
      return true;
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
      return Optional.empty();
    }
  }
}
