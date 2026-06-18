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
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the ready-scan wall-clock budget. The budget lets a lease scan a caller has
 * already abandoned (past the worker-control client deadline) stop and yield its concurrency slot
 * instead of running to completion, which is what prevents abandoned scans from piling up.
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
}
