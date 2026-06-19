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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore.LeaseScanStats;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NativeReconcileReadyQueueStoreBudgetTest {

  @Test
  void scanAbortsBeforeBackendCallsWhenDeadlineAlreadyExpired() {
    CountingBackend backend = new CountingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats();
    stats.deadlineAtMs = System.currentTimeMillis() - 1L;

    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertTrue(stats.abortedByDeadline);
    assertEquals(0, backend.scanReadySliceCalls);
    assertEquals(0, backend.loadCanonicalSnapshotCalls);
  }

  @Test
  void scanReachesBackendWhenNoDeadlineIsConfigured() {
    CountingBackend backend = new CountingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertFalse(stats.abortedByDeadline);
    assertTrue(backend.scanReadySliceCalls >= 1);
  }

  @Test
  void scanPrunesOrphanedReadyPointer() {
    PruningBackend backend = new PruningBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true);

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(1, stats.prunedCount);
    assertEquals(List.of("rp-orphan"), backend.deleted);
  }

  @Test
  void scanStopsBeforeLeaseWriteWhenCallerCancelsAfterRecordDecode() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    ReconcileReadyQueueBackend backend = mock(ReconcileReadyQueueBackend.class);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileLeaseStore leaseStore = mock(ReconcileLeaseStore.class);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    store.bind(backend, jobIndexStore, leaseStore, 128, record -> true);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob record = new StoredReconcileJob();
    record.jobId = "job-1";
    record.accountId = "acct-1";
    record.jobKind = ReconcileJobKind.PLAN_CONNECTOR.name();
    record.state = "JS_QUEUED";
    record.executionClass = "DEFAULT";
    record.executionLane = "default";
    record.laneKey = "default";
    record.nextAttemptAtMs = dueAtMs;
    record.readyPointerKey = store.readyPointerKeyFor(record, dueAtMs);

    ReconcileReadyQueueStore.ReadyQueueEntry candidate =
        new ReconcileReadyQueueStore.ReadyQueueEntry(
            record.readyPointerKey,
            "/canonical",
            "acct-1",
            "job-1",
            dueAtMs,
            ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
            "");
    when(backend.scanReadySlice(any(), eq(128), eq(""), any()))
        .thenReturn(new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(candidate), ""));
    CanonicalPointerSnapshot snapshot =
        new CanonicalPointerSnapshot("/canonical", "blob://job", 7L);
    when(backend.loadCanonicalSnapshot(eq("/canonical"), any())).thenReturn(Optional.of(snapshot));
    when(jobIndexStore.readRecord(snapshot))
        .thenAnswer(
            ignored -> {
              cancelled.set(true);
              return Optional.of(record);
            });

    LeaseScanStats stats = new LeaseScanStats();
    stats.cancelled = cancelled::get;

    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertTrue(stats.abortedByCaller);
    verifyNoInteractions(leaseStore);
  }

  private static final class CountingBackend implements ReconcileReadyQueueBackend {
    int scanReadySliceCalls;
    int loadCanonicalSnapshotCalls;

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      scanReadySliceCalls++;
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
        String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      loadCanonicalSnapshotCalls++;
      return Optional.empty();
    }
  }

  private static final class PruningBackend implements ReconcileReadyQueueBackend {
    final List<String> deleted = new ArrayList<>();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
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
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      deleted.add(readyPointerKey);
      return true;
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
        String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      return Optional.empty();
    }
  }
}
