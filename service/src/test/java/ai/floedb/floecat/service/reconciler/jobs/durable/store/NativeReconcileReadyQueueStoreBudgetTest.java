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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NativeReconcileReadyQueueStoreBudgetTest {

  @Test
  void scanAbortsBeforeBackendCallsWhenDeadlineAlreadyExpired() {
    CountingBackend backend = new CountingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);

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
    store.bind(backend, null, null, 128, record -> true, record -> false);

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
    store.bind(backend, null, null, 128, record -> true, record -> false);

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(List.of("rp-orphan"), backend.deleted);
    assertEquals(1, stats.stalePointerSkipCount);
  }

  @Test
  void specializedJobKindRequestBoundsPinnedExecutorBucketBeforeKindIndex() {
    SliceRecordingBackend backend = new SliceRecordingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);

    Optional<LeasedJob> leased =
        store.leaseReadyDue(
            System.currentTimeMillis(),
            LeaseRequest.of(
                Set.of(),
                Set.of(),
                Set.of("floescan_ingest"),
                Set.of(ReconcileJobKind.EXEC_FILE_GROUP)),
            new LeaseScanStats());

    assertTrue(leased.isEmpty());
    assertEquals(2, backend.slices.size());
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, backend.slices.get(0).indexType());
    assertEquals("floescan_ingest", backend.slices.get(0).filterValue());
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, backend.slices.get(1).indexType());
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP.name(), backend.slices.get(1).filterValue());
    assertEquals(List.of(16, 16), backend.pageSizes);
  }

  @Test
  void wildcardLaneJobKindRequestUsesOnePinnedBucketThenOneJobKindBucket() {
    SliceRecordingBackend backend = new SliceRecordingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);

    Optional<LeasedJob> leased =
        store.leaseReadyDue(
            System.currentTimeMillis(),
            LeaseRequest.of(
                Set.of(),
                Set.of(LeaseRequest.anyLaneToken()),
                new java.util.LinkedHashSet<>(
                    List.of("remote_default_worker", "remote_planner_worker")),
                Set.of(ReconcileJobKind.PLAN_CONNECTOR, ReconcileJobKind.PLAN_TABLE)),
            new LeaseScanStats());

    assertTrue(leased.isEmpty());
    assertEquals(2, backend.slices.size());
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, backend.slices.get(0).indexType());
    assertTrue(
        Set.of("remote_default_worker", "remote_planner_worker")
            .contains(backend.slices.get(0).filterValue()));
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, backend.slices.get(1).indexType());
    assertEquals(ReconcileJobKind.PLAN_CONNECTOR.name(), backend.slices.get(1).filterValue());
    assertEquals(List.of(16, 16), backend.pageSizes);
  }

  @Test
  void wildcardLaneJobKindRequestRoundRobinsUnpinnedKindBuckets() {
    SliceRecordingBackend backend = new SliceRecordingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);
    LeaseRequest request =
        LeaseRequest.of(
            Set.of(),
            Set.of(LeaseRequest.anyLaneToken()),
            Set.of(),
            Set.of(ReconcileJobKind.PLAN_CONNECTOR, ReconcileJobKind.PLAN_TABLE));

    assertTrue(
        store.leaseReadyDue(System.currentTimeMillis(), request, new LeaseScanStats()).isEmpty());
    assertTrue(
        store.leaseReadyDue(System.currentTimeMillis(), request, new LeaseScanStats()).isEmpty());

    assertEquals(2, backend.slices.size());
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, backend.slices.get(0).indexType());
    assertEquals(ReconcileJobKind.PLAN_CONNECTOR.name(), backend.slices.get(0).filterValue());
    assertEquals(
        ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, backend.slices.get(1).indexType());
    assertEquals(ReconcileJobKind.PLAN_TABLE.name(), backend.slices.get(1).filterValue());
  }

  @Test
  void scanReadsOnlyOnePagePerReadySelection() {
    PagingBackend backend = new PagingBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(1, backend.scanReadySliceCalls);
    assertEquals(List.of("rp-page-1"), backend.deleted);
  }

  @Test
  void failedLeaseClassifiesRunningRaceWithoutPruningReadyPointer() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    ReconcileReadyQueueBackend backend = mock(ReconcileReadyQueueBackend.class);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileLeaseStore leaseStore = mock(ReconcileLeaseStore.class);
    store.bind(backend, jobIndexStore, leaseStore, 128, record -> true, record -> false);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob queued = queuedRecord("job-1", dueAtMs, store);
    StoredReconcileJob running = queuedRecord("job-1", dueAtMs, store);
    running.state = "JS_RUNNING";
    ReconcileReadyQueueStore.ReadyQueueEntry candidate = candidateFor(queued, "/canonical", store);
    CanonicalPointerSnapshot snapshot =
        new CanonicalPointerSnapshot("/canonical", "blob://job", 7L);

    when(backend.scanReadySlice(any(), eq(16), eq(""), any()))
        .thenReturn(new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(candidate), ""));
    when(backend.loadCanonicalSnapshot(eq("/canonical"), any())).thenReturn(Optional.of(snapshot));
    when(jobIndexStore.readRecord(snapshot)).thenReturn(Optional.of(queued), Optional.of(running));
    when(leaseStore.leaseCanonical(
            eq("/canonical"),
            eq(queued.readyPointerKey),
            anyLong(),
            eq(snapshot),
            eq(queued),
            any()))
        .thenReturn(Optional.empty());

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(1, stats.leaseRaceRunningSkipCount);
    verify(backend, org.mockito.Mockito.never()).deleteReadyEntry(queued.readyPointerKey);
  }

  @Test
  void failedLeaseClassifiesTerminalRaceAndPrunesReadyPointer() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    ReconcileReadyQueueBackend backend = mock(ReconcileReadyQueueBackend.class);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileLeaseStore leaseStore = mock(ReconcileLeaseStore.class);
    store.bind(backend, jobIndexStore, leaseStore, 128, record -> true, record -> false);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob queued = queuedRecord("job-1", dueAtMs, store);
    StoredReconcileJob terminal = queuedRecord("job-1", dueAtMs, store);
    terminal.state = "JS_SUCCEEDED";
    ReconcileReadyQueueStore.ReadyQueueEntry candidate = candidateFor(queued, "/canonical", store);
    CanonicalPointerSnapshot snapshot =
        new CanonicalPointerSnapshot("/canonical", "blob://job", 7L);

    when(backend.scanReadySlice(any(), eq(16), eq(""), any()))
        .thenReturn(new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(candidate), ""));
    when(backend.loadCanonicalSnapshot(eq("/canonical"), any())).thenReturn(Optional.of(snapshot));
    when(jobIndexStore.readRecord(snapshot)).thenReturn(Optional.of(queued), Optional.of(terminal));
    when(leaseStore.leaseCanonical(
            eq("/canonical"),
            eq(queued.readyPointerKey),
            anyLong(),
            eq(snapshot),
            eq(queued),
            any()))
        .thenReturn(Optional.empty());

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(1, stats.leaseRaceTerminalSkipCount);
    verify(backend).deleteReadyEntry(queued.readyPointerKey);
  }

  @Test
  void failedLeaseUsesLeaseAttemptFailureReasonWhenReadyEntryStillMatches() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    ReconcileReadyQueueBackend backend = mock(ReconcileReadyQueueBackend.class);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileLeaseStore leaseStore = mock(ReconcileLeaseStore.class);
    store.bind(backend, jobIndexStore, leaseStore, 128, record -> true, record -> false);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob queued = queuedRecord("job-1", dueAtMs, store);
    ReconcileReadyQueueStore.ReadyQueueEntry candidate = candidateFor(queued, "/canonical", store);
    CanonicalPointerSnapshot snapshot =
        new CanonicalPointerSnapshot("/canonical", "blob://job", 7L);

    when(backend.scanReadySlice(any(), eq(16), eq(""), any()))
        .thenReturn(new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(candidate), ""));
    when(backend.loadCanonicalSnapshot(eq("/canonical"), any())).thenReturn(Optional.of(snapshot));
    when(jobIndexStore.readRecord(snapshot)).thenReturn(Optional.of(queued), Optional.of(queued));
    when(leaseStore.leaseCanonical(
            eq("/canonical"),
            eq(queued.readyPointerKey),
            anyLong(),
            eq(snapshot),
            eq(queued),
            any()))
        .thenAnswer(
            invocation -> {
              ReconcileLeaseStore.LeaseAttemptStats stats = invocation.getArgument(5);
              stats.recordFailure("cas_conflict");
              return Optional.empty();
            });

    LeaseScanStats stats = new LeaseScanStats();
    Optional<LeasedJob> leased =
        store.leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), stats);

    assertTrue(leased.isEmpty());
    assertEquals(1, stats.skipCounts().get("lease_conflict_cas_conflict"));
    verify(backend, org.mockito.Mockito.never()).deleteReadyEntry(queued.readyPointerKey);
  }

  @Test
  void scanCursorAdvancesAfterUnleasedPage() {
    CursorBackend backend = new CursorBackend();
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(backend, null, null, 128, record -> true, record -> false);

    assertTrue(
        store
            .leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), new LeaseScanStats())
            .isEmpty());
    assertTrue(
        store
            .leaseReadyDue(System.currentTimeMillis(), LeaseRequest.all(), new LeaseScanStats())
            .isEmpty());

    assertEquals(List.of("", "next-page"), backend.pageTokens);
  }

  @Test
  void scanStopsBeforeLeaseWriteWhenCallerCancelsAfterRecordDecode() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    ReconcileReadyQueueBackend backend = mock(ReconcileReadyQueueBackend.class);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileLeaseStore leaseStore = mock(ReconcileLeaseStore.class);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    store.bind(backend, jobIndexStore, leaseStore, 128, record -> true, record -> false);
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
    when(backend.scanReadySlice(any(), eq(16), eq(""), any()))
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

  @Test
  void immediateQueuedReadyPointerMatchesWithoutNextAttemptTimestamp() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(
        mock(ReconcileReadyQueueBackend.class), null, null, 128, record -> true, record -> false);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob record = new StoredReconcileJob();
    record.jobId = "job-1";
    record.accountId = "acct-1";
    record.jobKind = ReconcileJobKind.PLAN_SNAPSHOT.name();
    record.state = "JS_QUEUED";
    record.executionClass = "DEFAULT";
    record.executionLane = "default";
    record.laneKey = "snapshot-plan|table-1";
    record.nextAttemptAtMs = 0L;
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

    assertTrue(store.readyPointerMatchesRecord(candidate, record));
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

  private static StoredReconcileJob queuedRecord(
      String jobId, long dueAtMs, NativeReconcileReadyQueueStore store) {
    StoredReconcileJob record = new StoredReconcileJob();
    record.jobId = jobId;
    record.accountId = "acct-1";
    record.jobKind = ReconcileJobKind.PLAN_CONNECTOR.name();
    record.state = "JS_QUEUED";
    record.executionClass = "DEFAULT";
    record.executionLane = "default";
    record.laneKey = "default";
    record.nextAttemptAtMs = dueAtMs;
    record.readyPointerKey = store.readyPointerKeyFor(record, dueAtMs);
    return record;
  }

  private static ReconcileReadyQueueStore.ReadyQueueEntry candidateFor(
      StoredReconcileJob record, String canonicalPointerKey, NativeReconcileReadyQueueStore store) {
    return new ReconcileReadyQueueStore.ReadyQueueEntry(
        record.readyPointerKey,
        canonicalPointerKey,
        record.accountId,
        record.jobId,
        record.nextAttemptAtMs,
        ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
        "");
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

  private static final class SliceRecordingBackend implements ReconcileReadyQueueBackend {
    final List<ReadyQueueSlice> slices = new ArrayList<>();
    final List<Integer> pageSizes = new ArrayList<>();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      slices.add(slice);
      pageSizes.add(pageSize);
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      return true;
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
        String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      return Optional.empty();
    }
  }

  private static final class PagingBackend implements ReconcileReadyQueueBackend {
    int scanReadySliceCalls;
    final List<String> deleted = new ArrayList<>();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      scanReadySliceCalls++;
      ReconcileReadyQueueStore.ReadyQueueEntry orphan =
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              "rp-page-1",
              "cp-page-1",
              "acct",
              "job",
              1L,
              ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
              "");
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(orphan), "next-page");
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

  private static final class CursorBackend implements ReconcileReadyQueueBackend {
    final List<String> pageTokens = new ArrayList<>();
    final List<String> deleted = new ArrayList<>();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      pageTokens.add(pageToken);
      if ("next-page".equals(pageToken)) {
        return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(), "");
      }
      ReconcileReadyQueueStore.ReadyQueueEntry orphan =
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              "rp-page-1",
              "cp-page-1",
              "acct",
              "job",
              1L,
              ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
              "");
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(List.of(orphan), "next-page");
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
