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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class ReconcileMaintenanceServicesTest {
  @Test
  void refreshDirtyParentsAdvancesPaginationTokenUnderChurn() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> refreshed = new ArrayList<>();
    AtomicInteger churnCount = new AtomicInteger();

    putDirtyMarker(pointerStore, "acct", "a");
    putDirtyMarker(pointerStore, "acct", "z");

    service.bind(
        pointerStore,
        (accountId, parentJobId) -> {
          refreshed.add(parentJobId);
          if (parentJobId.startsWith("a") && churnCount.getAndIncrement() < 100) {
            String nextParentId = "a" + String.format("%03d", churnCount.get());
            putDirtyMarker(pointerStore, accountId, nextParentId);
          }
        },
        (accountId, parentJobId) -> false,
        1);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(refreshed.contains("a"));
    assertTrue(
        refreshed.contains("z"),
        "dirty-parent refresh should reach later markers even when earlier markers keep adding"
            + " more work");
  }

  @Test
  void refreshDirtyParentsResumesPastChurningLowerMarkerOnNextRun() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> refreshed = new ArrayList<>();

    String churningChildJobId = "691924fc-b1a9-48f7-863d-a5ed60f9c639";
    String rootJobId = "a7007123-0376-45d5-838b-bd6697ddd3a2";
    putDirtyMarker(pointerStore, "acct", churningChildJobId);
    putDirtyMarker(pointerStore, "acct", "z-later");

    service.bind(
        pointerStore,
        (accountId, parentJobId) -> {
          refreshed.add(parentJobId);
          if (churningChildJobId.equals(parentJobId)) {
            putDirtyMarker(pointerStore, accountId, parentJobId);
            putDirtyMarker(pointerStore, accountId, rootJobId);
            sleepUnchecked(5L);
          }
        },
        (accountId, parentJobId) -> false,
        1);

    service.runProjectionMaintenanceOnce(1L);
    service.runProjectionMaintenanceOnce(200L);

    assertTrue(
        refreshed.contains(rootJobId),
        "dirty-parent refresh should resume past a repeatedly dirtied lower marker");
  }

  @Test
  void leaseMaintenancePrunesOrphanedReadyPointer() {
    TestReadyQueueBackend readyQueueBackend = new TestReadyQueueBackend();
    ReconcileLeaseMaintenanceService service = new ReconcileLeaseMaintenanceService();

    service.bind(
        new NoopLeaseStore(),
        readyQueueBackend,
        mock(ReconcileReadyQueueStore.class),
        mock(ReconcileJobIndexStore.class),
        (entry, nowMs) -> {},
        record -> false,
        10,
        5_000L);

    service.runLeaseMaintenanceOnce(200L);

    assertTrue(readyQueueBackend.deleted.contains("rp-orphan"));
  }

  @Test
  void leaseMaintenancePrunesCancellationBlockedReadyPointer() {
    TestReadyQueueBackend readyQueueBackend = new TestReadyQueueBackend();
    CanonicalPointerSnapshot snapshot = new CanonicalPointerSnapshot("cp-blocked", "blob", 1L);
    readyQueueBackend.readyPointerKey = "rp-blocked";
    readyQueueBackend.canonicalPointerKey = "cp-blocked";
    readyQueueBackend.snapshot = Optional.of(snapshot);
    StoredReconcileJob record = new StoredReconcileJob();
    record.accountId = "acct";
    record.jobId = "job";

    ReconcileReadyQueueStore readyQueueStore = mock(ReconcileReadyQueueStore.class);
    when(readyQueueStore.readyPointerMatchesRecord(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.same(record)))
        .thenReturn(true);
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    when(jobIndexStore.readRecord(snapshot)).thenReturn(Optional.of(record));

    ReconcileLeaseMaintenanceService service = new ReconcileLeaseMaintenanceService();
    service.bind(
        new NoopLeaseStore(),
        readyQueueBackend,
        readyQueueStore,
        jobIndexStore,
        (entry, nowMs) -> {},
        blocked -> true,
        10,
        5_000L);

    service.runLeaseMaintenanceOnce(200L);

    assertTrue(readyQueueBackend.deleted.contains("rp-blocked"));
  }

  @Test
  void readyIndexMaintenanceCommitsRepairsInWriteItemChunks() {
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileReadyQueueStore readyQueueStore = mock(ReconcileReadyQueueStore.class);
    List<StoredReconcileJob> queued = new ArrayList<>();
    for (int i = 0; i < 26; i++) {
      StoredReconcileJob record = new StoredReconcileJob();
      record.accountId = "acct";
      record.jobId = "job-" + i;
      record.state = "JS_QUEUED";
      record.canonicalPointerKey = "canonical-" + i;
      queued.add(record);
      CanonicalPointerSnapshot snapshot =
          new CanonicalPointerSnapshot(record.canonicalPointerKey, "blob-" + i, 1L);
      when(jobIndexStore.loadCanonicalSnapshot(record.canonicalPointerKey))
          .thenReturn(Optional.of(snapshot));
      when(jobIndexStore.readRecord(snapshot)).thenReturn(Optional.of(record));
      when(readyQueueStore.readyPointerKeys(record))
          .thenReturn(
              List.of(
                  "ready/global/" + i, "ready/class/" + i, "ready/kind/" + i, "ready/lane/" + i));
    }
    when(jobIndexStore.cloneStoredRecord(
            org.mockito.ArgumentMatchers.any(StoredReconcileJob.class)))
        .thenAnswer(
            invocation -> {
              StoredReconcileJob source = invocation.getArgument(0);
              StoredReconcileJob copy = new StoredReconcileJob();
              copy.accountId = source.accountId;
              copy.jobId = source.jobId;
              copy.state = source.state;
              copy.canonicalPointerKey = source.canonicalPointerKey;
              copy.readyIndexVersion = source.readyIndexVersion;
              return copy;
            });
    ReconcileJobIndexStore.JobIndexWriteBatch repairMutationBatch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexUpsert(
                    "canonical", 1L, "blob", PointerReferenceKind.PRK_INLINE_JSON),
                new ReconcileJobIndexStore.JobIndexUpsert(
                    "lookup", 1L, "canonical", PointerReferenceKind.PRK_POINTER_KEY)),
            new ReconcileJobIndexStore.ReadyQueueMutation(
                List.of(
                    new ReconcileJobIndexStore.ReadyQueueWrite(
                        "ready/new", "canonical", PointerReferenceKind.PRK_POINTER_KEY)),
                List.of("ready/old")));
    when(jobIndexStore.buildJobIndexWriteBatch(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any()))
        .thenReturn(repairMutationBatch);
    when(jobIndexStore.listStoredJobsInState("JS_QUEUED", 128, ""))
        .thenReturn(new ReconcileJobIndexStore.StoredJobPage(queued, ""));
    List<Integer> chunkSizes = new ArrayList<>();
    when(jobIndexStore.commitReadyQueueRepairBatch(anyList(), anyList()))
        .thenAnswer(
            invocation -> {
              List<?> mutations = invocation.getArgument(0);
              List<?> writes = invocation.getArgument(1);
              chunkSizes.add((mutations.size() * 4) + writes.size());
              return true;
            });

    ReconcileReadyIndexMaintenanceService service = new ReconcileReadyIndexMaintenanceService();
    service.bind(jobIndexStore, readyQueueStore, 128);

    service.runReadyIndexMaintenanceOnce(1_000L);

    assertEquals(List.of(96, 96, 16), chunkSizes);
  }

  @Test
  void readyIndexMaintenanceSkipsCurrentVersionJobs() {
    ReconcileJobIndexStore jobIndexStore = mock(ReconcileJobIndexStore.class);
    ReconcileReadyQueueStore readyQueueStore = mock(ReconcileReadyQueueStore.class);
    StoredReconcileJob record = new StoredReconcileJob();
    record.accountId = "acct";
    record.jobId = "job-current";
    record.state = "JS_QUEUED";
    record.canonicalPointerKey = "canonical-current";
    record.readyIndexVersion = ReconcileReadyIndexMaintenanceService.CURRENT_READY_INDEX_VERSION;

    when(jobIndexStore.listStoredJobsInState("JS_QUEUED", 128, ""))
        .thenReturn(new ReconcileJobIndexStore.StoredJobPage(List.of(record), ""));

    ReconcileReadyIndexMaintenanceService service = new ReconcileReadyIndexMaintenanceService();
    service.bind(jobIndexStore, readyQueueStore, 128);

    service.runReadyIndexMaintenanceOnce(1_000L);

    verify(jobIndexStore, never()).commitReadyQueueRepairBatch(anyList(), anyList());
  }

  @Test
  void dirtyParentRefreshDeletesProjectionMarker() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> events = new ArrayList<>();

    putDirtyMarker(pointerStore, "acct", "parent");

    service.bind(
        pointerStore,
        (accountId, parentJobId) -> events.add("refresh:" + parentJobId),
        (accountId, parentJobId) -> false,
        10);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(events.size() == 1);
    assertTrue("refresh:parent".equals(events.get(0)));
    assertTrue(pointerStore.get(Keys.reconcileDirtyParentPointer("acct", "parent")).isEmpty());
  }

  @Test
  void obsoleteDirtyParentMarkerIsDeletedWithoutRefresh() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> events = new ArrayList<>();

    putDirtyMarker(pointerStore, "acct", "cancelled-child");

    service.bind(
        pointerStore,
        (accountId, parentJobId) -> events.add("refresh:" + parentJobId),
        (accountId, parentJobId) -> true,
        10);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(events.isEmpty());
    assertTrue(
        pointerStore.get(Keys.reconcileDirtyParentPointer("acct", "cancelled-child")).isEmpty());
  }

  @Test
  void cancellationCleanupMarkerPersistsChildCursorUntilComplete() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileCancellationMaintenanceService service = new ReconcileCancellationMaintenanceService();
    List<String> cursors = new ArrayList<>();

    putCancellationMarker(pointerStore, "acct", "root");

    service.bind(
        pointerStore,
        (request, childPageSize) -> {
          cursors.add(request.childPageToken());
          if (request.childPageToken().isBlank()) {
            return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
                false, "child-token-1", true, false, false);
          }
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        request -> false,
        10);

    service.runCancellationMaintenanceOnce(200L);

    Pointer marker =
        pointerStore.get(Keys.reconcileCancellationCleanupPointer("acct", "root")).orElseThrow();
    assertTrue(marker.getBlobUri().contains("child-token-1"));
    assertTrue(marker.getBlobUri().contains("true\nfalse"));

    service.runCancellationMaintenanceOnce(200L);

    assertEquals(List.of("", "child-token-1"), cursors);
    assertTrue(
        pointerStore.get(Keys.reconcileCancellationCleanupPointer("acct", "root")).isEmpty());
  }

  @Test
  void cancellationCleanupSkipsPausedMarker() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileCancellationMaintenanceService service = new ReconcileCancellationMaintenanceService();
    AtomicInteger calls = new AtomicInteger();
    String key = Keys.reconcileCancellationCleanupPointer("acct", "root");
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                "acct", "root", "", true, false, true));
    pointerStore.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L));

    service.bind(
        pointerStore,
        (request, childPageSize) -> {
          calls.incrementAndGet();
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        request -> false,
        10);

    service.runCancellationMaintenanceOnce(200L);

    assertEquals(0, calls.get());
    assertTrue(pointerStore.get(key).isPresent());
  }

  @Test
  void cancellationCleanupDeletesObsoletePausedMarker() throws Exception {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileCancellationMaintenanceService service = new ReconcileCancellationMaintenanceService();
    AtomicInteger calls = new AtomicInteger();
    String key = Keys.reconcileCancellationCleanupPointer("acct", "missing-root");
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                "acct", "missing-root", "", true, false, true));
    pointerStore.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L));

    service.bind(
        pointerStore,
        (request, childPageSize) -> {
          calls.incrementAndGet();
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        request -> true,
        10);

    Object stats = cleanupCancellationMarkers(service, System.currentTimeMillis() + 200L);

    assertEquals(0, calls.get());
    assertEquals(0, cancellationStat(stats, "paused"));
    assertEquals(1, cancellationStat(stats, "obsoleteDeleted"));
    assertEquals(1, cancellationStat(stats, "deleted"));
    assertTrue(pointerStore.get(key).isEmpty());
  }

  private static void putDirtyMarker(
      PointerStore pointerStore, String accountId, String parentJobId) {
    String key = Keys.reconcileDirtyParentPointer(accountId, parentJobId);
    putMarker(pointerStore, key, accountId, parentJobId);
  }

  private static void putCancellationMarker(
      PointerStore pointerStore, String accountId, String rootJobId) {
    String key = Keys.reconcileCancellationCleanupPointer(accountId, rootJobId);
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                accountId, rootJobId, "", false, false, false));
    long nextVersion = pointerStore.get(key).map(Pointer::getVersion).orElse(0L) + 1L;
    pointerStore.compareAndSet(
        key, nextVersion - 1L, PointerReferences.opaqueMarkerPointer(key, payload, nextVersion));
  }

  private static void sleepUnchecked(long millis) {
    try {
      Thread.sleep(Math.max(0L, millis));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static Object cleanupCancellationMarkers(
      ReconcileCancellationMaintenanceService service, long deadlineMs) throws Exception {
    Method method =
        ReconcileCancellationMaintenanceService.class.getDeclaredMethod(
            "cleanupCancellationMarkers", long.class);
    method.setAccessible(true);
    return method.invoke(service, deadlineMs);
  }

  private static int cancellationStat(Object stats, String name) throws Exception {
    Method method = stats.getClass().getDeclaredMethod(name);
    method.setAccessible(true);
    return (Integer) method.invoke(stats);
  }

  private static void putMarker(
      PointerStore pointerStore, String key, String accountId, String parentJobId) {
    String payload = accountId + "\n" + parentJobId;
    long nextVersion = pointerStore.get(key).map(Pointer::getVersion).orElse(0L) + 1L;
    pointerStore.compareAndSet(
        key, nextVersion - 1L, PointerReferences.opaqueMarkerPointer(key, payload, nextVersion));
  }

  private static final class TestPointerStore implements PointerStore {
    private final Map<String, Pointer> pointers =
        Collections.synchronizedSortedMap(new TreeMap<>());

    @Override
    public Optional<Pointer> get(String key) {
      return Optional.ofNullable(pointers.get(key));
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      Pointer current = pointers.get(key);
      long currentVersion = current == null ? 0L : current.getVersion();
      if (currentVersion != expectedVersion) {
        return false;
      }
      pointers.put(key, next.toBuilder().setKey(key).setVersion(expectedVersion + 1L).build());
      return true;
    }

    @Override
    public boolean delete(String key) {
      return pointers.remove(key) != null;
    }

    @Override
    public boolean compareAndDelete(String key, long expectedVersion) {
      Pointer current = pointers.get(key);
      if (current == null || current.getVersion() != expectedVersion) {
        return false;
      }
      pointers.remove(key);
      return true;
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      String effectivePrefix = prefix == null ? "" : prefix;
      List<String> keys = new ArrayList<>();
      synchronized (pointers) {
        for (String key : pointers.keySet()) {
          if (key.startsWith(effectivePrefix)
              && (pageToken == null || pageToken.isBlank() || key.compareTo(pageToken) > 0)) {
            keys.add(key);
          }
        }
      }
      int end = Math.min(keys.size(), Math.max(1, limit));
      List<Pointer> page = new ArrayList<>(end);
      for (int i = 0; i < end; i++) {
        page.add(pointers.get(keys.get(i)));
      }
      if (nextTokenOut != null) {
        nextTokenOut.setLength(0);
        if (end < keys.size()) {
          nextTokenOut.append(keys.get(end - 1));
        }
      }
      return page;
    }

    @Override
    public int deleteByPrefix(String prefix) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int countByPrefix(String prefix) {
      int count = 0;
      String effectivePrefix = prefix == null ? "" : prefix;
      synchronized (pointers) {
        for (String key : pointers.keySet()) {
          if (key.startsWith(effectivePrefix)) {
            count++;
          }
        }
      }
      return count;
    }

    @Override
    public boolean isEmpty() {
      return pointers.isEmpty();
    }
  }

  private static final class TestReadyQueueBackend implements ReconcileReadyQueueBackend {
    final List<String> deleted = new ArrayList<>();
    String readyPointerKey = "rp-orphan";
    String canonicalPointerKey = "cp-orphan";
    Optional<CanonicalPointerSnapshot> snapshot = Optional.empty();

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      if (pageToken != null && !pageToken.isBlank()) {
        return new ReadyQueueScanPage(List.of(), "");
      }
      ReconcileReadyQueueStore.ReadyQueueEntry orphan =
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              readyPointerKey,
              canonicalPointerKey,
              "acct",
              "job",
              1L,
              ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
              "");
      return new ReadyQueueScanPage(List.of(orphan), "");
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      deleted.add(readyPointerKey);
      return true;
    }

    @Override
    public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
        String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      return snapshot;
    }
  }

  private static final class NoopLeaseStore implements ReconcileLeaseStore {
    @Override
    public void bind(
        ReconcileLeaseBackend leaseBackend,
        ReconcileJobExecutionLoader executionLoader,
        ReconcileLeaseStateCodec leaseStateCodec,
        int casMax,
        long leaseMs,
        long leaseRenewGraceMs,
        ReconcileJobIndexStore jobIndexStore,
        CanonicalJobMutator mutateCanonicalJob,
        Predicate<String> isTerminalState,
        BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
        int maxAttempts,
        IntToLongFunction backoffMs) {}

    @Override
    public Optional<LeasedJob> leaseCanonical(
        String canonicalPointerKey,
        String readyPointerKey,
        long now,
        CanonicalPointerSnapshot initialSnapshot,
        StoredReconcileJob initialRecord) {
      return Optional.empty();
    }

    @Override
    public boolean hasActiveLease(
        String jobId,
        String leaseEpoch,
        StoredReconcileJob current,
        String context,
        boolean allowWaitingState,
        boolean logMissingLease,
        boolean allowExpiredWithinGrace) {
      return false;
    }

    @Override
    public boolean hasLiveLease(StoredReconcileJob record, boolean allowCancelling, long now) {
      return false;
    }

    @Override
    public Optional<StoredJobLease> loadLease(String accountId, String jobId) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> loadLease(StoredReconcileJob record) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> mutateLease(
        String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
      return Optional.empty();
    }

    @Override
    public Optional<StoredJobLease> renewLeaseIfEpochMatches(
        String accountId, String jobId, String leaseEpoch) {
      return Optional.empty();
    }

    @Override
    public LeaseExpiryScanPage scanExpiredLeasePointersPage(
        long nowMs, int pageSize, String pageToken) {
      return new LeaseExpiryScanPage(List.of(), "");
    }

    @Override
    public void reclaimExpiredLease(LeaseExpiryEntry leaseExpiryEntry, long nowMs) {}

    @Override
    public boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
      return false;
    }

    @Override
    public boolean tryAcquireLaneLease(
        StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
      return false;
    }

    @Override
    public void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference) {}

    @Override
    public boolean tryAcquireSnapshotLease(
        StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
      return false;
    }

    @Override
    public void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference) {}

    @Override
    public String leaseExpiryPointerKey(StoredJobLease lease) {
      return "";
    }

    @Override
    public String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
      return "";
    }
  }
}
