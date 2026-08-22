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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileProjectionMaintenanceService.RefreshResult;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
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
  private static final String WORKER_AFFINITY = "reconciler-v1";
  private static final String DIRTY_PARENT_PREFIX =
      Keys.reconcileDirtyParentPointerPrefix(WORKER_AFFINITY);

  @Test
  void projectionMaintenanceOnlyConsumesItsWorkerAffinityNamespace() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> refreshed = new ArrayList<>();
    putDirtyMarker(pointerStore, WORKER_AFFINITY, "acct", "owned-parent", 1L, 0L);
    putDirtyMarker(pointerStore, "reconciler-v2", "acct", "other-parent", 1L, 0L);
    String legacyKey = "/accounts/by-id/reconcile/jobs/dirty-parents/acct/legacy-parent";
    pointerStore.compareAndSet(
        legacyKey,
        0L,
        PointerReferences.opaqueMarkerPointer(
            legacyKey, "acct\nlegacy-parent", 1L));

    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          refreshed.add(parentJobId);
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);

    assertEquals(List.of("owned-parent"), refreshed);
    assertTrue(pointerStore.get(legacyKey).isPresent());
    assertTrue(
        pointerStore
            .get(dirtyParentKey("reconciler-v2", "acct", "other-parent"))
            .isPresent());
  }

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
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          refreshed.add(parentJobId);
          if (parentJobId.startsWith("a") && churnCount.getAndIncrement() < 100) {
            String nextParentId = "a" + String.format("%03d", churnCount.get());
            putDirtyMarker(pointerStore, accountId, nextParentId);
          }
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
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
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          refreshed.add(parentJobId);
          if (churningChildJobId.equals(parentJobId)) {
            putDirtyMarker(pointerStore, accountId, parentJobId);
            putDirtyMarker(pointerStore, accountId, rootJobId);
            sleepUnchecked(5L);
          }
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        1);

    service.runProjectionMaintenanceOnce(1L);
    service.runProjectionMaintenanceOnce(200L);

    assertTrue(
        refreshed.contains(rootJobId),
        "dirty-parent refresh should resume past a repeatedly dirtied lower marker");
  }

  @Test
  void leaseMaintenanceQueriesOnlyExpiryIndex() {
    NoopLeaseStore leaseStore = new NoopLeaseStore();
    ReconcileLeaseMaintenanceService service = new ReconcileLeaseMaintenanceService();
    service.bind(leaseStore, (entry, nowMs) -> {}, 10, 0L);

    service.runLeaseMaintenanceOnce(200L);

    assertEquals(1, leaseStore.expiryQueries.get());
  }

  @Test
  void dirtyParentRefreshDeletesProjectionMarker() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> events = new ArrayList<>();

    putDirtyMarker(pointerStore, "acct", "parent");

    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          events.add("refresh:" + parentJobId);
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(events.size() == 1);
    assertTrue("refresh:parent".equals(events.get(0)));
    assertTrue(pointerStore.get(dirtyParentKey("acct", "parent")).isEmpty());
  }

  @Test
  void obsoleteDirtyParentMarkerIsDeletedWithoutRefresh() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> events = new ArrayList<>();

    putDirtyMarker(pointerStore, "acct", "cancelled-child");

    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> RefreshResult.OBSOLETE,
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);

    assertTrue(events.isEmpty());
    assertTrue(
        pointerStore.get(dirtyParentKey("acct", "cancelled-child")).isEmpty());
  }

  @Test
  void dirtyParentRefreshDebouncesUntilMarkerIsDue() {
    TestPointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> events = new ArrayList<>();
    putDirtyMarker(pointerStore, "acct", "parent", 7L, System.currentTimeMillis() + 60_000L);
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          events.add(parentJobId);
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    int readsAfterFirstTick = pointerStore.prefixReads.get();
    service.runProjectionMaintenanceOnce(200L);

    assertTrue(events.isEmpty());
    assertEquals(readsAfterFirstTick, pointerStore.prefixReads.get());
    assertTrue(pointerStore.get(dirtyParentKey("acct", "parent")).isPresent());
  }

  @Test
  void newerGenerationSurvivesRefreshOfObservedMarker() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    AtomicInteger refreshes = new AtomicInteger();
    putDirtyMarker(pointerStore, "acct", "parent", 1L, 0L);
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          if (refreshes.incrementAndGet() == 1) {
            putDirtyMarker(pointerStore, accountId, parentJobId, 2L, 0L);
          }
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    assertTrue(pointerStore.get(dirtyParentKey("acct", "parent")).isPresent());
    service.runProjectionMaintenanceOnce(200L);

    assertEquals(2, refreshes.get());
    assertTrue(pointerStore.get(dirtyParentKey("acct", "parent")).isEmpty());
  }

  @Test
  void retryableProjectionConflictRetainsObservedMarkerForNextTick() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    AtomicInteger attempts = new AtomicInteger();
    String markerKey = dirtyParentKey("acct", "parent");
    putDirtyMarker(pointerStore, "acct", "parent", 1L, 0L);
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, key, markerVersion) ->
            attempts.incrementAndGet() == 1
                ? RefreshResult.PROJECTION_CONFLICT
                : RefreshResult.OBSOLETE,
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    assertTrue(pointerStore.get(markerKey).isPresent());

    service.runProjectionMaintenanceOnce(200L);
    assertEquals(2, attempts.get());
    assertTrue(pointerStore.get(markerKey).isEmpty());
  }

  @Test
  void markerAcknowledgementConflictDefersNewerMarkerWithoutRetryingProjectionCommit() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    AtomicInteger attempts = new AtomicInteger();
    String markerKey = dirtyParentKey("acct", "parent");
    putDirtyMarker(pointerStore, "acct", "parent", 1L, 0L);
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, key, markerVersion) -> {
          if (attempts.incrementAndGet() == 1) {
            putDirtyMarker(pointerStore, accountId, parentJobId, 2L, 0L);
            return RefreshResult.MARKER_ACK_CONFLICT;
          }
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    assertTrue(pointerStore.get(markerKey).isPresent());

    service.runProjectionMaintenanceOnce(200L);
    assertEquals(2, attempts.get());
    assertTrue(pointerStore.get(markerKey).isEmpty());
  }

  @Test
  void markerCountBudgetResumesAfterLastConsumedMarker() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    List<String> refreshed = new ArrayList<>();
    putDirtyMarker(pointerStore, "acct", "a");
    putDirtyMarker(pointerStore, "acct", "b");
    putDirtyMarker(pointerStore, "acct", "c");
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          refreshed.add(parentJobId);
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(1_000L, 2);
    service.runProjectionMaintenanceOnce(1_000L, 2);

    assertEquals(List.of("a", "b", "c"), refreshed);
  }

  @Test
  void idleProjectionMaintenanceSkipsPrefixReadsUntilSignalled() {
    TestPointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> RefreshResult.OBSOLETE,
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    assertEquals(1, pointerStore.prefixReads.get());

    service.runProjectionMaintenanceOnce(200L);
    assertEquals(1, pointerStore.prefixReads.get());

    service.signalWork();
    service.runProjectionMaintenanceOnce(200L);
    assertEquals(2, pointerStore.prefixReads.get());
  }

  @Test
  void projectionMaintenanceRecoveryIntervalIsConfigurable() {
    TestPointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    service.configureIdleRecoveryMillis(1L);
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> RefreshResult.OBSOLETE,
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    sleepUnchecked(5L);
    service.runProjectionMaintenanceOnce(200L);

    assertEquals(2, pointerStore.prefixReads.get());
  }

  @Test
  void workSignalledDuringProjectionRefreshKeepsNextPassActive() {
    TestPointerStore pointerStore = new TestPointerStore();
    ReconcileProjectionMaintenanceService service = new ReconcileProjectionMaintenanceService();
    putDirtyMarker(pointerStore, "acct", "parent");
    service.bind(
        pointerStore,
        (accountId, parentJobId, generation, markerKey, markerVersion) -> {
          service.signalWork();
          return RefreshResult.OBSOLETE;
        },
        DIRTY_PARENT_PREFIX,
        10);

    service.runProjectionMaintenanceOnce(200L);
    int readsAfterRefresh = pointerStore.prefixReads.get();
    service.runProjectionMaintenanceOnce(200L);

    assertTrue(pointerStore.prefixReads.get() > readsAfterRefresh);
  }

  @Test
  void cancellationCleanupMarkerPersistsChildCursorUntilComplete() {
    PointerStore pointerStore = new TestPointerStore();
    ReconcileCancellationMaintenanceService service = new ReconcileCancellationMaintenanceService();
    List<String> cursors = new ArrayList<>();

    putCancellationMarker(pointerStore, "acct", "root");

    service.bind(
        pointerStore,
        (request, childPageSize, deadlineMs) -> {
          cursors.add(request.childPageToken());
          if (request.childPageToken().isBlank()) {
            return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
                false, "child-token-1", true, false, false);
          }
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        (request, deadlineMs) -> false,
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
        (request, childPageSize, deadlineMs) -> {
          calls.incrementAndGet();
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        (request, deadlineMs) -> false,
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
        (request, childPageSize, deadlineMs) -> {
          calls.incrementAndGet();
          return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
              true, "", false, false, false);
        },
        (request, deadlineMs) -> true,
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
    putDirtyMarker(pointerStore, accountId, parentJobId, 1L, 0L);
  }

  private static void putDirtyMarker(
      PointerStore pointerStore,
      String accountId,
      String parentJobId,
      long generation,
      long dirtyAtMs) {
    putDirtyMarker(
        pointerStore, WORKER_AFFINITY, accountId, parentJobId, generation, dirtyAtMs);
  }

  private static void putDirtyMarker(
      PointerStore pointerStore,
      String workerAffinity,
      String accountId,
      String parentJobId,
      long generation,
      long dirtyAtMs) {
    String key = dirtyParentKey(workerAffinity, accountId, parentJobId);
    String payload = accountId + "\n" + parentJobId + "\n" + generation + "\n" + dirtyAtMs;
    long nextVersion = pointerStore.get(key).map(Pointer::getVersion).orElse(0L) + 1L;
    pointerStore.compareAndSet(
        key, nextVersion - 1L, PointerReferences.opaqueMarkerPointer(key, payload, nextVersion));
  }

  private static String dirtyParentKey(String accountId, String parentJobId) {
    return dirtyParentKey(WORKER_AFFINITY, accountId, parentJobId);
  }

  private static String dirtyParentKey(
      String workerAffinity, String accountId, String parentJobId) {
    return Keys.reconcileDirtyParentPointer(workerAffinity, accountId, parentJobId);
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
    private final AtomicInteger prefixReads = new AtomicInteger();

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
      prefixReads.incrementAndGet();
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
    public String pageTokenAfterKey(String key) {
      return key;
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

  private static final class NoopLeaseStore implements ReconcileLeaseStore {
    private final AtomicInteger expiryQueries = new AtomicInteger();

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
    public Optional<ReconcileJobIndexStore.CanonicalEnvelope> completeLeaseTransition(
        String jobId,
        String leaseEpoch,
        UnaryOperator<StoredReconcileJob> mutator,
        java.util.function.Function<StoredReconcileJob, List<PointerStore.UnconditionalUpsert>>
            pointerTouches) {
      return Optional.empty();
    }

    @Override
    public LeaseExpiryScanPage scanExpiredLeasePointersPage(
        long nowMs, int pageSize, String pageToken) {
      expiryQueries.incrementAndGet();
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
    public void clearSnapshotOwnershipIfOwned(
        StoredReconcileJob record, String expectedReference) {}

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
