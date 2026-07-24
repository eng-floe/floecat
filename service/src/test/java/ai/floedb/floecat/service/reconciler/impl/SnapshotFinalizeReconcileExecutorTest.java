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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionContext;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.impl.InMemoryReconcileJobStore;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SnapshotFinalizeReconcileExecutorTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String CONNECTOR_ID = "conn";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;
  private static final Map<SnapshotPlanBlobStore, Map<String, List<ReconcileFileGroupTask>>>
      SNAPSHOT_PLAN_GROUPS = new IdentityHashMap<>();
  private static final Map<SnapshotPlanBlobStore, Map<String, List<TargetStatsRecord>>>
      DIRECT_STATS_RECORDS = new IdentityHashMap<>();
  private static final Map<SnapshotPlanBlobStore, Map<String, List<TargetStatsRecord>>>
      FILE_GROUP_STATS_RECORDS = new IdentityHashMap<>();

  @Test
  void enabledDefaultsTrueAndCanBeDisabled() {
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.enabled = true;
    assertTrue(executor.enabled());

    executor.enabled = false;
    assertFalse(executor.enabled());
  }

  @Test
  void neverClaimsNonEmptyFileGroupSnapshots() {
    var store = new InMemoryReconcileJobStore();
    var snapshotPlanBlobStore = snapshotPlanBlobStore();
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/file.parquet"));
    ReconcileScope scope = captureScope();
    ReconcileSnapshotTask snapshotTask = persistedSnapshotPlan(snapshotPlanBlobStore, scope, group);
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");
    ReconcileJobStore.LeasedJob lease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    SnapshotFinalizeReconcileExecutor executor =
        executor(
            store,
            new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore()),
            snapshotPlanBlobStore);

    assertFalse(executor.supports(lease));
  }

  private static SnapshotPlanBlobStore snapshotPlanBlobStore() {
    SnapshotPlanBlobStore store = mock(SnapshotPlanBlobStore.class);
    Map<String, List<ReconcileFileGroupTask>> groupsByUri = new HashMap<>();
    Map<String, List<TargetStatsRecord>> directStatsByUri = new HashMap<>();
    Map<String, List<TargetStatsRecord>> fileGroupStatsByUri = new HashMap<>();
    SNAPSHOT_PLAN_GROUPS.put(store, groupsByUri);
    DIRECT_STATS_RECORDS.put(store, directStatsByUri);
    FILE_GROUP_STATS_RECORDS.put(store, fileGroupStatsByUri);
    when(store.loadFileGroups(org.mockito.ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              ReconcileSnapshotTask snapshotTask = invocation.getArgument(0);
              if (snapshotTask == null || snapshotTask.fileGroupCount() == 0) {
                return List.of();
              }
              List<ReconcileFileGroupTask> groups =
                  groupsByUri.get(snapshotTask.fileGroupPlanBlobUri());
              if (groups == null) {
                throw new IllegalStateException(
                    "Missing snapshot plan blob fixture " + snapshotTask.fileGroupPlanBlobUri());
              }
              return groups;
            });
    when(store.loadDirectStats(org.mockito.ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              ReconcileSnapshotTask snapshotTask = invocation.getArgument(0);
              if (snapshotTask == null || snapshotTask.directStatsRecordCount() == 0) {
                return List.of();
              }
              List<TargetStatsRecord> records =
                  directStatsByUri.get(snapshotTask.directStatsBlobUri());
              if (records == null) {
                throw new IllegalStateException(
                    "Missing direct stats blob fixture " + snapshotTask.directStatsBlobUri());
              }
              return records;
            });
    when(store.loadFileGroupStats(org.mockito.ArgumentMatchers.anyString()))
        .thenAnswer(
            invocation -> {
              String blobUri = invocation.getArgument(0);
              if (blobUri == null || blobUri.isBlank()) {
                return List.of();
              }
              List<TargetStatsRecord> records = fileGroupStatsByUri.get(blobUri);
              if (records == null) {
                throw new IllegalStateException("Missing file-group stats blob fixture " + blobUri);
              }
              return records;
            });
    return store;
  }

  private static SnapshotFinalizePersistenceService persistence(StatsStore statsStore) {
    var persistence = new SnapshotFinalizePersistenceService();
    persistence.statsStore = statsStore;
    persistence.statsOrchestrator = mock(StatsOrchestrator.class);
    return persistence;
  }

  private static SnapshotFinalizeCoverageService coverageService(
      SnapshotPlanBlobStore snapshotPlanBlobStore) {
    var coverageService = new SnapshotFinalizeCoverageService();
    coverageService.snapshotPlanBlobStore = snapshotPlanBlobStore;
    return coverageService;
  }

  private static SnapshotFinalizeReconcileExecutor executor(
      ReconcileJobStore jobs, StatsStore statsStore, SnapshotPlanBlobStore snapshotPlanBlobStore) {
    var executor = new SnapshotFinalizeReconcileExecutor();
    var childStateService = new SnapshotFinalizeChildStateService();
    executor.jobs = jobs;
    executor.persistence = persistence(statsStore);
    executor.snapshotPlanBlobStore = snapshotPlanBlobStore;
    executor.coverageService = coverageService(snapshotPlanBlobStore);
    executor.currentSnapshotPointerService = mock(CurrentSnapshotPointerService.class);
    childStateService.jobs = jobs;
    return executor;
  }

  private static ResourceId tableId(String accountId, String tableId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(tableId)
        .build();
  }

  private static ReconcileSnapshotTask persistedSnapshotPlan(
      SnapshotPlanBlobStore store, ReconcileScope scope, ReconcileFileGroupTask... groups) {
    String blobUri = "/accounts/acct/reconcile/jobs/plan-1/snapshot-plan/plan.json";
    List<ReconcileFileGroupTask> plannedGroups =
        List.of(groups).stream().filter(group -> group != null && !group.isEmpty()).toList();
    SNAPSHOT_PLAN_GROUPS.get(store).put(blobUri, plannedGroups);
    return ReconcileSnapshotTask.of(
        TABLE_ID,
        SNAPSHOT_ID,
        "db",
        "events",
        plannedGroups,
        true,
        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
        blobUri,
        groups.length);
  }

  @Test
  void executeReturnsSuccessWhenDifferentFinalizerAlreadyFinalizedSnapshot() {
    var store =
        new InMemoryReconcileJobStore() {
          @Override
          public Optional<ReconcileJobStore.FinalizedSnapshotEvent> getFinalizedSnapshot(
              String accountId, String tableId, long snapshotId) {
            return Optional.of(
                new ReconcileJobStore.FinalizedSnapshotEvent(
                    accountId + ":" + tableId + ":" + snapshotId,
                    accountId,
                    tableId,
                    snapshotId,
                    123L,
                    "winning-finalizer"));
          }
        };
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var snapshotPlanBlobStore = snapshotPlanBlobStore();
    var executor = executor(store, statsStore, snapshotPlanBlobStore);
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileScope scope = captureScope();
    ReconcileSnapshotTask snapshotTask = persistedSnapshotPlan(snapshotPlanBlobStore, scope, group);
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();
    ExecutionResult result =
        executor.execute(
            new ExecutionContext(finalizerLease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    assertEquals(ExecutionResult.JobOutcome.SUCCESS, result.outcome);
    assertEquals(0, result.errors);
    assertEquals(ExecutionResult.FailureKind.NONE, result.failureKind);
    assertNull(result.error);
    assertEquals(1, result.snapshotsProcessed);
    assertTrue(result.message.contains("already finalized by job winning-finalizer"));
  }

  @Test
  void executeSucceedsForDirectStatsCompletionWithoutFileCoverage() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "",
            0,
            0,
            "",
            0);
    ReconcileScope scope = captureScope();
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.CAPTURE_ONLY,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertTrue(result.success());
    assertTrue(result.message.contains("direct stats"));
    verify(executor.currentSnapshotPointerService)
        .maybeAdvance(tableId(ACCOUNT_ID, TABLE_ID), SNAPSHOT_ID, finalizerLease.jobId);
  }

  @Test
  void executeFailsRetryablyWhenCurrentSnapshotAdvanceFailsAfterDirectStatsCompletion() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "",
            0,
            0,
            "",
            0);
    ReconcileScope scope = captureScope();
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.CAPTURE_ONLY,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();
    doThrow(new RuntimeException("pointer conflict"))
        .when(executor.currentSnapshotPointerService)
        .maybeAdvance(tableId(ACCOUNT_ID, TABLE_ID), SNAPSHOT_ID, finalizerLease.jobId);

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.success());
    assertEquals(ExecutionResult.JobOutcome.RETRYABLE_FAILURE, result.outcome);
    assertEquals(ExecutionResult.FailureKind.INTERNAL, result.failureKind);
    assertTrue(result.message.contains("Current snapshot pointer advance failed"));
  }

  @Test
  void executeLoadsDirectStatsBlobAndBuildsMissingAggregates() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var snapshotPlanBlobStore = snapshotPlanBlobStore();
    var executor = executor(store, statsStore, snapshotPlanBlobStore);
    String blobUri = "/accounts/acct/reconcile/jobs/plan-1/direct-stats/stats.json";
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord fileRecord =
        TargetStatsRecords.fileRecord(
            tableId,
            SNAPSHOT_ID,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/data/file-1.parquet")
                .setRowCount(9L)
                .setSizeBytes(128L)
                .build(),
            null);
    DIRECT_STATS_RECORDS.get(snapshotPlanBlobStore).put(blobUri, List.of(fileRecord));

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "",
            0,
            4,
            blobUri,
            1);
    ReconcileScope scope = captureScope();
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.CAPTURE_ONLY,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertTrue(result.success());
    assertEquals(
        9L,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow()
            .getTable()
            .getRowCount());
    assertEquals(
        9L,
        statsStore
            .getTargetStats(
                tableId,
                SNAPSHOT_ID,
                StatsTargetIdentity.fileTarget("s3://bucket/data/file-1.parquet"))
            .orElseThrow()
            .getFile()
            .getRowCount());
  }

  @Test
  void executeFailsExplicitlyWhenDirectStatsBlobIsMissing() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var snapshotPlanBlobStore = snapshotPlanBlobStore();
    var executor = executor(store, statsStore, snapshotPlanBlobStore);

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "",
            0,
            4,
            "/accounts/acct/reconcile/jobs/plan-1/direct-stats/missing.json",
            1);
    ReconcileScope scope = captureScope();
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.CAPTURE_ONLY,
        scope,
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.success());
    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertTrue(result.message.contains("Missing direct stats blob fixture"));
  }

  @Test
  void executePersistsEmptySnapshotSentinelForFileStatsOnly() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(ReconcileCapturePolicy.Output.FILE_STATS),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(ReconcileCapturePolicy.Output.FILE_STATS),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    assertTrue(result.ok());
    assertEquals(
        0L,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow()
            .getTable()
            .getDataFileCount());
  }

  @Test
  void executePersistsEmptySnapshotSentinelForColumnStatsOnly() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(ReconcileCapturePolicy.Output.COLUMN_STATS),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    assertTrue(result.ok());
    assertEquals(
        0L,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow()
            .getTable()
            .getRowCount());
  }

  @Test
  void executePreservesExistingTableStatsWhenEmptyMarkerWouldRetry() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(ReconcileCapturePolicy.Output.FILE_STATS),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord existing =
        TargetStatsRecords.tableRecord(
            tableId,
            SNAPSHOT_ID,
            TableValueStats.newBuilder()
                .setRowCount(17L)
                .setDataFileCount(3L)
                .setTotalSizeBytes(2048L)
                .build(),
            null);
    statsStore.putTargetStats(existing);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(ReconcileCapturePolicy.Output.FILE_STATS),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    assertTrue(result.ok());
    assertEquals(
        existing,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow());
  }

  @Test
  void executeDoesNotPersistEmptySnapshotSentinelForIndexOnly() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    assertTrue(result.ok());
    assertTrue(result.message.contains("no planned file groups"));
    assertTrue(
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .isEmpty());
  }

  @Test
  void executeFailsWhenSnapshotCoverageMetadataIsMissing() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(
                ReconcileFileGroupTask.of(
                    "plan-1",
                    "group-1",
                    TABLE_ID,
                    SNAPSHOT_ID,
                    List.of("s3://bucket/data/file-1.parquet"))));

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "missing-parent")));

    assertFalse(result.cancelled);
    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertEquals(ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertTrue(result.message.contains("requires explicit snapshot coverage metadata"));
  }

  @Test
  void executeFailsWhenParentSnapshotPlanJobIsMissing() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var snapshotPlanBlobStore = snapshotPlanBlobStore();
    var executor = executor(store, statsStore, snapshotPlanBlobStore);
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        persistedSnapshotPlan(snapshotPlanBlobStore, captureScope(), group);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "")));

    assertFalse(result.success());
    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertTrue(result.message.contains("requires parent snapshot plan job"));
  }

  @Test
  void executeFailsWhenNoPlannedFileGroupsExist() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of());

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "")));

    assertFalse(result.success());
    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertEquals(ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertTrue(result.message.contains("requires explicit snapshot coverage metadata"));
  }

  @Test
  void executeSkipsWhenParentSnapshotPlanHasNoPlannedFileGroups() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    assertTrue(result.success());
    assertNull(result.error);
    assertTrue(result.message.contains("no planned file groups"));
  }

  @Test
  void executeRejectsFileGroupChildForExplicitEmptySnapshotPlan() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(), true);
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        captureScope(),
        ReconcileFileGroupTask.of(
            "plan-1",
            "unexpected-group",
            TABLE_ID,
            SNAPSHOT_ID,
            List.of("s3://bucket/unexpected.parquet")),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertTrue(result.message.contains("EXEC_FILE_GROUP children for explicit-empty coverage"));
    assertTrue(result.message.contains("plan-1/unexpected-group"));
  }

  @Test
  void executeFailsWhenParentSnapshotPlanHasUnrecordedEmptyFileGroups() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = executor(store, statsStore, snapshotPlanBlobStore());

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            captureScope(),
            ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of()),
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(TABLE_ID, SNAPSHOT_ID, "db", "events", List.of());

    ExecutionResult result =
        executor.execute(
            context(
                new ReconcileJobStore.LeasedJob(
                    "finalizer-1",
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    captureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "",
                    "",
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    parentJobId)));

    assertFalse(result.cancelled);
    assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertEquals(ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertTrue(result.message.contains("requires explicit snapshot coverage metadata"));
  }

  private static TargetStatsRecord fileRecordWithColumnOrder(String first, String second) {
    return TargetStatsRecords.fileRecord(
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build(),
        SNAPSHOT_ID,
        FileTargetStats.newBuilder()
            .setFilePath("s3://bucket/data/file-1.parquet")
            .setRowCount(10L)
            .setSizeBytes(128L)
            .addColumns(fileColumn(first))
            .addColumns(fileColumn(second))
            .build(),
        null);
  }

  private static FileColumnStats fileColumn(String name) {
    long columnId = "id".equals(name) ? 1L : 2L;
    return FileColumnStats.newBuilder()
        .setColumnId(columnId)
        .setScalar(
            ScalarStats.newBuilder()
                .setDisplayName(name)
                .setLogicalType("STRING")
                .setRowCount(10L)
                .build())
        .build();
  }

  private static ExecutionContext context(ReconcileJobStore.LeasedJob lease) {
    return new ExecutionContext(lease, () -> false, (a, b, c, d, e, f, g, h) -> {});
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private static ReconcileScope captureScope() {
    return captureScope(
        ReconcileCapturePolicy.Output.TABLE_STATS,
        ReconcileCapturePolicy.Output.COLUMN_STATS,
        ReconcileCapturePolicy.Output.FILE_STATS);
  }

  private static ReconcileScope captureScope(ReconcileCapturePolicy.Output... outputs) {
    return ReconcileScope.of(
        List.of(),
        TABLE_ID,
        List.of(),
        ReconcileCapturePolicy.of(List.of(), EnumSet.copyOf(List.of(outputs))));
  }

  private static final class DuplicateFileStatsStore implements StatsStore {
    private final ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();

    @Override
    public void putTargetStats(TargetStatsRecord value) {}

    @Override
    public boolean putTargetStatsIfAbsent(TargetStatsRecord value) {
      return false;
    }

    @Override
    public Optional<TargetStatsRecord> getTargetStats(
        ResourceId tableId, long snapshotId, StatsTarget target) {
      return Optional.empty();
    }

    @Override
    public boolean deleteTargetStats(ResourceId tableId, long snapshotId, StatsTarget target) {
      return false;
    }

    @Override
    public StatsStorePage listTargetStats(
        ResourceId tableId,
        long snapshotId,
        Optional<StatsTargetType> targetType,
        int limit,
        String pageToken) {
      TargetStatsRecord record =
          TargetStatsRecords.fileRecord(
              this.tableId,
              SNAPSHOT_ID,
              FileTargetStats.newBuilder()
                  .setFilePath("s3://bucket/data/file-1.parquet")
                  .setRowCount(10L)
                  .setSizeBytes(128L)
                  .build(),
              null);
      return new StatsStorePage(List.of(record, record), "");
    }

    @Override
    public int countTargetStats(
        ResourceId tableId, long snapshotId, Optional<StatsTargetType> targetType) {
      return 2;
    }

    @Override
    public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
      return false;
    }

    @Override
    public void publishStatsGeneration(
        ResourceId tableId,
        long snapshotId,
        String generationId,
        List<TargetStatsRecord> finalRecords) {}

    @Override
    public MutationMeta metaForTargetStats(
        ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs) {
      return MutationMeta.getDefaultInstance();
    }
  }
}
