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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionContext;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.impl.InMemoryReconcileJobStore;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SnapshotFinalizeReconcileExecutorTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String CONNECTOR_ID = "conn";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  @Test
  void executeReturnsDependencyNotReadyWhenSiblingFileGroupIsStillQueued() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    ReconcileScope scope = captureScope();
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
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask.fileGroups().getFirst(),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
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

    assertFalse(result.cancelled);
    assertEquals(ExecutionResult.JobOutcome.RETRYABLE_FAILURE, result.outcome);
    assertEquals(ExecutionResult.RetryClass.DEPENDENCY_NOT_READY, result.retryClass);
    assertTrue(result.message.contains("Waiting for snapshot file groups"));
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    assertTrue(
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .isEmpty());
  }

  @Test
  void executeCancelsWhenSiblingFileGroupWasCancelled() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    ReconcileScope scope = captureScope();
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
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask.fileGroups().getFirst(),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
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

    ReconcileJobStore.LeasedJob childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.markCancelled(
        childLease.jobId,
        childLease.leaseEpoch,
        System.currentTimeMillis(),
        "Cancelled upstream",
        0,
        0,
        0,
        0,
        0,
        0,
        0);

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.success());
    assertEquals(ExecutionResult.JobOutcome.OBSOLETE, result.outcome);
    assertTrue(result.message.contains("cancelled file-group jobs"));
    assertTrue(result.message.contains("plan-1/group-1"));
  }

  @Test
  void executeFailsWhenSiblingFileGroupFailed() {
    String maxAttemptsKey = "floecat.reconciler.job-store.max-attempts";
    String previousMaxAttempts = System.getProperty(maxAttemptsKey);
    try {
      System.setProperty(maxAttemptsKey, "1");
      var store = new InMemoryReconcileJobStore();
      var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
      var executor = new SnapshotFinalizeReconcileExecutor();
      executor.jobs = store;
      executor.statsStore = statsStore;

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
                      List.of("s3://bucket/data/file-1.parquet"))),
              true);
      ReconcileScope scope = captureScope();
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
      store.enqueueFileGroupExecution(
          ACCOUNT_ID,
          CONNECTOR_ID,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          scope,
          snapshotTask.fileGroups().getFirst(),
          ReconcileExecutionPolicy.defaults(),
          parentJobId,
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

      ReconcileJobStore.LeasedJob childLease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
              .orElseThrow();
      store.markFailed(
          childLease.jobId,
          childLease.leaseEpoch,
          System.currentTimeMillis(),
          "boom",
          0,
          0,
          0,
          0,
          1,
          0,
          0);

      ReconcileJobStore.LeasedJob finalizerLease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
              .orElseThrow();

      ExecutionResult result = executor.execute(context(finalizerLease));

      assertFalse(result.success());
      assertEquals(ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
      assertTrue(result.message.contains("blocked by failed file-group jobs"));
      assertTrue(result.message.contains("plan-1/group-1"));
      assertTrue(result.message.contains("boom"));
    } finally {
      restoreProperty(maxAttemptsKey, previousMaxAttempts);
    }
  }

  @Test
  void executeSucceedsAfterAllPlannedFileGroupsPersistResults() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

    ReconcileFileGroupTask groupOne =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileFileGroupTask groupTwo =
        ReconcileFileGroupTask.of(
            "plan-1", "group-2", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-2.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(groupOne, groupTwo), true);
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
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
    String childOneJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            groupOne,
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    String childTwoJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            groupTwo,
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
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

    store.persistFileGroupResult(
        childOneJobId,
        groupOne.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.persistFileGroupResult(
        childTwoJobId,
        groupTwo.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    ReconcileJobStore.LeasedJob childOneLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.markSucceeded(
        childOneLease.jobId, childOneLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);
    ReconcileJobStore.LeasedJob childTwoLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.markSucceeded(
        childTwoLease.jobId, childTwoLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    statsStore.putTargetStats(
        TargetStatsRecords.fileRecord(
            tableId,
            SNAPSHOT_ID,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/data/file-1.parquet")
                .setRowCount(10L)
                .setSizeBytes(128L)
                .build(),
            null));
    statsStore.putTargetStats(
        TargetStatsRecords.fileRecord(
            tableId,
            SNAPSHOT_ID,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/data/file-2.parquet")
                .setRowCount(20L)
                .setSizeBytes(256L)
                .build(),
            null));

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertTrue(result.ok());
    assertEquals(
        30L,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow()
            .getTable()
            .getRowCount());
  }

  @Test
  void executeFailsWhenPersistedFileStatsDoNotCoverPlannedFiles() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
                    List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"))),
            true);
    ReconcileScope scope = captureScope();
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
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask.fileGroups().getFirst(),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
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

    ReconcileJobStore.LeasedJob childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        childLease.jobId,
        snapshotTask
            .fileGroups()
            .getFirst()
            .withFileResults(
                List.of(
                    ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L),
                    ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    store.markSucceeded(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    statsStore.putTargetStats(
        TargetStatsRecords.fileRecord(
            tableId,
            SNAPSHOT_ID,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/data/file-1.parquet")
                .setRowCount(10L)
                .setSizeBytes(128L)
                .build(),
            null));

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.cancelled);
    assertNotNull(result.error);
    assertTrue(result.message.contains("coverage mismatch"));
    assertTrue(result.message.contains("missing"));
  }

  @Test
  void executeFailsWhenSnapshotCoverageMetadataIsMissing() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
  void executeFailsWhenNoPlannedFileGroupsExist() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
  void executeFailsWhenParentSnapshotPlanHasUnrecordedEmptyFileGroups() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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

  @Test
  void executeFailsWhenDuplicateChildJobsExistForAPlannedGroup() {
    ReconcileJobStore jobs = mock(ReconcileJobStore.class);
    StatsStore statsStore = mock(StatsStore.class);
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = jobs;
    executor.statsStore = statsStore;

    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(plannedGroup), true);
    ReconcileJobStore.ReconcileJob parentJob =
        new ReconcileJobStore.ReconcileJob(
            "snapshot-plan-1",
            ACCOUNT_ID,
            CONNECTOR_ID,
            "JS_SUCCEEDED",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            captureScope(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            "table-plan-1");
    ReconcileJobStore.ReconcileJob duplicateOne =
        new ReconcileJobStore.ReconcileJob(
            "exec-1",
            ACCOUNT_ID,
            CONNECTOR_ID,
            "JS_QUEUED",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            captureScope(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            plannedGroup,
            "snapshot-plan-1");
    ReconcileJobStore.ReconcileJob duplicateTwo =
        new ReconcileJobStore.ReconcileJob(
            "exec-2",
            ACCOUNT_ID,
            CONNECTOR_ID,
            "JS_RUNNING",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            captureScope(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            plannedGroup,
            "snapshot-plan-1");
    when(jobs.get(ACCOUNT_ID, "snapshot-plan-1")).thenReturn(Optional.of(parentJob));
    when(jobs.childJobs(ACCOUNT_ID, "snapshot-plan-1"))
        .thenReturn(List.of(duplicateOne, duplicateTwo));

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
                    "snapshot-plan-1")));

    assertFalse(result.cancelled);
    assertNotNull(result.error);
    assertTrue(result.message.contains("duplicate EXEC_FILE_GROUP children"));
    assertTrue(result.message.contains("plan-1/group-1"));
  }

  @Test
  void executeMarksMissingPlannedChildGroupAsTerminalInvariantFailure() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID, SNAPSHOT_ID, "db", "events", List.of(plannedGroup), true);
    ReconcileScope scope = captureScope();
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
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

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.cancelled);
    assertNotNull(result.error);
    assertEquals(ExecutionResult.RetryDisposition.RETRYABLE, result.retryDisposition);
    assertEquals(ExecutionResult.RetryClass.STATE_UNCERTAIN, result.retryClass);
    assertTrue(result.message.contains("missing EXEC_FILE_GROUP children"));
  }

  @Test
  void executeFailsWhenDuplicateFileStatsAreListed() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new DuplicateFileStatsStore();
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    ReconcileScope scope = captureScope();
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
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask.fileGroups().getFirst(),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
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

    ReconcileJobStore.LeasedJob childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        childLease.jobId,
        snapshotTask
            .fileGroups()
            .getFirst()
            .withFileResults(
                List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.markSucceeded(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertFalse(result.cancelled);
    assertNotNull(result.error);
    assertTrue(result.message.contains("duplicates"));
  }

  @Test
  void executeSucceedsWhenAggregateAlreadyExistsWithIdenticalContent() {
    var store = new InMemoryReconcileJobStore();
    var statsStore = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var executor = new SnapshotFinalizeReconcileExecutor();
    executor.jobs = store;
    executor.statsStore = statsStore;

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
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
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
    store.enqueueFileGroupExecution(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        snapshotTask.fileGroups().getFirst(),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
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

    ReconcileJobStore.LeasedJob childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        childLease.jobId,
        snapshotTask
            .fileGroups()
            .getFirst()
            .withFileResults(
                List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.markSucceeded(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    statsStore.putTargetStats(
        TargetStatsRecords.fileRecord(
            tableId,
            SNAPSHOT_ID,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/data/file-1.parquet")
                .setRowCount(10L)
                .setSizeBytes(128L)
                .build(),
            null));
    statsStore.putTargetStats(
        TargetStatsRecords.tableRecord(
            tableId,
            SNAPSHOT_ID,
            TableValueStats.newBuilder()
                .setRowCount(10L)
                .setDataFileCount(1L)
                .setTotalSizeBytes(128L)
                .build(),
            null));

    ReconcileJobStore.LeasedJob finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();

    ExecutionResult result = executor.execute(context(finalizerLease));

    assertTrue(result.ok());
    assertEquals(
        10L,
        statsStore
            .getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget())
            .orElseThrow()
            .getTable()
            .getRowCount());
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
    return ReconcileScope.of(
        List.of(),
        TABLE_ID,
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(),
            EnumSet.of(
                ReconcileCapturePolicy.Output.TABLE_STATS,
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.FILE_STATS)));
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
    public MutationMeta metaForTargetStats(
        ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs) {
      return MutationMeta.getDefaultInstance();
    }
  }
}
