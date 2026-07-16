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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedSnapshotFinalizeExecutionServiceTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String FINALIZE_JOB_ID = "finalize-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  private LeasedSnapshotFinalizeExecutionService service;
  private ReconcileJobStore jobs;
  private SnapshotFinalizePersistenceService persistence;
  private SnapshotFinalizeCoverageService coverageService;
  private SnapshotFinalizeChildStateService childStateService;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeExecutionService();
    jobs = mock(ReconcileJobStore.class);
    persistence = mock(SnapshotFinalizePersistenceService.class);
    coverageService = mock(SnapshotFinalizeCoverageService.class);
    childStateService = mock(SnapshotFinalizeChildStateService.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.persistence = persistence;
    service.coverageService = coverageService;
    service.childStateService = childStateService;
    when(principal.getCorrelationId()).thenReturn("corr");
  }

  @Test
  void persistSuccessRejectsStatsPayloadForExplicitEmptyCoverage() {
    ReconcileSnapshotTask explicitEmptyTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            0);
    when(coverageService.expectedCoverage(explicitEmptyTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.EXPLICIT_EMPTY,
                List.of(),
                List.of(),
                ""));
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistStatsChunk(
                    leasedJobWithStatsOutputs(false),
                    explicitEmptyTask,
                    tableId,
                    SNAPSHOT_ID,
                    0,
                    List.of(mock(TargetStatsRecord.class))));

    assertEquals(
        "INVALID_ARGUMENT: snapshot finalize chunk must not include stats records for this submission",
        error.getMessage());
  }

  @Test
  void persistStatsChunkRejectsStatsPayloadWhenNoStatsOutputsRequested() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistStatsChunk(
                    leasedJob(false),
                    snapshotTask,
                    tableId,
                    SNAPSHOT_ID,
                    0,
                    List.of(mock(TargetStatsRecord.class))));

    assertEquals(
        "INVALID_ARGUMENT: snapshot finalize chunk must not include stats records for this submission",
        error.getMessage());
  }

  @Test
  void persistSuccessDoesNotFinalizeIdempotencyWhenLeaseOutcomeRejected() {
    IdempotencyRepository idempotencyStore = mock(IdempotencyRepository.class);
    service.idempotencyStore = idempotencyStore;
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
    when(idempotencyStore.get(anyString())).thenReturn(java.util.Optional.empty());
    when(idempotencyStore.createPending(
            anyString(), anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(true);
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            0);
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(FINALIZE_JOB_ID))
        .thenReturn(java.util.Optional.of(finalizeJobView(snapshotTask)));
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.EXPLICIT_EMPTY,
                List.of(),
                List.of(),
                ""));
    when(jobs.applyLeaseOutcome(
            anyString(),
            anyString(),
            any(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(false);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1"));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(idempotencyStore, never())
        .finalizeSuccess(
            anyString(), anyString(), anyString(), anyString(), any(), any(), any(), any(), any());
  }

  @Test
  void finalizeChunkedSuccessReplacesFileGroupStatsForFullRescan() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(childStateService.childState(lease.accountId, lease.parentJobId, lease.jobId, List.of()))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                0, 0, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));

    service.finalizeChunkedSuccess(lease, snapshotTask, tableId, SNAPSHOT_ID);

    verify(persistence)
        .publishFileGroupStatsGeneration(tableId, SNAPSHOT_ID, "full-rescan-parent-job", List.of());
    verify(persistence, never()).persistStats(anyList());
  }

  @Test
  void finalizeChunkedSuccessReplacesMergedAggregatesForFullRescanRemoteFinalize() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithAggregateOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord aggregateRecord = mock(TargetStatsRecord.class);
    when(aggregateRecord.hasTarget()).thenReturn(true);
    ReconcileFileGroupTask completedGroup =
        ReconcileFileGroupTask.of(
                "plan-1",
                "group-1",
                TABLE_ID,
                SNAPSHOT_ID,
                List.of("s3://bucket/file-1.parquet"),
                List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 0L)))
            .withPartialAggregateRecords(List.of(aggregateRecord));
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(completedGroup.asReference()),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(childStateService.childState(
            lease.accountId, lease.parentJobId, lease.jobId, List.of(completedGroup.asReference())))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                1,
                1,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(completedGroup)));
    when(persistence.mergeCompletedGroupPartials(
            tableId,
            SNAPSHOT_ID,
            Set.of(FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.COLUMN),
            List.of(completedGroup)))
        .thenReturn(List.of(aggregateRecord));

    service.finalizeChunkedSuccess(lease, snapshotTask, tableId, SNAPSHOT_ID);

    verify(persistence)
        .publishFileGroupStatsGeneration(
            tableId, SNAPSHOT_ID, "full-rescan-parent-job", List.of(aggregateRecord));
    verify(persistence)
        .mergeCompletedGroupPartials(
            eq(tableId),
            eq(SNAPSHOT_ID),
            eq(
                Set.of(
                    FloecatConnector.StatsTargetKind.TABLE,
                    FloecatConnector.StatsTargetKind.COLUMN)),
            eq(List.of(completedGroup)));
    verify(persistence, never()).persistStats(anyList());
  }

  @Test
  void persistStatsChunkPersistsFinalizerAggregateStatsForIncrementalFileGroupSnapshot() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(false);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord aggregateRecord = mock(TargetStatsRecord.class);
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(persistence.validateAggregateStats(List.of(aggregateRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(aggregateRecord));

    service.persistStatsChunk(
        lease, snapshotTask, tableId, SNAPSHOT_ID, 0, List.of(aggregateRecord));

    verify(persistence).persistStats(List.of(aggregateRecord));
    verify(jobs)
        .persistSnapshotFinalizeDirectStatsProgress(
            lease.jobId, lease.leaseEpoch, lease.fullRescan, 0, 1);
    verify(persistence, never()).mergeCompletedGroupPartials(any(), anyLong(), anySet(), anyList());
  }

  @Test
  void persistStatsChunkDoesNotPersistFinalizerAggregateStatsForFullFileGroupSnapshot() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord aggregateRecord = mock(TargetStatsRecord.class);
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(persistence.validateAggregateStats(List.of(aggregateRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(aggregateRecord));

    service.persistStatsChunk(
        lease, snapshotTask, tableId, SNAPSHOT_ID, 0, List.of(aggregateRecord));

    verify(persistence, never()).persistStats(anyList());
    verify(jobs, never())
        .persistSnapshotFinalizeDirectStatsProgress(
            lease.jobId, lease.leaseEpoch, lease.fullRescan, 0, 1);
    verify(persistence, never()).mergeCompletedGroupPartials(any(), anyLong(), anySet(), anyList());
  }

  @Test
  void finalizeChunkedSuccessSkipsJavaRollupWhenFinalizerAggregatesWereSubmitted() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1)
            .withDirectStatsPersistedRecordCountForChunk(0, 1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithAggregateOutputs(false);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));

    service.finalizeChunkedSuccess(lease, snapshotTask, tableId, SNAPSHOT_ID);

    verify(childStateService, never()).childState(any(), any(), any(), anyList());
    verify(persistence, never()).mergeCompletedGroupPartials(any(), anyLong(), anySet(), anyList());
    verify(persistence, never()).persistStats(anyList());
  }

  @Test
  void finalizeChunkedSuccessRejectsDirectStatsWhenPlannerCountDoesNotMatch() {
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
            "blob://planner-direct-stats",
            2);
    ReconcileSnapshotTask persistedCountTask =
        snapshotTask.withDirectStatsPersistedRecordCountForChunk(0, 1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord tableRecord = mock(TargetStatsRecord.class);
    when(persistence.validateReplacementStats(List.of(tableRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(tableRecord));
    when(coverageService.expectedCoverage(persistedCountTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
                List.of(),
                List.of(),
                ""));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.finalizeChunkedSuccess(lease, persistedCountTask, tableId, SNAPSHOT_ID));

    assertEquals(
        "FAILED_PRECONDITION: snapshot finalize direct stats record count mismatch expected=2 actual=1",
        error.getMessage());
    verify(persistence, never())
        .replaceAllStatsForSnapshot(eq(tableId), eq(SNAPSHOT_ID), anyList());
    verify(persistence, never()).persistStats(anyList());
  }

  @Test
  void fullRescanEmptyDirectStatsChunkResetsProgressAndBlocksSuccessUntilRepersisted() {
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
                "blob://planner-direct-stats",
                3)
            .withDirectStatsPersistedRecordCountForChunk(0, 1)
            .withDirectStatsPersistedRecordCountForChunk(1, 1)
            .withDirectStatsPersistedRecordCountForChunk(2, 1);
    ReconcileSnapshotTask resetTask = snapshotTask.withoutDirectStatsPersistedRecordCounts();
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
                List.of(),
                List.of(),
                ""));
    when(coverageService.expectedCoverage(resetTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
                List.of(),
                List.of(),
                ""));

    service.persistStatsChunk(lease, snapshotTask, tableId, SNAPSHOT_ID, 0, List.of());

    // A full-rescan finalize that finds no files re-finalizes a LIVE snapshot: it publishes an
    // empty generation via replaceAllStatsForSnapshot (which RETAINS superseded generations for
    // pinned readers), NOT the whole-prefix deleteAllStatsForSnapshot teardown (reserved for
    // actual snapshot deletion).
    verify(persistence).replaceAllStatsForSnapshot(tableId, SNAPSHOT_ID, List.of());
    verify(persistence, never()).deleteAllStatsForSnapshot(tableId, SNAPSHOT_ID);
    verify(jobs)
        .persistSnapshotFinalizeDirectStatsProgress(lease.jobId, lease.leaseEpoch, true, 0, 0);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.finalizeChunkedSuccess(lease, resetTask, tableId, SNAPSHOT_ID));

    assertEquals(
        "FAILED_PRECONDITION: snapshot finalize direct stats record count mismatch expected=3 actual=0",
        error.getMessage());
  }

  @Test
  void finalizeChunkedSuccessDoesNotBuildAggregatesForDirectStats() {
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
            "blob://planner-direct-stats",
            1);
    snapshotTask = snapshotTask.withDirectStatsPersistedRecordCountForChunk(0, 1);
    ReconcileJobStore.LeasedJob lease = leasedJobWithAggregateOutputs(false);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
                List.of(),
                List.of(),
                ""));

    service.finalizeChunkedSuccess(lease, snapshotTask, tableId, SNAPSHOT_ID);

    verify(persistence, never()).persistStats(anyList());
    verify(persistence, never())
        .completeStatsWithAggregates(eq(tableId), eq(SNAPSHOT_ID), anySet(), anyList());
  }

  @Test
  void persistStatsChunkRejectsFileGroupStatsPayloadsWhenNoStatsOutputsRequested() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistStatsChunk(
                    leasedJob(false),
                    snapshotTask,
                    tableId,
                    SNAPSHOT_ID,
                    0,
                    List.of(mock(TargetStatsRecord.class))));

    assertEquals(
        "INVALID_ARGUMENT: snapshot finalize chunk must not include stats records for this submission",
        error.getMessage());
    verify(persistence, never()).persistStats(anyList());
  }

  private static ReconcileJobStore.LeasedJob leasedJob(boolean fullRescan) {
    return new ReconcileJobStore.LeasedJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        fullRescan,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        LEASE_EPOCH,
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView(
      ReconcileSnapshotTask snapshotTask) {
    return new ReconcileJobStore.ReconcileJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
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
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }

  private static ReconcileJobStore.LeasedJob leasedJobWithStatsOutputs(boolean fullRescan) {
    return new ReconcileJobStore.LeasedJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        fullRescan,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(
            List.of(),
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), EnumSet.of(ReconcileCapturePolicy.Output.FILE_STATS))),
        ReconcileExecutionPolicy.defaults(),
        LEASE_EPOCH,
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }

  private static ReconcileJobStore.LeasedJob leasedJobWithAggregateOutputs(boolean fullRescan) {
    return new ReconcileJobStore.LeasedJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        fullRescan,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(
            List.of(),
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                EnumSet.of(
                    ReconcileCapturePolicy.Output.FILE_STATS,
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS))),
        ReconcileExecutionPolicy.defaults(),
        LEASE_EPOCH,
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }
}
