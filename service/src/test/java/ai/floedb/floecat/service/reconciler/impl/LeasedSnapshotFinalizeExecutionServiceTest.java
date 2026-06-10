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
import static org.mockito.ArgumentMatchers.anyList;
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
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
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
  private SnapshotPlanBlobStore snapshotPlanBlobStore;
  private SnapshotFinalizeCoverageService coverageService;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeExecutionService();
    jobs = mock(ReconcileJobStore.class);
    persistence = mock(SnapshotFinalizePersistenceService.class);
    snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    coverageService = mock(SnapshotFinalizeCoverageService.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.persistence = persistence;
    service.snapshotPlanBlobStore = snapshotPlanBlobStore;
    service.coverageService = coverageService;
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
                service.persistSuccessOutputBlob(
                    leasedJobWithStatsOutputs(false),
                    explicitEmptyTask,
                    tableId,
                    SNAPSHOT_ID,
                    "/accounts/acct/reconcile/jobs/finalize-job/snapshot-finalize-stats/result.json",
                    7));

    assertEquals(
        "INVALID_ARGUMENT: snapshot finalize success payload must not include stats blob metadata for this submission",
        error.getMessage());
  }

  @Test
  void persistSuccessOutputBlobRejectsStatsPayloadWhenNoStatsOutputsRequested() {
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
                service.persistSuccessOutputBlob(
                    leasedJob(false), snapshotTask, tableId, SNAPSHOT_ID, "blob://result", 1));

    assertEquals(
        "INVALID_ARGUMENT: snapshot finalize success payload must not include stats blob metadata for this submission",
        error.getMessage());
  }

  @Test
  void persistSuccessOutputBlobRejectsFullReplaceCoverageMismatchBeforeWrite() {
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
    TargetStatsRecord fileRecord = mock(TargetStatsRecord.class);
    when(snapshotPlanBlobStore.loadTargetStatsBlob("blob://result"))
        .thenReturn(List.of(fileRecord));
    when(persistence.validateReplacementStats(List.of(fileRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(fileRecord));
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(coverageService.validateCoverage(
            List.of("s3://bucket/file-1.parquet"), List.of(fileRecord)))
        .thenReturn(
            new SnapshotFinalizeCoverageService.CoverageValidation(
                false,
                List.of("s3://bucket/file-1.parquet"),
                List.of(),
                List.of(),
                "Snapshot finalization coverage mismatch missing=[s3://bucket/file-1.parquet]"));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistSuccessOutputBlob(
                    lease, snapshotTask, tableId, SNAPSHOT_ID, "blob://result", 1));

    assertEquals(
        "FAILED_PRECONDITION: Snapshot finalization coverage mismatch"
            + " missing=[s3://bucket/file-1.parquet]",
        error.getMessage());
    verify(persistence, never())
        .replaceAllStatsForSnapshot(tableId, SNAPSHOT_ID, List.of(fileRecord));
  }

  @Test
  void persistSuccessOutputBlobBuildsAggregatesForFullRescanRemoteFinalize() {
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
    TargetStatsRecord fileRecord = mock(TargetStatsRecord.class);
    TargetStatsRecord aggregateRecord = mock(TargetStatsRecord.class);
    when(snapshotPlanBlobStore.loadTargetStatsBlob("blob://result"))
        .thenReturn(List.of(fileRecord));
    when(persistence.validateReplacementStats(List.of(fileRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(fileRecord));
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
                List.of(),
                List.of("s3://bucket/file-1.parquet"),
                ""));
    when(coverageService.validateCoverage(
            List.of("s3://bucket/file-1.parquet"), List.of(fileRecord)))
        .thenReturn(
            new SnapshotFinalizeCoverageService.CoverageValidation(
                true, List.of(), List.of(), List.of(), ""));
    when(persistence.buildAggregateStats(
            tableId,
            SNAPSHOT_ID,
            Set.of(FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord)))
        .thenReturn(List.of(aggregateRecord));

    service.persistSuccessOutputBlob(lease, snapshotTask, tableId, SNAPSHOT_ID, "blob://result", 1);

    verify(persistence).replaceAllStatsForSnapshot(tableId, SNAPSHOT_ID, List.of(fileRecord));
    verify(persistence)
        .buildAggregateStats(
            eq(tableId),
            eq(SNAPSHOT_ID),
            eq(
                Set.of(
                    FloecatConnector.StatsTargetKind.TABLE,
                    FloecatConnector.StatsTargetKind.COLUMN)),
            eq(List.of(fileRecord)));
    verify(persistence).persistStats(List.of(aggregateRecord));
    verify(persistence, never()).listFileStats(eq(tableId), eq(SNAPSHOT_ID));
  }

  @Test
  void persistSuccessOutputBlobRejectsDirectStatsWhenPlannerCountDoesNotMatch() {
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
    ReconcileJobStore.LeasedJob lease = leasedJobWithStatsOutputs(true);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId(TABLE_ID)
            .build();
    TargetStatsRecord tableRecord = mock(TargetStatsRecord.class);
    when(snapshotPlanBlobStore.loadTargetStatsBlob("blob://result"))
        .thenReturn(List.of(tableRecord));
    when(persistence.validateReplacementStats(List.of(tableRecord), tableId, SNAPSHOT_ID))
        .thenReturn(List.of(tableRecord));
    when(coverageService.expectedCoverage(snapshotTask))
        .thenReturn(
            new SnapshotFinalizeCoverageService.ExpectedCoverage(
                SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
                List.of(),
                List.of(),
                ""));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistSuccessOutputBlob(
                    lease, snapshotTask, tableId, SNAPSHOT_ID, "blob://result", 1));

    assertEquals(
        "FAILED_PRECONDITION: snapshot finalize direct stats record count mismatch expected=2 actual=1",
        error.getMessage());
    verify(persistence, never())
        .replaceAllStatsForSnapshot(tableId, SNAPSHOT_ID, List.of(tableRecord));
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
