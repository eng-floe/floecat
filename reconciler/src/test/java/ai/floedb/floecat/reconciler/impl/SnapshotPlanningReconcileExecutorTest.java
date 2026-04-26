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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.StatsTargetScopeCodec;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SnapshotPlanningReconcileExecutorTest {

  @Test
  void executeSucceedsForValidSnapshotTask() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)).thenReturn(true);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(55L)
                    .setManifestList("s3://bucket/path/manifest-list.avro")
                    .build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    java.util.List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/data/file-1.parquet",
                            "PARQUET",
                            0L,
                            0L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            java.util.List.of(),
                            null),
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/data/file-2.parquet",
                            "PARQUET",
                            0L,
                            0L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            java.util.List.of(),
                            null)),
                    java.util.List.of())));
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    assertThat(result.snapshotsProcessed).isZero();
    verify(jobs)
        .persistSnapshotPlan(
            "job-1",
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "events",
                java.util.List.of(
                    ReconcileFileGroupTask.of(
                        "job-1",
                        "snapshot-55-group-0",
                        "table-1",
                        55L,
                        java.util.List.of(
                            "s3://bucket/data/file-1.parquet",
                            "s3://bucket/data/file-2.parquet")))));
    verify(jobs)
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.METADATA_AND_CAPTURE),
            org.mockito.ArgumentMatchers.eq(ReconcileScope.empty()),
            org.mockito.ArgumentMatchers.eq(
                ReconcileFileGroupTask.of(
                    "job-1", "snapshot-55-group-0", "table-1", 55L, java.util.List.of())),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
  }

  @Test
  void executeFailsWhenSnapshotTaskMissing() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            "");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isFalse();
    assertThat(result.message).contains("snapshot task is required");
  }

  @Test
  void executeSkipsFileGroupExecutionWhenSnapshotFilePlanUnavailable() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(55L)
                    .build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), anyLong())).thenReturn(Optional.empty());
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    verify(jobs)
        .persistSnapshotPlan(
            "job-1", ReconcileSnapshotTask.of("table-1", 55L, "db", "events", java.util.List.of()));
    verify(jobs, org.mockito.Mockito.never())
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyString(),
            anyBoolean(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any());
  }

  @Test
  void executeAcceptsZeroSnapshotIdForDelta() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)).thenReturn(true);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(0L)
                    .build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    java.util.List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/data/file-0.parquet",
                            "PARQUET",
                            0L,
                            0L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            java.util.List.of(),
                            null)),
                    java.util.List.of())));
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 0L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    verify(jobs)
        .persistSnapshotPlan(
            "job-1",
            ReconcileSnapshotTask.of(
                "table-1",
                0L,
                "db",
                "events",
                java.util.List.of(
                    ReconcileFileGroupTask.of(
                        "job-1",
                        "snapshot-0-group-0",
                        "table-1",
                        0L,
                        java.util.List.of("s3://bucket/data/file-0.parquet")))));
    verify(jobs)
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.METADATA_AND_CAPTURE),
            org.mockito.ArgumentMatchers.eq(ReconcileScope.empty()),
            org.mockito.ArgumentMatchers.eq(
                ReconcileFileGroupTask.of(
                    "job-1", "snapshot-0-group-0", "table-1", 0L, java.util.List.of())),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
  }

  @Test
  void executeRespectsCancellation() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> true,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.cancelled).isTrue();
    assertThat(result.snapshotsProcessed).isZero();
  }

  @Test
  void executeReusesPersistedSnapshotPlanFromLease() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)).thenReturn(true);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(55L)
                    .build()));
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var plannedGroup =
        ReconcileFileGroupTask.of(
            "job-1",
            "snapshot-55-group-0",
            "table-1",
            55L,
            java.util.List.of("s3://bucket/data/file-1.parquet"));
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of(
                "table-1", 55L, "db", "events", java.util.List.of(plannedGroup)),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    verify(jobs, org.mockito.Mockito.never()).persistSnapshotPlan(any(), any());
    verify(jobs)
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.METADATA_AND_CAPTURE),
            org.mockito.ArgumentMatchers.eq(ReconcileScope.empty()),
            org.mockito.ArgumentMatchers.eq(plannedGroup.asReference()),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
  }

  @Test
  void executeCarriesExplicitSnapshotScopedColumnSelectorsForFileGroupJobs() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)).thenReturn(true);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(55L)
                    .build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    java.util.List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/data/file-1.parquet",
                            "PARQUET",
                            0L,
                            0L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            java.util.List.of(),
                            null)),
                    java.util.List.of())));
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(
                new ReconcileScope.ScopedCaptureRequest(
                    "table-1",
                    55L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS)));
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    org.mockito.ArgumentCaptor<ReconcileScope> scopeCaptor =
        org.mockito.ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobs)
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.CAPTURE_ONLY),
            scopeCaptor.capture(),
            org.mockito.ArgumentMatchers.eq(
                ReconcileFileGroupTask.of(
                    "job-1", "snapshot-55-group-0", "table-1", 55L, java.util.List.of())),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
    assertThat(scopeCaptor.getValue().destinationCaptureRequests()).hasSize(1);
    assertThat(scopeCaptor.getValue().capturePolicy().outputs())
        .containsExactly(ReconcileCapturePolicy.Output.COLUMN_STATS);
    assertThat(scopeCaptor.getValue().capturePolicy().selectorsForStats()).containsExactly("#9");
  }

  @Test
  void executeKeepsScopedIndexOnlySelectorsOutOfStatsCapture() {
    var backend = mock(ReconcilerBackend.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)).thenReturn(true);
    when(backend.fetchSnapshot(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("table-1"))
                    .setSnapshotId(55L)
                    .build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), anyLong()))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    java.util.List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/data/file-1.parquet",
                            "PARQUET",
                            0L,
                            0L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            java.util.List.of(),
                            null)),
                    java.util.List.of())));
    var executor =
        new SnapshotPlanningReconcileExecutor(backend, jobs, executorRegistry, 128, true);
    var scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(
                new ReconcileScope.ScopedCaptureRequest(
                    "table-1",
                    55L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of("id"))),
            ReconcileCapturePolicy.of(
                List.of(new ReconcileCapturePolicy.Column("id", false, true)),
                Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.CAPTURE_ONLY,
            scope,
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileFileGroupTask.empty(),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    org.mockito.ArgumentCaptor<ReconcileScope> scopeCaptor =
        org.mockito.ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobs)
        .enqueueFileGroupExecution(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.CAPTURE_ONLY),
            scopeCaptor.capture(),
            org.mockito.ArgumentMatchers.eq(
                ReconcileFileGroupTask.of(
                    "job-1", "snapshot-55-group-0", "table-1", 55L, java.util.List.of())),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
    assertThat(scopeCaptor.getValue().capturePolicy().outputs())
        .containsExactly(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX);
    assertThat(scopeCaptor.getValue().capturePolicy().selectorsForStats()).isEmpty();
    assertThat(scopeCaptor.getValue().capturePolicy().selectorsForIndex()).containsExactly("id");
  }
}
