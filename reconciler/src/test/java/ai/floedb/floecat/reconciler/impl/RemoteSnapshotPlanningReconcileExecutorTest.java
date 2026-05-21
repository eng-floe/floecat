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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RemoteSnapshotPlanningReconcileExecutorTest {

  @Test
  void executeUsesDirectStatsFastPathForStatsOnlySnapshot() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = java.util.Optional::<String>empty;
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ReconcileSnapshotTask task = snapshotTask();
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                task));
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(Optional.of(mock(Snapshot.class)));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(
            Optional.of(
                List.of(
                    TargetStatsRecords.tableRecord(
                        tableId(),
                        55L,
                        TableValueStats.newBuilder().setRowCount(7L).build(),
                        null))));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never()).fetchSnapshotFilePlan(any(), any(), anyLong());
    verify(backend, never()).putTargetStats(any(), any());
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                snapshotTask ->
                    snapshotTask != null
                        && snapshotTask.fileGroups().isEmpty()
                        && snapshotTask.fileGroupPlanRecorded()
                        && snapshotTask.directStatsRecordCount() == 1
                        && snapshotTask.completionMode()
                            == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS),
            argThat(fileGroupJobs -> fileGroupJobs != null && fileGroupJobs.isEmpty()),
            argThat(stats -> stats != null && stats.size() == 1));
  }

  @Test
  void executeFallsBackToFileGroupsWhenPageIndexesAreRequested() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = java.util.Optional::<String>empty;
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(pageIndexScope());
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                pageIndexScope(),
                snapshotTask()));
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(Optional.of(mock(Snapshot.class)));
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/file-1.parquet",
                            "PARQUET",
                            10L,
                            1L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            List.of(),
                            null)),
                    List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never())
        .captureSnapshotTargetStatsDirect(any(), any(), anyLong(), any(), any(), any());
    verify(backend).fetchSnapshotFilePlan(any(), any(), eq(55L));
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                snapshotTask ->
                    snapshotTask != null
                        && snapshotTask.completionMode()
                            == ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
                        && snapshotTask.fileGroups().size() == 1),
            argThat(fileGroupJobs -> fileGroupJobs != null && fileGroupJobs.size() == 1),
            argThat(stats -> stats != null && stats.isEmpty()));
  }

  @Test
  void executeFallsBackToFileGroupsWhenDirectStatsAreUnavailable() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = java.util.Optional::<String>empty;
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(Optional.of(mock(Snapshot.class)));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/file-1.parquet",
                            "PARQUET",
                            10L,
                            1L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            List.of(),
                            null)),
                    List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend).captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any());
    verify(backend).fetchSnapshotFilePlan(any(), any(), eq(55L));
  }

  @Test
  void executePersistsGrpcFailureDetailsForSnapshotPlanningErrors() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = java.util.Optional::<String>empty;
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(Optional.of(mock(Snapshot.class)));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.RESOURCE_EXHAUSTED.withDescription(
                    "grpc: received message larger than max")));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(!result.success());
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            any(),
            any(),
            any(),
            eq("grpc=RESOURCE_EXHAUSTED desc=grpc: received message larger than max"));
  }

  private static ReconcileJobStore.LeasedJob lease(ReconcileScope scope) {
    return new ReconcileJobStore.LeasedJob(
        "job-1",
        "acct",
        "connector-1",
        false,
        ReconcilerService.CaptureMode.CAPTURE_ONLY,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "lease-1",
        "",
        "",
        ReconcileJobKind.PLAN_SNAPSHOT,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        snapshotTask(),
        ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
        "");
  }

  private static ReconcileSnapshotTask snapshotTask() {
    return ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
  }

  private static ReconcileScope statsOnlyScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(), EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
  }

  private static ReconcileScope pageIndexScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(),
            EnumSet.of(
                ReconcileCapturePolicy.Output.TABLE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId("table-1")
        .build();
  }
}
