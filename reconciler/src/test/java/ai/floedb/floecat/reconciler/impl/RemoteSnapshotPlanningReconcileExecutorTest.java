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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.mockito.ArgumentCaptor;

class RemoteSnapshotPlanningReconcileExecutorTest {

  @Test
  void executeUsesDirectStatsFastPathForStatsOnlySnapshot() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
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
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
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
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
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
  void executeCapsPlannedFileGroupsToAtomicSubmissionBudget() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
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
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(new FloecatConnector.SnapshotFilePlan(snapshotFiles(40), List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                plannedSnapshot ->
                    plannedSnapshot != null
                        && plannedSnapshot.fileGroups().size() == 14
                        && plannedSnapshot.fileGroups().stream()
                            .allMatch(group -> group.filePaths().size() <= 3)),
            argThat(fileGroupJobs -> fileGroupJobs != null && fileGroupJobs.size() == 14),
            argThat(stats -> stats != null && stats.isEmpty()));
  }

  @Test
  void executePersistsGrpcFailureDetailsForSnapshotPlanningErrors() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
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

  @Test
  void executeDoesNotRequirePersistedSnapshotMetadataForCaptureOnlyPlanning() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
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
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.of(List.of()));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never()).fetchSnapshot(any(), any(), anyLong());
  }

  @Test
  void executeDoesNotLeakWorkerAuthorizationAcrossAccounts() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider =
        accountId -> java.util.Optional.of("Bearer worker-token-" + accountId);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob leaseOne = lease("job-1", "acct-a", statsOnlyScope());
    ReconcileJobStore.LeasedJob leaseTwo = lease("job-2", "acct-b", statsOnlyScope());

    when(workerClient.getPlanSnapshotInput(any()))
        .thenAnswer(
            invocation -> {
              RemoteLeasedJob remoteLease = invocation.getArgument(0);
              ReconcileJobStore.LeasedJob lease = remoteLease.lease();
              return new StandalonePlanSnapshotPayload(
                  lease.jobId,
                  lease.leaseEpoch,
                  "",
                  connectorId(lease.accountId),
                  ReconcilerService.CaptureMode.CAPTURE_ONLY,
                  false,
                  statsOnlyScope(),
                  snapshotTask());
            });
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.of(List.of()));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    leaseOne, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .ok());
    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    leaseTwo, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .ok());

    ArgumentCaptor<ai.floedb.floecat.reconciler.spi.ReconcileContext> contextCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.spi.ReconcileContext.class);
    verify(backend, org.mockito.Mockito.times(2))
        .captureSnapshotTargetStatsDirect(
            contextCaptor.capture(), any(), eq(55L), any(), any(), any());
    assertThat(contextCaptor.getAllValues())
        .extracting(
            ctx ->
                ctx.principal().getAccountId() + "|" + ctx.authorizationToken().orElse("<missing>"))
        .containsExactly("acct-a|Bearer worker-token-acct-a", "acct-b|Bearer worker-token-acct-b");
  }

  private static ReconcileJobStore.LeasedJob lease(ReconcileScope scope) {
    return lease("job-1", "acct", scope);
  }

  private static List<FloecatConnector.SnapshotFileEntry> snapshotFiles(int count) {
    java.util.ArrayList<FloecatConnector.SnapshotFileEntry> out = new java.util.ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      out.add(
          new FloecatConnector.SnapshotFileEntry(
              "s3://bucket/file-" + i + ".parquet",
              "PARQUET",
              10L,
              1L,
              ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
              "",
              0,
              List.of(),
              null));
    }
    return List.copyOf(out);
  }

  private static ReconcileJobStore.LeasedJob lease(
      String jobId, String accountId, ReconcileScope scope) {
    return new ReconcileJobStore.LeasedJob(
        jobId,
        accountId,
        "connector-1",
        false,
        ReconcilerService.CaptureMode.CAPTURE_ONLY,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "lease-" + jobId,
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
    return connectorId("acct");
  }

  private static ResourceId connectorId(String accountId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
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
