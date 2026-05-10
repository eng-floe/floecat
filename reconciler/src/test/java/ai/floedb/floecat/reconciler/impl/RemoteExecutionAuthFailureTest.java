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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RemoteExecutionAuthFailureTest {

  @Test
  void remoteSnapshotPlanningMarksTerminalAuthFailureTerminal() {
    ReconcilerBackend backend = mock(ReconcilerBackend.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = () -> Optional.of("Bearer worker-token");
    RemoteSnapshotPlanningReconcileExecutor executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 128, true);

    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("table-1", 99L, "src_ns", "src_table"),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
            "");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("connector-1")
            .build();
    when(workerClient.getPlanSnapshotInput(remoteLease))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId,
                ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
                false,
                ReconcileScope.empty(),
                ReconcileSnapshotTask.of("table-1", 99L, "src_ns", "src_table")));
    when(backend.lookupConnector(any(), eq(connectorId)))
        .thenReturn(Connector.newBuilder().setKind(ConnectorKind.CK_ICEBERG).build());
    when(backend.fetchSnapshot(any(), any(), eq(99L)))
        .thenReturn(Optional.of(Snapshot.newBuilder().build()));
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(99L)))
        .thenThrow(
            new ReconcileFailureException(
                ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
                "Forbidden",
                null));
    when(workerClient.submitPlanSnapshotFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq("Forbidden")))
        .thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
  }

  @Test
  void remoteFileGroupMarksTerminalAuthFailureTerminal() {
    RemoteFileGroupWorkerClient workerClient = mock(RemoteFileGroupWorkerClient.class);
    StandaloneJavaFileGroupExecutionRunner runner =
        mock(StandaloneJavaFileGroupExecutionRunner.class);
    RemoteFileGroupReconcileExecutor executor =
        new RemoteFileGroupReconcileExecutor(workerClient, runner, true);

    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
            "");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    StandaloneFileGroupExecutionPayload payload =
        new StandaloneFileGroupExecutionPayload(
            lease.jobId,
            lease.leaseEpoch,
            "",
            Connector.newBuilder().setKind(ConnectorKind.CK_ICEBERG).build(),
            "s3://warehouse/metadata.json",
            "src_ns",
            "src_table",
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            99L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/path/file-1.parquet"),
            ReconcileCapturePolicy.of(
                List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.FILE_STATS)),
            Map.of());
    when(workerClient.getExecution(remoteLease)).thenReturn(payload);
    when(runner.execute(payload))
        .thenThrow(
            new ReconcileFailureException(
                ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
                "Forbidden",
                null));
    when(workerClient.submitFailure(any(), any(), eq("Forbidden"))).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
  }
}
