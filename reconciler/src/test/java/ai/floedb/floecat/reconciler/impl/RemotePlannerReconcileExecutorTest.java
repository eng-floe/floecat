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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.storage.AwsCredentialsUnavailableException;
import java.util.List;
import org.junit.jupiter.api.Test;

class RemotePlannerReconcileExecutorTest {

  @Test
  void executeMarksMissingPinnedDestinationTableIdTerminal() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    var executor = new RemotePlannerReconcileExecutor(reconcilerService, workerClient, true);

    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "missing_table"),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
            "");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("connector-1")
            .build();
    when(workerClient.getPlanConnectorInput(remoteLease))
        .thenReturn(
            new StandalonePlanConnectorPayload(
                lease.jobId,
                lease.leaseEpoch,
                connectorId,
                ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
                false,
                ReconcileScope.of(List.of(), "missing_table"),
                ReconcileExecutionPolicy.defaults(),
                ""));
    when(reconcilerService.planTableTasks(any(), eq(connectorId), any(), eq(null)))
        .thenThrow(
            new ReconcileFailureException(
                ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
                "Destination table id does not exist: missing_table",
                null));
    when(workerClient.submitPlanConnectorFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq("Destination table id does not exist: missing_table")))
        .thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertNotNull(result.error);
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    verify(workerClient)
        .submitPlanConnectorFailure(
            remoteLease,
            ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
            ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
            ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
            "Destination table id does not exist: missing_table");
  }

  @Test
  void executeMarksMissingAwsCredentialsTerminal() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    var executor = new RemotePlannerReconcileExecutor(reconcilerService, workerClient, true);

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
            ReconcileJobKind.PLAN_CONNECTOR,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
            "");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("connector-1")
            .build();
    when(workerClient.getPlanConnectorInput(remoteLease))
        .thenReturn(
            new StandalonePlanConnectorPayload(
                lease.jobId,
                lease.leaseEpoch,
                connectorId,
                ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
                false,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                ""));
    when(reconcilerService.planTableTasks(any(), eq(connectorId), any(), eq(null)))
        .thenThrow(
            new AwsCredentialsUnavailableException(
                "AWS credentials are unavailable", new IllegalStateException("missing")));
    when(workerClient.submitPlanConnectorFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq("AWS credentials are unavailable")))
        .thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertNotNull(result.error);
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    verify(workerClient)
        .submitPlanConnectorFailure(
            remoteLease,
            ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
            ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
            ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
            "AWS credentials are unavailable");
  }
}
