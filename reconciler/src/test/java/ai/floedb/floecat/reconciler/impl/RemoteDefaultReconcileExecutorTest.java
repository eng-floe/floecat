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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import java.util.List;
import org.junit.jupiter.api.Test;

class RemoteDefaultReconcileExecutorTest {

  @Test
  void executeTableDoesNotLeakWorkerAuthorizationAcrossAccounts() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    QueuedReconcileWorkerSupport queuedWorkerSupport = mock(QueuedReconcileWorkerSupport.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider =
        accountId -> java.util.Optional.of("Bearer worker-token-" + accountId);
    var executor =
        new RemoteDefaultReconcileExecutor(
            reconcilerService, queuedWorkerSupport, workerClient, authProvider, true);

    ReconcileJobStore.LeasedJob leaseOne = tableLease("job-1", "acct-a");
    ReconcileJobStore.LeasedJob leaseTwo = tableLease("job-2", "acct-b");
    RemoteLeasedJob remoteLeaseOne = new RemoteLeasedJob(leaseOne);
    RemoteLeasedJob remoteLeaseTwo = new RemoteLeasedJob(leaseTwo);

    when(workerClient.getPlanTableInput(remoteLeaseOne))
        .thenReturn(planTablePayload(leaseOne, connectorId("acct-a")));
    when(workerClient.getPlanTableInput(remoteLeaseTwo))
        .thenReturn(planTablePayload(leaseTwo, connectorId("acct-b")));
    when(queuedWorkerSupport.executePlannedTable(
            any(),
            eq(connectorId("acct-a")),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_ONLY),
            eq("Bearer worker-token-acct-a"),
            any(),
            any()))
        .thenReturn(
            new QueuedReconcileWorkerSupport.TableExecutionResult(
                ReconcileExecutor.ExecutionResult.successHandled(1, 0, 0, 0, 0, 0, 0, "ok"),
                List.of()));
    when(queuedWorkerSupport.executePlannedTable(
            any(),
            eq(connectorId("acct-b")),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_ONLY),
            eq("Bearer worker-token-acct-b"),
            any(),
            any()))
        .thenReturn(
            new QueuedReconcileWorkerSupport.TableExecutionResult(
                ReconcileExecutor.ExecutionResult.successHandled(1, 0, 0, 0, 0, 0, 0, "ok"),
                List.of()));
    when(workerClient.submitPlanTableSuccess(
            any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenReturn(true);

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

    verify(queuedWorkerSupport)
        .executePlannedTable(
            any(),
            eq(connectorId("acct-a")),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_ONLY),
            eq("Bearer worker-token-acct-a"),
            any(),
            any());
    verify(queuedWorkerSupport)
        .executePlannedTable(
            any(),
            eq(connectorId("acct-b")),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_ONLY),
            eq("Bearer worker-token-acct-b"),
            any(),
            any());
  }

  @Test
  void executeTableReturnsClassifiedSnapshotPlanningFailure() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    QueuedReconcileWorkerSupport queuedWorkerSupport = mock(QueuedReconcileWorkerSupport.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = accountId -> java.util.Optional.of("Bearer token");
    var executor =
        new RemoteDefaultReconcileExecutor(
            reconcilerService, queuedWorkerSupport, workerClient, authProvider, true);

    ReconcileJobStore.LeasedJob lease =
        tableLease("job-1", "acct-a", ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ResourceId connectorId = connectorId("acct-a");
    ReconcileFailureException failure =
        new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING,
            ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
            ReconcileExecutor.ExecutionResult.RetryClass.NONE,
            "missing table",
            null);

    when(workerClient.getPlanTableInput(remoteLease))
        .thenReturn(planTablePayload(lease, connectorId));
    when(queuedWorkerSupport.executePlannedTable(
            any(),
            eq(connectorId),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_AND_CAPTURE),
            eq("Bearer token"),
            any(),
            any()))
        .thenReturn(
            new QueuedReconcileWorkerSupport.TableExecutionResult(
                ReconcileExecutor.ExecutionResult.success(1, 1, 0, 0, 0, 0, 0, "ok"), List.of()));
    when(reconcilerService.planSnapshotTasks(
            any(),
            eq(connectorId),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_AND_CAPTURE),
            eq("Bearer token")))
        .thenThrow(failure);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertEquals(ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING, result.failureKind);
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.NONE, result.retryClass);
    verify(workerClient)
        .submitPlanTableFailure(
            remoteLease,
            ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING,
            ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
            ReconcileExecutor.ExecutionResult.RetryClass.NONE,
            "missing table");
  }

  private static ReconcileJobStore.LeasedJob tableLease(String jobId, String accountId) {
    return tableLease(jobId, accountId, ReconcilerService.CaptureMode.METADATA_ONLY);
  }

  private static ReconcileJobStore.LeasedJob tableLease(
      String jobId, String accountId, ReconcilerService.CaptureMode captureMode) {
    return new ReconcileJobStore.LeasedJob(
        jobId,
        accountId,
        "connector-1",
        false,
        captureMode,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "lease-" + jobId,
        "",
        "",
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("src", "table", "ns", "table-1", "table-1"),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
        "");
  }

  private static StandalonePlanTablePayload planTablePayload(
      ReconcileJobStore.LeasedJob lease, ResourceId connectorId) {
    return new StandalonePlanTablePayload(
        lease.jobId,
        lease.leaseEpoch,
        "",
        connectorId,
        lease.captureMode,
        false,
        ReconcileScope.empty(),
        lease.tableTask);
  }

  private static ResourceId connectorId(String accountId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }
}
