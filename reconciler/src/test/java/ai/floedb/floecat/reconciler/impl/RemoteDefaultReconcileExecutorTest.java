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
import java.util.concurrent.atomic.AtomicBoolean;
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
            eq("job-1"),
            eq("lease-job-1"),
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
            eq("job-2"),
            eq("lease-job-2"),
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
            eq("job-1"),
            eq("lease-job-1"),
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
            eq("job-2"),
            eq("lease-job-2"),
            any(),
            any());
  }

  @Test
  void executeTableMarksCompletionStartedWhenRemoteLeasePreconditionFails() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    QueuedReconcileWorkerSupport queuedWorkerSupport = mock(QueuedReconcileWorkerSupport.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = accountId -> java.util.Optional.empty();
    var executor =
        new RemoteDefaultReconcileExecutor(
            reconcilerService, queuedWorkerSupport, workerClient, authProvider, true);

    ReconcileJobStore.LeasedJob lease = tableLease("job-precondition", "acct-a");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    when(workerClient.getPlanTableInput(remoteLease))
        .thenReturn(planTablePayload(lease, connectorId("acct-a")));
    when(queuedWorkerSupport.executePlannedTable(
            any(),
            eq(connectorId("acct-a")),
            eq(false),
            any(),
            any(),
            eq(ReconcilerService.CaptureMode.METADATA_ONLY),
            eq(null),
            eq("job-precondition"),
            eq("lease-job-precondition"),
            any(),
            any()))
        .thenReturn(
            new QueuedReconcileWorkerSupport.TableExecutionResult(
                ReconcileExecutor.ExecutionResult.success(1, 0, 0, 0, 0, "ok"), List.of()));
    when(workerClient.submitPlanTableSuccess(
            any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong()))
        .thenThrow(new RemoteLeasePreconditionFailedException("submitPlanTableSuccess", null));
    AtomicBoolean completionStarted = new AtomicBoolean(false);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (a, b, c, d, e, f, g, h) -> {},
                () -> completionStarted.set(true)));

    assertTrue(result.cancelled);
    assertTrue(completionStarted.get());
  }

  @Test
  void executeViewMarksCompletionStartedWhenRemoteLeasePreconditionFails() {
    ReconcilerService reconcilerService = mock(ReconcilerService.class);
    QueuedReconcileWorkerSupport queuedWorkerSupport = mock(QueuedReconcileWorkerSupport.class);
    RemotePlannerWorkerClient workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = accountId -> java.util.Optional.empty();
    var executor =
        new RemoteDefaultReconcileExecutor(
            reconcilerService, queuedWorkerSupport, workerClient, authProvider, true);

    ReconcileJobStore.LeasedJob lease = viewLease("job-view-precondition", "acct-a");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    when(workerClient.getPlanViewInput(remoteLease))
        .thenReturn(planViewPayload(lease, connectorId("acct-a")));
    when(queuedWorkerSupport.prepareViewMutation(
            any(),
            eq(connectorId("acct-a")),
            any(),
            any(),
            eq(null),
            eq("job-view-precondition"),
            eq("lease-job-view-precondition"),
            any(),
            any()))
        .thenReturn(
            new QueuedReconcileWorkerSupport.PlannedViewMutationResult(
                ReconcileExecutor.ExecutionResult.success(0, 0, 1, 0, 0, 0, 0, "ok"), null));
    when(workerClient.submitPlanViewSuccess(any(), any()))
        .thenThrow(new RemoteLeasePreconditionFailedException("submitPlanViewSuccess", null));
    AtomicBoolean completionStarted = new AtomicBoolean(false);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (a, b, c, d, e, f, g, h) -> {},
                () -> completionStarted.set(true)));

    assertTrue(result.cancelled);
    assertTrue(completionStarted.get());
  }

  private static ReconcileJobStore.LeasedJob tableLease(String jobId, String accountId) {
    return new ReconcileJobStore.LeasedJob(
        jobId,
        accountId,
        "connector-1",
        false,
        ReconcilerService.CaptureMode.METADATA_ONLY,
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

  private static ReconcileJobStore.LeasedJob viewLease(String jobId, String accountId) {
    return new ReconcileJobStore.LeasedJob(
        jobId,
        accountId,
        "connector-1",
        false,
        ReconcilerService.CaptureMode.METADATA_ONLY,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "lease-" + jobId,
        "",
        "",
        ReconcileJobKind.PLAN_VIEW,
        ReconcileTableTask.empty(),
        ReconcileViewTask.of("src", "view", "ns", "view-1"),
        ReconcileSnapshotTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
        "parent-job");
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

  private static StandalonePlanViewPayload planViewPayload(
      ReconcileJobStore.LeasedJob lease, ResourceId connectorId) {
    return new StandalonePlanViewPayload(
        lease.jobId, lease.leaseEpoch, lease.parentJobId, connectorId, lease.scope, lease.viewTask);
  }

  private static ResourceId connectorId(String accountId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }
}
