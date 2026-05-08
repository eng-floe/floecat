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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryClass;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryDisposition;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileExecutorControlImplTest {
  private ReconcileExecutorControlImpl service;

  @BeforeEach
  void setUp() {
    service = new ReconcileExecutorControlImpl();
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.jobs = mock(ReconcileJobStore.class);
    service.cancellations = mock(ReconcileCancellationRegistry.class);

    PrincipalContext principalContext = mock(PrincipalContext.class);
    when(service.principalProvider.get()).thenReturn(principalContext);
    when(principalContext.getCorrelationId()).thenReturn("corr");
    doNothing()
        .when(service.authz)
        .require(any(), eq(ReconcileExecutorControlImpl.EXECUTOR_CONTROL_PERMISSIONS));
  }

  @Test
  void leaseReconcileJobAuthorizesWithWorkerPermission() {
    when(service.jobs.leaseNext(any())).thenReturn(Optional.empty());

    service.leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance()).await().indefinitely();

    verify(service.authz)
        .require(any(), eq(List.of(RolePermissions.RECONCILE_EXECUTOR_CONTROL_INTERNAL)));
  }

  @Test
  void leaseReconcileJobRejectsUnrelatedPrincipal() {
    service.authz = new Authorizer();
    PrincipalContext principalContext =
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build();
    when(service.principalProvider.get()).thenReturn(principalContext);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
                .await()
                .indefinitely());
  }

  @Test
  void leaseReconcileJobAcceptsWorkerPrincipal() {
    service.authz = new Authorizer();
    PrincipalContext principalContext =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr")
            .addPermissions(RolePermissions.RECONCILE_EXECUTOR_CONTROL_INTERNAL)
            .build();
    when(service.principalProvider.get()).thenReturn(principalContext);
    when(service.jobs.leaseNext(any())).thenReturn(Optional.empty());

    var response =
        service
            .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
            .await()
            .indefinitely();

    assertTrue(!response.getFound());
  }

  @Test
  void leaseReconcileJobUsesExecutorAwareLeaseFilterAndMapsLease() {
    when(service.jobs.leaseNext(any()))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(java.util.List.of(), "orders"),
                    ReconcileExecutionPolicy.of(
                        ReconcileExecutionClass.HEAVY, "remote", Map.of("tier", "gold")),
                    "lease-1",
                    "remote-executor",
                    "")));

    var response =
        service
            .leaseReconcileJob(
                LeaseReconcileJobRequest.newBuilder()
                    .setExecutorId("remote-executor")
                    .addExecutionClasses(ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_HEAVY)
                    .addLanes("remote")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getFound());
    assertEquals("job-1", response.getJob().getJobId());
    assertEquals("connector-1", response.getJob().getConnectorId().getId());
    assertEquals("orders", response.getJob().getScope().getDestinationTableId());
    assertEquals("remote-executor", response.getJob().getPinnedExecutorId());
    verify(service.jobs)
        .leaseNext(
            argThat(
                request ->
                    request != null
                        && request.executionClasses.contains(ReconcileExecutionClass.HEAVY)
                        && request.lanes.contains("remote")
                        && request.executorIds.contains("remote-executor")));
  }

  @Test
  void leaseReconcileJobMapsIdBasedTableAndViewTasks() {
    when(service.jobs.leaseNext(any()))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-2",
                    "acct",
                    "connector-2",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(java.util.List.of("analytics-namespace-id"), null),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-2",
                    "",
                    "",
                    ReconcileJobKind.PLAN_VIEW,
                    ReconcileTableTask.of("sales", "orders", "orders-table-id", "orders_curated"),
                    ReconcileViewTask.of(
                        "sales", "orders_view", "analytics-namespace-id", "orders-view-id"),
                    "")));

    var response =
        service
            .leaseReconcileJob(
                LeaseReconcileJobRequest.newBuilder().setExecutorId("executor-1").build())
            .await()
            .indefinitely();

    assertTrue(response.getFound());
    assertEquals("orders-table-id", response.getJob().getTableTask().getDestinationTableId());
    assertEquals(
        "analytics-namespace-id", response.getJob().getViewTask().getDestinationNamespaceId());
    assertEquals("orders-view-id", response.getJob().getViewTask().getDestinationViewId());
  }

  @Test
  void renewReconcileLeaseReturnsCancellationSignal() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(service.jobs.isCancellationRequested("job-1")).thenReturn(true);

    var response =
        service
            .renewReconcileLease(
                RenewReconcileLeaseRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getRenewed());
    assertTrue(response.getCancellationRequested());
  }

  @Test
  void reportReconcileProgressActsAsHeartbeat() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);

    var response =
        service
            .reportReconcileProgress(
                ReportReconcileProgressRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setTablesScanned(4)
                    .setTablesChanged(2)
                    .setErrors(1)
                    .setSnapshotsProcessed(3)
                    .setStatsProcessed(5)
                    .setMessage("working")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getLeaseValid());
    verify(service.jobs).markProgress("job-1", "lease-1", 4, 2, 0, 0, 1, 3, 5, "working");
  }

  @Test
  void completeLeasedReconcileJobMarksSucceeded() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);

    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_SUCCEEDED)
                    .setTablesScanned(7)
                    .setTablesChanged(3)
                    .setSnapshotsProcessed(2)
                    .setStatsProcessed(9)
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs)
        .markSucceeded(
            eq("job-1"), eq("lease-1"), anyLong(), eq(7L), eq(3L), eq(0L), eq(0L), eq(2L), eq(9L));
  }

  @Test
  void completeLeasedReconcileJobLeavesDescendantsIntactOnRetryableFailure() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(service.jobs.get(null, "job-1"))
        .thenReturn(Optional.of(job("job-1", "acct", ReconcileJobKind.PLAN_CONNECTOR, "")));
    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_FAILED)
                    .setFailureRetryDisposition(ReconcileFailureRetryDisposition.RFRD_RETRYABLE)
                    .setMessage("boom")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs)
        .markFailed(
            eq("job-1"),
            eq("lease-1"),
            anyLong(),
            eq("boom"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void completeLeasedReconcileJobRequeuesDependencyNotReadyWithoutPenalty() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);

    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_FAILED)
                    .setFailureRetryDisposition(ReconcileFailureRetryDisposition.RFRD_RETRYABLE)
                    .setFailureRetryClass(ReconcileFailureRetryClass.RFRC_DEPENDENCY_NOT_READY)
                    .setMessage("waiting on file-group children")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs)
        .markWaiting(
            eq("job-1"),
            eq("lease-1"),
            anyLong(),
            eq("waiting on file-group children"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void completeLeasedReconcileJobMarksStructuredTerminalFailureTerminal() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(service.jobs.get(null, "job-1"))
        .thenReturn(
            Optional.of(job("job-1", "acct", ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, "")));

    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_FAILED)
                    .setFailureRetryDisposition(ReconcileFailureRetryDisposition.RFRD_TERMINAL)
                    .setMessage("deterministic invariant failure")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs)
        .markFailedTerminal(
            eq("job-1"),
            eq("lease-1"),
            anyLong(),
            eq("deterministic invariant failure"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId, String accountId, ReconcileJobKind jobKind, String parentJobId) {
    return job(jobId, accountId, jobKind, parentJobId, "JS_QUEUED");
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId, String accountId, ReconcileJobKind jobKind, String parentJobId, String state) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        accountId,
        "connector",
        state,
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
        jobKind,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        parentJobId);
  }

  @Test
  void getReconcileCancellationReadsQueueState() {
    when(service.jobs.isCancellationRequested("job-1")).thenReturn(true);

    var response =
        service
            .getReconcileCancellation(
                GetReconcileCancellationRequest.newBuilder().setJobId("job-1").build())
            .await()
            .indefinitely();

    assertTrue(response.getCancellationRequested());
  }
}
