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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
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
import ai.floedb.floecat.reconciler.rpc.GetLeasedSnapshotFinalizeInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryClass;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryDisposition;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.service.reconciler.jobs.LeaseScanCapacityExceededException;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
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
    service.leasedFileGroupExecutionService = mock(LeasedFileGroupExecutionService.class);
    service.leasedSnapshotFinalizeInputService = mock(LeasedSnapshotFinalizeInputService.class);
    service.leasedSnapshotFinalizeExecutionService =
        mock(LeasedSnapshotFinalizeExecutionService.class);

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
  void leaseReconcileJobDoesNotHideMissingRequiredDefinitionFailures() {
    when(service.jobs.leaseNext(any()))
        .thenThrow(
            new IllegalStateException(
                "Reconcile job job-1 is missing required job definition"
                    + " blob=/accounts/acct/reconcile/jobs/job-1/job-definition.json"));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
                    .await()
                    .indefinitely());

    assertEquals("INTERNAL", error.getStatus().getCode().name());
  }

  @Test
  void leaseReconcileJobDoesNotHideUnrelatedIllegalStateFailures() {
    when(service.jobs.leaseNext(any())).thenThrow(new IllegalStateException("other failure"));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
                    .await()
                    .indefinitely());

    assertEquals("INTERNAL", error.getStatus().getCode().name());
  }

  @Test
  void leaseReconcileJobMapsLeaseScanCapacityToResourceExhausted() {
    when(service.jobs.leaseNext(any()))
        .thenThrow(new LeaseScanCapacityExceededException("capacity exhausted"));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
                    .await()
                    .indefinitely());

    assertEquals("RESOURCE_EXHAUSTED", error.getStatus().getCode().name());
  }

  @Test
  void leaseReconcileJobMapsAdmissionCancellationToCancelled() {
    when(service.jobs.leaseNext(any()))
        .thenThrow(new CancellationException("reconcile lease scan admission interrupted"));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .leaseReconcileJob(LeaseReconcileJobRequest.getDefaultInstance())
                    .await()
                    .indefinitely());

    assertEquals("CANCELLED", error.getStatus().getCode().name());
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
    when(service.jobs.reportProgress("job-1", "lease-1", 4L, 2L, 0L, 0L, 1L, 3L, 5L, "working"))
        .thenReturn(new ReconcileJobStore.ProgressUpdate(true, false));

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
    verify(service.jobs).reportProgress("job-1", "lease-1", 4L, 2L, 0L, 0L, 1L, 3L, 5L, "working");
  }

  @Test
  void completeLeasedReconcileJobMarksSucceeded() {
    when(service.jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED),
            anyLong(),
            eq(""),
            eq(7L),
            eq(3L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(2L),
            eq(9L)))
        .thenReturn(true);

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
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED),
            anyLong(),
            eq(""),
            eq(7L),
            eq(3L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(2L),
            eq(9L));
  }

  @Test
  void submitLeasedFileGroupExecutionResultRoutesChunk() {
    when(service.leasedFileGroupExecutionService.persistChunk(
            any(), eq("job-1"), eq("lease-1"), eq("result-1"), eq(3), any(), any()))
        .thenReturn(true);

    var response =
        service
            .submitLeasedFileGroupExecutionResult(
                SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setChunk(
                        SubmitLeasedFileGroupExecutionResultRequest.Chunk.newBuilder()
                            .setResultId("result-1")
                            .setChunkIndex(3)
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.leasedFileGroupExecutionService)
        .persistChunk(any(), eq("job-1"), eq("lease-1"), eq("result-1"), eq(3), any(), any());
  }

  @Test
  void submitLeasedFileGroupExecutionResultRoutesSuccessCompletion() {
    when(service.leasedFileGroupExecutionService.persistSuccess(
            any(), eq("job-1"), eq("lease-1"), eq("result-1"), any()))
        .thenReturn(true);

    var response =
        service
            .submitLeasedFileGroupExecutionResult(
                SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setSuccess(
                        SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
                            .setResultId("result-1")
                            .addFileResults(
                                ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.newBuilder()
                                    .setFilePath("s3://bucket/file-1.parquet")
                                    .setState(
                                        ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State
                                            .RFRS_SUCCEEDED)
                                    .build())
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.leasedFileGroupExecutionService)
        .persistSuccess(any(), eq("job-1"), eq("lease-1"), eq("result-1"), any());
  }

  @Test
  void getLeasedSnapshotFinalizeInputRoutesPayload() {
    when(service.leasedSnapshotFinalizeInputService.resolve(any(), eq("job-1"), eq("lease-1")))
        .thenReturn(
            new LeasedSnapshotFinalizeInputService.SnapshotFinalizeInput(
                "job-1",
                "lease-1",
                "parent-1",
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("table-1")
                    .build(),
                55L,
                LeasedSnapshotFinalizeInputService.FinalizeMode.FILE_GROUPS_NON_EMPTY,
                false,
                "",
                0,
                4));

    var response =
        service
            .getLeasedSnapshotFinalizeInput(
                GetLeasedSnapshotFinalizeInputRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-1", response.getInput().getJobId());
    assertEquals("parent-1", response.getInput().getParentJobId());
    assertEquals("table-1", response.getInput().getTableId().getId());
    assertEquals(55L, response.getInput().getSnapshotId());
    assertEquals(
        ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
            .FZM_FILE_GROUPS_NON_EMPTY,
        response.getInput().getFinalizeMode());
    assertEquals(false, response.getInput().getFullRescan());
    assertEquals(4, response.getInput().getSourceFileCount());
  }

  @Test
  void getLeasedSnapshotFinalizeInputRoutesDirectStatsPayload() {
    when(service.leasedSnapshotFinalizeInputService.resolve(any(), eq("job-1"), eq("lease-1")))
        .thenReturn(
            new LeasedSnapshotFinalizeInputService.SnapshotFinalizeInput(
                "job-1",
                "lease-1",
                "parent-1",
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("table-1")
                    .build(),
                55L,
                LeasedSnapshotFinalizeInputService.FinalizeMode.DIRECT_STATS,
                true,
                "/accounts/acct/reconcile/jobs/plan-1/direct-stats/blob.json",
                3,
                6));

    var response =
        service
            .getLeasedSnapshotFinalizeInput(
                GetLeasedSnapshotFinalizeInputRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .build())
            .await()
            .indefinitely();

    assertEquals(
        ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode.FZM_DIRECT_STATS,
        response.getInput().getFinalizeMode());
    assertTrue(response.getInput().getFullRescan());
    assertEquals(
        "/accounts/acct/reconcile/jobs/plan-1/direct-stats/blob.json",
        response.getInput().getDirectStatsBlobUri());
    assertEquals(3, response.getInput().getDirectStatsRecordCount());
    assertEquals(6, response.getInput().getSourceFileCount());
  }

  @Test
  void submitLeasedSnapshotFinalizeResultRoutesSuccessCompletion() {
    when(service.leasedSnapshotFinalizeExecutionService.persistSuccess(
            any(), eq("job-1"), eq("lease-1"), eq("result-1")))
        .thenReturn(true);

    var response =
        service
            .submitLeasedSnapshotFinalizeResult(
                SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setSuccess(
                        SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
                            .setResultId("result-1")
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.leasedSnapshotFinalizeExecutionService)
        .persistSuccess(any(), eq("job-1"), eq("lease-1"), eq("result-1"));
  }

  @Test
  void completeLeasedReconcileJobLeavesDescendantsIntactOnRetryableFailure() {
    when(service.jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_RETRYABLE),
            anyLong(),
            eq("boom"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);
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
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_RETRYABLE),
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
    when(service.jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY),
            anyLong(),
            eq("waiting on file-group children"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);

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
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY),
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
    when(service.jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_TERMINAL),
            anyLong(),
            eq("deterministic invariant failure"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);
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
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_TERMINAL),
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

  void completeLeasedReconcileJobCancelsQueuedGrandchildrenOfCancelledPlanChildren() {
    when(service.jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            eq("stop"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);
    var connector = job("job-1", "acct", ReconcileJobKind.PLAN_CONNECTOR, "", "JS_CANCELLED");
    var table = job("table-1", "acct", ReconcileJobKind.PLAN_TABLE, "job-1", "JS_CANCELLED");
    var snapshot =
        job("snapshot-1", "acct", ReconcileJobKind.PLAN_SNAPSHOT, "table-1", "JS_CANCELLED");
    var fileGroup =
        job("file-1", "acct", ReconcileJobKind.EXEC_FILE_GROUP, "snapshot-1", "JS_CANCELLED");
    when(service.jobs.get(null, "job-1")).thenReturn(Optional.of(connector));
    when(service.jobs.get("acct", "table-1")).thenReturn(Optional.of(table));
    when(service.jobs.get("acct", "snapshot-1")).thenReturn(Optional.of(snapshot));
    when(service.jobs.cancel("acct", "table-1", "stop")).thenReturn(Optional.of(table));
    when(service.jobs.cancel("acct", "snapshot-1", "stop")).thenReturn(Optional.of(snapshot));
    when(service.jobs.cancel("acct", "file-1", "stop")).thenReturn(Optional.of(fileGroup));
    when(service.jobs.childJobsPage("acct", "job-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(List.of(table), ""));
    when(service.jobs.childJobsPage("acct", "table-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(List.of(snapshot), ""));
    when(service.jobs.childJobsPage("acct", "snapshot-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(List.of(fileGroup), ""));

    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_CANCELLED)
                    .setMessage("stop")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs).cancel("acct", "table-1", "stop");
    verify(service.jobs).cancel("acct", "snapshot-1", "stop");
    verify(service.jobs).cancel("acct", "file-1", "stop");
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
