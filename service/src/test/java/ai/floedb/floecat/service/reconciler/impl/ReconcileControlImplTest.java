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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileControlImplTest {
  private ReconcileControlImpl service;

  @BeforeEach
  void setUp() {
    service = new ReconcileControlImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.jobs = mock(ReconcileJobStore.class);
    service.cancellations = mock(ReconcileCancellationRegistry.class);
    service.settings = mock(ReconcilerSettingsStore.class);
    service.captureNowDefaultWait = java.time.Duration.ofSeconds(10);
    service.captureNowMaxWait = java.time.Duration.ofSeconds(30);

    PrincipalContext principalContext = mock(PrincipalContext.class);
    when(service.principalProvider.get()).thenReturn(principalContext);
    when(principalContext.getCorrelationId()).thenReturn("corr");
    when(principalContext.getAccountId()).thenReturn("acct");
    doNothing().when(service.authz).require(any(), anyString());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    when(service.connectorRepo.getById(any()))
        .thenReturn(Optional.of(connector(connectorId, ConnectorState.CS_ACTIVE)));
    when(service.jobs.childJobs(anyString(), anyString())).thenReturn(java.util.List.of());
  }

  @Test
  void captureNowEnqueuesAndWaitsForTerminalJob() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(
            Optional.of(job("job-1", "JS_QUEUED", 0, 0, 0, "")),
            Optional.of(job("job-1", "JS_RUNNING", 0, 0, 0, "")),
            Optional.of(job("job-1", "JS_SUCCEEDED", 3, 2, 1, "")));

    var response =
        service
            .captureNow(CaptureNowRequest.newBuilder().setScope(captureScope()).build())
            .await()
            .indefinitely();

    assertEquals(3L, response.getTablesScanned());
    assertEquals(2L, response.getTablesChanged());
    assertEquals(1L, response.getErrors());
    verify(service.jobs)
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowFailsWhenQueuedJobFails() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_FAILED", 0, 0, 1, "boom")));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(CaptureNowRequest.newBuilder().setScope(captureScope()).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INTERNAL, ex.getStatus().getCode());
  }

  @Test
  void captureNowTimesOutWhenJobDoesNotFinishWithinWaitBudget() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_QUEUED", 0, 0, 0, "")));
    when(service.jobs.cancel("acct", "job-1", "capture_now timed out while waiting for completion"))
        .thenReturn(Optional.of(job("job-1", "JS_CANCELLING", 0, 0, 0, "timing out")));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setScope(captureScope())
                            .setMaxWait(
                                Duration.newBuilder().setSeconds(0).setNanos(1_000_000).build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode());
    verify(service.jobs, times(1))
        .cancel("acct", "job-1", "capture_now timed out while waiting for completion");
    verify(service.cancellations, times(1)).requestCancel("job-1");
  }

  @Test
  void captureNowEnqueuesWhenNoExecutionExecutorIsAvailableLocally() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_SUCCEEDED", 0, 0, 0, "")));

    service
        .captureNow(CaptureNowRequest.newBuilder().setScope(captureScope()).build())
        .await()
        .indefinitely();

    verify(service.jobs)
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowEnqueuesWhenNoSnapshotPlanningExecutorIsAvailableLocally() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_SUCCEEDED", 0, 0, 0, "")));

    service
        .captureNow(CaptureNowRequest.newBuilder().setScope(captureScope()).build())
        .await()
        .indefinitely();

    verify(service.jobs)
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureEnqueuesWhenNoFileGroupExecutorIsAvailableLocally() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    var response =
        service
            .startCapture(
                ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                    .setScope(captureScope())
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-1", response.getJobId());
  }

  @Test
  void startCaptureRejectsPausedConnector() {
    ResourceId connectorId = accountScopedConnectorId();
    when(service.connectorRepo.getById(connectorId))
        .thenReturn(Optional.of(connector(connectorId, ConnectorState.CS_PAUSED)));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setScope(captureScope())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowRejectsPausedConnector() {
    ResourceId connectorId = accountScopedConnectorId();
    when(service.connectorRepo.getById(connectorId))
        .thenReturn(Optional.of(connector(connectorId, ConnectorState.CS_PAUSED)));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(CaptureNowRequest.newBuilder().setScope(captureScope()).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureRejectsCaptureOnlyViewScopeEarly() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY)
                            .setScope(
                                captureScope().toBuilder().setDestinationViewId("view-1").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowCaptureOnlyEnqueuesWithoutLocalTablePlannerExecutor() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_SUCCEEDED", 0, 0, 0, "")));

    service
        .captureNow(
            CaptureNowRequest.newBuilder()
                .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY)
                .setScope(captureScope())
                .build())
        .await()
        .indefinitely();

    verify(service.jobs)
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureViewScopeEnqueuesWithoutLocalViewPlannerExecutor() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    var response =
        service
            .startCapture(
                ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY)
                    .setScope(
                        CaptureScope.newBuilder()
                            .setConnectorId(connectorId())
                            .setDestinationViewId("view-1")
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-1", response.getJobId());
  }

  @Test
  void startCaptureTableScopeEnqueuesWithoutLocalTablePlannerExecutor() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    var response =
        service
            .startCapture(
                ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY)
                    .setScope(
                        CaptureScope.newBuilder()
                            .setConnectorId(connectorId())
                            .setDestinationTableId("table-1")
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-1", response.getJobId());
  }

  @Test
  void startCaptureBroadMetadataEnqueuesWithoutLocalViewPlannerExecutor() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    var response =
        service
            .startCapture(
                ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY)
                    .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-1", response.getJobId());
  }

  @Test
  void startCaptureRejectsCaptureOnlyScopedRequestsWithNamespaceScope() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY)
                            .setScope(
                                captureScope().toBuilder()
                                    .addDestinationNamespaceIds("ns-1")
                                    .addDestinationCaptureRequests(
                                        ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest
                                            .newBuilder()
                                            .setTableId("table-1")
                                            .setSnapshotId(1L)
                                            .setTargetSpec("table")
                                            .build())
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureRejectsMismatchedTableScopedCaptureRequests() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY)
                            .setScope(
                                captureScope().toBuilder()
                                    .setDestinationTableId("table-a")
                                    .addDestinationCaptureRequests(
                                        ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest
                                            .newBuilder()
                                            .setTableId("table-b")
                                            .setSnapshotId(1L)
                                            .setTargetSpec("table")
                                            .build())
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowDoesNotRequestExecutorCancellationWhenJobAlreadyCancelledInStore() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1"))
        .thenReturn(Optional.of(job("job-1", "JS_QUEUED", 0, 0, 0, "")));
    when(service.jobs.cancel("acct", "job-1", "capture_now timed out while waiting for completion"))
        .thenReturn(Optional.of(job("job-1", "JS_CANCELLED", 0, 0, 0, "cancelled")));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .captureNow(
                    CaptureNowRequest.newBuilder()
                        .setScope(captureScope())
                        .setMaxWait(Duration.newBuilder().setSeconds(0).setNanos(1_000_000).build())
                        .build())
                .await()
                .indefinitely());

    verify(service.jobs, times(1))
        .cancel("acct", "job-1", "capture_now timed out while waiting for completion");
    verify(service.cancellations, never()).requestCancel(anyString());
  }

  @Test
  void captureNowCancelsActiveChildrenWhenPlanJobAlreadySucceeded() {
    var planJob = job("job-1", "JS_SUCCEEDED", 0, 0, 0, "");
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1")).thenReturn(Optional.of(planJob), Optional.of(planJob));
    when(service.jobs.cancel("acct", "job-1", "capture_now timed out while waiting for completion"))
        .thenReturn(Optional.empty());
    when(service.jobs.childJobs("acct", "job-1"))
        .thenReturn(
            java.util.List.of(
                childJob("child-1", "JS_RUNNING", 0, 0, 0, "", "job-1"),
                childJob("child-2", "JS_QUEUED", 0, 0, 0, "", "job-1")));
    when(service.jobs.cancel(
            "acct", "child-1", "capture_now timed out while waiting for completion"))
        .thenReturn(
            Optional.of(childJob("child-1", "JS_CANCELLING", 0, 0, 0, "timing out", "job-1")));
    when(service.jobs.cancel(
            "acct", "child-2", "capture_now timed out while waiting for completion"))
        .thenReturn(
            Optional.of(childJob("child-2", "JS_CANCELLED", 0, 0, 0, "timing out", "job-1")));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .captureNow(
                    CaptureNowRequest.newBuilder()
                        .setScope(captureScope())
                        .setMaxWait(Duration.newBuilder().setSeconds(0).setNanos(1_000_000).build())
                        .build())
                .await()
                .indefinitely());

    verify(service.jobs, times(1))
        .cancel("acct", "child-1", "capture_now timed out while waiting for completion");
    verify(service.jobs, times(1))
        .cancel("acct", "child-2", "capture_now timed out while waiting for completion");
    verify(service.cancellations, times(1)).requestCancel("child-1");
  }

  @Test
  void getReconcileJobPrefersFailedPlanStateOverQueuedChildren() {
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(job("plan-1", "JS_FAILED", 0, 0, 0, "planning failed")));
    when(service.jobs.childJobs("acct", "plan-1"))
        .thenReturn(
            java.util.List.of(
                childJob("child-1", "JS_QUEUED", 1, 0, 0, "", "plan-1"),
                childJob("child-2", "JS_RUNNING", 2, 0, 0, "", "plan-1")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_FAILED, response.getState());
    assertEquals("planning failed", response.getMessage());
    assertEquals(3L, response.getTablesScanned());
  }

  @Test
  void getReconcileJobKeepsRunningPlanStateEvenWhenChildrenHaveSucceeded() {
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(job("plan-1", "JS_RUNNING", 4, 0, 0, "")));
    when(service.jobs.childJobs("acct", "plan-1"))
        .thenReturn(
            java.util.List.of(
                childJob("child-1", "JS_SUCCEEDED", 2, 1, 0, "", "plan-1"),
                childJob("child-2", "JS_SUCCEEDED", 3, 2, 0, "", "plan-1")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING, response.getState());
    assertEquals(5L, response.getTablesScanned());
    assertEquals(3L, response.getTablesChanged());
  }

  @Test
  void getReconcileJobKeepsConnectorPlanRunningWhileNestedSnapshotChildrenRun() {
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(job("plan-1", "JS_SUCCEEDED", 0, 0, 0, "")));
    when(service.jobs.childJobs("acct", "plan-1"))
        .thenReturn(
            java.util.List.of(childJob("table-plan-1", "JS_SUCCEEDED", 1, 1, 0, "", "plan-1")));
    when(service.jobs.childJobs("acct", "table-plan-1"))
        .thenReturn(
            java.util.List.of(
                snapshotChildJob("snapshot-1", "JS_RUNNING", 0, 0, 0, "", "table-plan-1")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING, response.getState());
    assertEquals(0L, response.getFinishedAt().getSeconds());
  }

  @Test
  void captureNowRejectsCaptureModeWithoutCapturePolicy() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setScope(
                                CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowRejectsCaptureModeWithEmptyCapturePolicy() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setScope(
                                CaptureScope.newBuilder()
                                    .setConnectorId(connectorId())
                                    .setCapturePolicy(CapturePolicy.newBuilder().build())
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void getReconcileJobDoesNotDoubleCountPlannerProgress() {
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(job("plan-1", "JS_SUCCEEDED", 7, 4, 0, "")));
    when(service.jobs.childJobs("acct", "plan-1"))
        .thenReturn(java.util.List.of(childJob("child-1", "JS_SUCCEEDED", 3, 2, 0, "", "plan-1")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_SUCCEEDED, response.getState());
    assertEquals(3L, response.getTablesScanned());
    assertEquals(2L, response.getTablesChanged());
  }

  @Test
  void getReconcileJobReportsSnapshotFileGroupCompletionCounts() {
    var snapshotJob =
        new ReconcileJobStore.ReconcileJob(
            "snapshot-1",
            "acct",
            "connector-1",
            "JS_SUCCEEDED",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            null,
            null,
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            null,
            null,
            ReconcileSnapshotTask.of(
                "table-1",
                100L,
                "ns",
                "tbl",
                java.util.List.of(
                    ReconcileFileGroupTask.of(
                        "snapshot-1", "group-1", "table-1", 100L, java.util.List.of("a.parquet")),
                    ReconcileFileGroupTask.of(
                        "snapshot-1",
                        "group-2",
                        "table-1",
                        100L,
                        java.util.List.of("b.parquet", "c.parquet")))),
            ReconcileFileGroupTask.empty(),
            "plan-table-1");
    when(service.jobs.get("acct", "snapshot-1")).thenReturn(Optional.of(snapshotJob));
    when(service.jobs.childJobs("acct", "snapshot-1"))
        .thenReturn(
            java.util.List.of(
                fileGroupChildJob("group-job-1", "JS_SUCCEEDED", "snapshot-1", "group-1"),
                fileGroupChildJob("group-job-2", "JS_FAILED", "snapshot-1", "group-2")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("snapshot-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_FAILED, response.getState());
    assertEquals(2L, response.getFileGroupsTotal());
    assertEquals(1L, response.getFileGroupsCompleted());
    assertEquals(1L, response.getFileGroupsFailed());
    assertEquals(3L, response.getFilesTotal());
    assertEquals(1L, response.getFilesCompleted());
    assertEquals(2L, response.getFilesFailed());
  }

  @Test
  void getReconcileJobUsesPerFileResultsForPartialFailureCounts() {
    var snapshotJob =
        new ReconcileJobStore.ReconcileJob(
            "snapshot-2",
            "acct",
            "connector-1",
            "JS_FAILED",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            null,
            null,
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            null,
            null,
            ReconcileSnapshotTask.of(
                "table-1",
                100L,
                "ns",
                "tbl",
                java.util.List.of(
                    ReconcileFileGroupTask.of(
                        "snapshot-2",
                        "group-1",
                        "table-1",
                        100L,
                        java.util.List.of("a.parquet", "b.parquet")))),
            ReconcileFileGroupTask.empty(),
            "plan-table-1");
    when(service.jobs.get("acct", "snapshot-2")).thenReturn(Optional.of(snapshotJob));
    when(service.jobs.childJobs("acct", "snapshot-2"))
        .thenReturn(
            java.util.List.of(
                new ReconcileJobStore.ReconcileJob(
                    "group-job-1",
                    "acct",
                    "connector-1",
                    "JS_FAILED",
                    "",
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode
                        .METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    null,
                    null,
                    "executor-1",
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    null,
                    null,
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.of(
                        "snapshot-2",
                        "group-1",
                        "table-1",
                        100L,
                        java.util.List.of(),
                        java.util.List.of(
                            ReconcileFileResult.succeeded(
                                "a.parquet",
                                2L,
                                ReconcileIndexArtifactResult.of(
                                    "s3://bucket/index/a.parquet.index", "parquet", 1)),
                            ReconcileFileResult.failed("b.parquet", "boom"))),
                    "snapshot-2")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("snapshot-2").build())
            .await()
            .indefinitely();

    assertEquals(2L, response.getFilesTotal());
    assertEquals(1L, response.getFilesCompleted());
    assertEquals(1L, response.getFilesFailed());
  }

  @Test
  void getReconcileJobReturnsIndexArtifactOnExecFileGroupResult() {
    var execJob =
        new ReconcileJobStore.ReconcileJob(
            "group-job-1",
            "acct",
            "connector-1",
            "JS_SUCCEEDED",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            null,
            null,
            "executor-1",
            ReconcileJobKind.EXEC_FILE_GROUP,
            null,
            null,
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "snapshot-2",
                "group-1",
                "table-1",
                100L,
                java.util.List.of(),
                java.util.List.of(
                    ReconcileFileResult.succeeded(
                        "a.parquet",
                        2L,
                        ReconcileIndexArtifactResult.of(
                            "s3://bucket/index/a.parquet.index", "parquet", 1)))),
            "snapshot-2");
    when(service.jobs.get("acct", "group-job-1")).thenReturn(Optional.of(execJob));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("group-job-1").build())
            .await()
            .indefinitely();

    assertEquals(
        "s3://bucket/index/a.parquet.index",
        response.getFileGroupTask().getFileResults(0).getIndexArtifact().getArtifactUri());
  }

  @Test
  void cancelReconcileJobCancelsActiveChildrenWhenPlanJobAlreadySucceeded() {
    var planJob = job("plan-1", "JS_SUCCEEDED", 0, 0, 0, "");
    var childOne = childJob("child-1", "JS_RUNNING", 0, 0, 0, "", "plan-1");
    var childTwo = childJob("child-2", "JS_QUEUED", 0, 0, 0, "", "plan-1");
    when(service.jobs.cancel("acct", "plan-1", "stop")).thenReturn(Optional.empty());
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(planJob), Optional.of(planJob), Optional.of(planJob));
    when(service.jobs.childJobs("acct", "plan-1"))
        .thenReturn(
            java.util.List.of(childOne, childTwo),
            java.util.List.of(
                childJob("child-1", "JS_CANCELLING", 0, 0, 0, "stop", "plan-1"),
                childJob("child-2", "JS_CANCELLED", 0, 0, 0, "stop", "plan-1")));
    when(service.jobs.cancel("acct", "child-1", "stop"))
        .thenReturn(Optional.of(childJob("child-1", "JS_CANCELLING", 0, 0, 0, "stop", "plan-1")));
    when(service.jobs.cancel("acct", "child-2", "stop"))
        .thenReturn(Optional.of(childJob("child-2", "JS_CANCELLED", 0, 0, 0, "stop", "plan-1")));

    var response =
        service
            .cancelReconcileJob(
                CancelReconcileJobRequest.newBuilder().setJobId("plan-1").setReason("stop").build())
            .await()
            .indefinitely();

    assertEquals(
        ai.floedb.floecat.reconciler.rpc.JobState.JS_CANCELLING, response.getJob().getState());
    verify(service.jobs).cancel("acct", "child-1", "stop");
    verify(service.jobs).cancel("acct", "child-2", "stop");
    verify(service.cancellations).requestCancel("child-1");
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();
  }

  private static ResourceId accountScopedConnectorId() {
    return connectorId().toBuilder().setAccountId("acct").build();
  }

  private static Connector connector(ResourceId connectorId, ConnectorState state) {
    return Connector.newBuilder().setResourceId(connectorId).setState(state).build();
  }

  private static CaptureScope captureScope() {
    return CaptureScope.newBuilder()
        .setConnectorId(connectorId())
        .setCapturePolicy(
            CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_TABLE_STATS)
                .addOutputs(CaptureOutput.CO_FILE_STATS)
                .addOutputs(CaptureOutput.CO_COLUMN_STATS)
                .build())
        .build();
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId, String state, long scanned, long changed, long errors, String message) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        "connector-1",
        state,
        message,
        0L,
        0L,
        scanned,
        changed,
        0L,
        0L,
        errors,
        false,
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        null,
        null,
        "",
        ReconcileJobKind.PLAN_CONNECTOR,
        null,
        null,
        "");
  }

  private static ReconcileJobStore.ReconcileJob childJob(
      String jobId,
      String state,
      long scanned,
      long changed,
      long errors,
      String message,
      String parentJobId) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        "connector-1",
        state,
        message,
        0L,
        0L,
        scanned,
        changed,
        0L,
        0L,
        errors,
        false,
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        null,
        null,
        "executor-1",
        ReconcileJobKind.PLAN_TABLE,
        null,
        null,
        parentJobId);
  }

  private static ReconcileJobStore.ReconcileJob snapshotChildJob(
      String jobId,
      String state,
      long scanned,
      long changed,
      long errors,
      String message,
      String parentJobId) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        "connector-1",
        state,
        message,
        0L,
        0L,
        scanned,
        changed,
        0L,
        0L,
        errors,
        false,
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        null,
        null,
        "executor-1",
        ReconcileJobKind.PLAN_SNAPSHOT,
        null,
        null,
        ReconcileSnapshotTask.of("table-1", 100L, "ns", "tbl", java.util.List.of()),
        ReconcileFileGroupTask.empty(),
        parentJobId);
  }

  private static ReconcileJobStore.ReconcileJob fileGroupChildJob(
      String jobId, String state, String parentJobId, String groupId) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        "connector-1",
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
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        null,
        null,
        "executor-1",
        ReconcileJobKind.EXEC_FILE_GROUP,
        null,
        null,
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of("snapshot-1", groupId, "table-1", 100L, java.util.List.of()),
        parentJobId);
  }
}
