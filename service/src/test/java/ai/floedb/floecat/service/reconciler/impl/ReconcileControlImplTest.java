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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.capture.rpc.CaptureColumnPolicy;
import ai.floedb.floecat.capture.rpc.CaptureOutput;
import ai.floedb.floecat.capture.rpc.CapturePolicy;
import ai.floedb.floecat.capture.rpc.DefaultColumnScope;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobTreeRequest;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsRequest;
import ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore;
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
import org.mockito.ArgumentCaptor;

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
    when(service.connectorRepo.existsById(any())).thenReturn(true);
    when(service.jobs.childJobsPage(anyString(), anyString(), anyInt(), anyString()))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(), ""));
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
            .captureNow(
                CaptureNowRequest.newBuilder()
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
                    .setScope(captureScope())
                    .build())
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
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScope())
                            .build())
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
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
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
        .captureNow(
            CaptureNowRequest.newBuilder()
                .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
                .setScope(captureScope())
                .build())
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
        .captureNow(
            CaptureNowRequest.newBuilder()
                .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
                .setScope(captureScope())
                .build())
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
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
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
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScope())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureRejectsDeletedConnectorAtEnqueueBoundary() {
    ResourceId connectorId = accountScopedConnectorId();
    when(service.connectorRepo.getById(connectorId))
        .thenReturn(Optional.of(connector(connectorId, ConnectorState.CS_ACTIVE)));
    when(service.connectorRepo.existsById(connectorId)).thenReturn(false);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScope())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureMapsDeletedConnectorRaceDuringEnqueueToNotFound() {
    ResourceId connectorId = accountScopedConnectorId();
    when(service.connectorRepo.getById(connectorId))
        .thenReturn(Optional.of(connector(connectorId, ConnectorState.CS_ACTIVE)));
    when(service.connectorRepo.existsById(connectorId)).thenReturn(true);
    when(service.jobs.enqueuePlan(
            eq(connectorId.getAccountId()),
            eq(connectorId.getId()),
            anyBoolean(),
            any(),
            any(),
            any(),
            anyString()))
        .thenThrow(new DurableReconcileJobStore.ConnectorDeletedException(connectorId.getId()));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScope())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
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
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScope())
                            .build())
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
  void startCaptureUsesConnectorAutoCapturePolicyWhenRequestOmitsPolicy() {
    ResourceId connectorId = accountScopedConnectorId();
    when(service.connectorRepo.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setState(ConnectorState.CS_ACTIVE)
                    .setPolicy(
                        ai.floedb.floecat.connector.rpc.ReconcilePolicy.newBuilder()
                            .setAutoCapturePolicy(
                                CapturePolicy.newBuilder()
                                    .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX)
                                    .setDefaultColumnScope(DefaultColumnScope.DCS_EXPLICIT_ONLY)
                                    .setMaxDefaultColumns(7)
                                    .build())
                            .build())
                    .build()));
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    service
        .startCapture(
            ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
                .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                .build())
        .await()
        .indefinitely();

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(service.jobs)
        .enqueuePlan(
            anyString(),
            anyString(),
            anyBoolean(),
            any(),
            scopeCaptor.capture(),
            any(),
            anyString());
    assertEquals(
        java.util.Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
        scopeCaptor.getValue().capturePolicy().outputs());
    assertEquals(
        ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
        scopeCaptor.getValue().capturePolicy().defaultColumnScope());
    assertEquals(7, scopeCaptor.getValue().capturePolicy().maxDefaultColumns());
  }

  @Test
  void startCaptureDefaultsOmittedSnapshotSelectionToCurrent() {
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");

    service
        .startCapture(
            ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY)
                .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                .build())
        .await()
        .indefinitely();

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(service.jobs)
        .enqueuePlan(
            anyString(),
            anyString(),
            anyBoolean(),
            any(),
            scopeCaptor.capture(),
            any(),
            anyString());

    assertEquals(
        ReconcileSnapshotSelection.Kind.CURRENT, scopeCaptor.getValue().snapshotSelection().kind());
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
                        .setMode(
                            ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
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
  void captureNowReturnsSucceededParentWithoutDescendingIntoChildren() {
    var planJob = job("job-1", "JS_SUCCEEDED", 0, 0, 0, "");
    when(service.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenReturn("job-1");
    when(service.jobs.get("acct", "job-1")).thenReturn(Optional.of(planJob));

    var response =
        service
            .captureNow(
                CaptureNowRequest.newBuilder()
                    .setMode(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE)
                    .setScope(captureScope())
                    .setMaxWait(Duration.newBuilder().setSeconds(0).setNanos(1_000_000).build())
                    .build())
            .await()
            .indefinitely();

    assertEquals(0L, response.getTablesScanned());
    verify(service.jobs, never())
        .cancel("acct", "job-1", "capture_now timed out while waiting for completion");
    verify(service.jobs, never()).childJobsPage("acct", "job-1", 200, "");
  }

  @Test
  void getReconcileJobPrefersFailedPlanStateOverQueuedChildren() {
    when(service.jobs.get("acct", "plan-1"))
        .thenReturn(Optional.of(job("plan-1", "JS_FAILED", 3, 0, 0, "planning failed")));

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
        .thenReturn(Optional.of(job("plan-1", "JS_RUNNING", 5, 3, 0, "")));

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
        .thenReturn(Optional.of(job("plan-1", "JS_RUNNING", 1, 1, 0, "")));

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
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
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
                                    .setCapturePolicy(
                                        ai.floedb.floecat.capture.rpc.CapturePolicy.newBuilder()
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
  void startCaptureRejectsColumnPolicyWithNoEnabledOutputs() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScopeWithDisabledColumnPolicy())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowRejectsColumnPolicyWithNoEnabledOutputs() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScopeWithDisabledColumnPolicy())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureRejectsUnspecifiedDefaultColumnScope() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(
                                captureScopeWithDefaultColumnScope(
                                    DefaultColumnScope.DCS_UNSPECIFIED))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowRejectsUnrecognizedDefaultColumnScope() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScopeWithDefaultColumnScopeValue(99))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(service.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void startCaptureRejectsZeroMaxDefaultColumns() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        ai.floedb.floecat.reconciler.rpc.StartCaptureRequest.newBuilder()
                            .setMode(
                                ai.floedb.floecat.reconciler.rpc.CaptureMode
                                    .CM_METADATA_AND_CAPTURE)
                            .setScope(captureScopeWithMaxDefaultColumns(0))
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
        .thenReturn(Optional.of(job("plan-1", "JS_SUCCEEDED", 3, 2, 0, "")));

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
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            100L,
            "ns",
            "tbl",
            java.util.List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/snapshot-1/snapshot-plan/plan.json",
            2);
    ReconcileFileGroupTask groupOne =
        ReconcileFileGroupTask.of(
            "snapshot-1", "group-1", "table-1", 100L, java.util.List.of("a.parquet"));
    ReconcileFileGroupTask groupTwo =
        ReconcileFileGroupTask.of(
            "snapshot-1", "group-2", "table-1", 100L, java.util.List.of("b.parquet", "c.parquet"));
    var snapshotJob =
        new ReconcileJobStore.ReconcileJob(
            "snapshot-1",
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
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            2L,
            3L,
            1L,
            1L,
            1L,
            2L,
            "plan-table-1");
    when(service.jobs.get("acct", "snapshot-1")).thenReturn(Optional.of(snapshotJob));

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
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            100L,
            "ns",
            "tbl",
            java.util.List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/snapshot-2/snapshot-plan/plan.json",
            1);
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "snapshot-2", "group-1", "table-1", 100L, java.util.List.of("a.parquet", "b.parquet"));
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
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            1L,
            2L,
            0L,
            1L,
            1L,
            1L,
            "plan-table-1");
    when(service.jobs.get("acct", "snapshot-2")).thenReturn(Optional.of(snapshotJob));

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
  void listReconcileJobsSkipsExpensiveSnapshotFileGroupCounts() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            100L,
            "ns",
            "tbl",
            java.util.List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/snapshot-1/snapshot-plan/plan.json",
            2);
    var snapshotJob =
        new ReconcileJobStore.ReconcileJob(
            "snapshot-1",
            "acct",
            "connector-1",
            "JS_RUNNING",
            "",
            0L,
            0L,
            0L,
            0L,
            0L,
            7L,
            0L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            1L,
            2L,
            null,
            null,
            "",
            ReconcileJobKind.PLAN_SNAPSHOT,
            null,
            null,
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            "plan-table-1");
    when(service.jobs.list("acct", 100, "", "", java.util.Set.of()))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(snapshotJob), ""));

    var response =
        service
            .listReconcileJobs(ListReconcileJobsRequest.newBuilder().build())
            .await()
            .indefinitely();

    assertEquals(1, response.getJobsCount());
    assertEquals(1L, response.getJobs(0).getSnapshotsProcessed());
    assertEquals(2L, response.getJobs(0).getStatsProcessed());
    assertEquals(0L, response.getJobs(0).getFileGroupsTotal());
    assertEquals(0L, response.getJobs(0).getFilesTotal());
    verify(service.jobs, never()).childJobsPage("acct", "snapshot-1", 200, "");
  }

  @Test
  void listReconcileJobsUsesStoredConnectorAggregateSummaryWithoutChildTraversal() {
    var connectorJob =
        new ReconcileJobStore.ReconcileJob(
            "plan-1",
            "acct",
            "connector-1",
            "JS_SUCCEEDED",
            "Succeeded",
            10L,
            30L,
            2L,
            2L,
            0L,
            0L,
            0L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            1L,
            1L,
            0L,
            true,
            ReconcileScope.empty(),
            null,
            "",
            "executor-2",
            ReconcileJobKind.PLAN_CONNECTOR,
            null,
            null,
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            2L,
            3L,
            1L,
            0L,
            3L,
            0L,
            "");

    when(service.jobs.list("acct", 100, "", "", java.util.Set.of()))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(connectorJob), ""));

    var response =
        service
            .listReconcileJobs(ListReconcileJobsRequest.newBuilder().build())
            .await()
            .indefinitely();

    assertEquals(1, response.getJobsCount());
    assertEquals(
        ai.floedb.floecat.reconciler.rpc.JobState.JS_SUCCEEDED, response.getJobs(0).getState());
    assertEquals("Succeeded", response.getJobs(0).getMessage());
    assertEquals(2L, response.getJobs(0).getTablesScanned());
    assertEquals(2L, response.getJobs(0).getTablesChanged());
    assertEquals(1L, response.getJobs(0).getSnapshotsProcessed());
    assertEquals(1L, response.getJobs(0).getStatsProcessed());
    verify(service.jobs, never()).childJobsPage("acct", "plan-1", 200, "");
  }

  @Test
  void getReconcileJobTreeUsesDedicatedStoreTraversal() {
    var root = job("plan-1", "JS_RUNNING", 1, 0, 0, "");
    var child = childJob("table-1", "JS_QUEUED", 0, 0, 0, "", "plan-1");
    when(service.jobs.jobTree("acct", "plan-1")).thenReturn(java.util.List.of(root, child));

    var response =
        service
            .getReconcileJobTree(GetReconcileJobTreeRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(2, response.getJobsCount());
    assertEquals("plan-1", response.getJobs(0).getJobId());
    assertEquals("table-1", response.getJobs(1).getJobId());
    verify(service.jobs).jobTree("acct", "plan-1");
    verify(service.jobs, never()).list("acct", 100, "", "", java.util.Set.of());
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
  void cancelReconcileJobRejectsAlreadySucceededParentWhenStoreDidNotCancelIt() {
    var planJob = job("plan-1", "JS_SUCCEEDED", 0, 0, 0, "");
    when(service.jobs.cancel("acct", "plan-1", "stop")).thenReturn(Optional.empty());
    when(service.jobs.get("acct", "plan-1")).thenReturn(Optional.of(planJob));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .cancelReconcileJob(
                        CancelReconcileJobRequest.newBuilder()
                            .setJobId("plan-1")
                            .setReason("stop")
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(service.jobs, never()).childJobsPage("acct", "plan-1", 200, "");
  }

  @Test
  void cancelReconcileJobCancelsQueuedGrandchildrenOfCancelledPlanChildren() {
    var connector = job("plan-1", "JS_CANCELLED", 0, 0, 0, "");
    var table = childJob("table-1", "JS_CANCELLED", 0, 0, 0, "", "plan-1");
    var snapshot = snapshotChildJob("snapshot-1", "JS_CANCELLED", 0, 0, 0, "", "table-1");
    var fileGroup =
        new ReconcileJobStore.ReconcileJob(
            "file-1",
            "acct",
            "connector-1",
            "JS_CANCELLED",
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
            "executor-1",
            ReconcileJobKind.EXEC_FILE_GROUP,
            null,
            null,
            null,
            ReconcileFileGroupTask.empty(),
            "snapshot-1");
    when(service.jobs.cancel("acct", "plan-1", "stop")).thenReturn(Optional.of(connector));
    when(service.jobs.cancel("acct", "table-1", "stop")).thenReturn(Optional.of(table));
    when(service.jobs.cancel("acct", "snapshot-1", "stop")).thenReturn(Optional.of(snapshot));
    when(service.jobs.cancel("acct", "file-1", "stop")).thenReturn(Optional.of(fileGroup));
    when(service.jobs.get("acct", "plan-1")).thenReturn(Optional.of(connector));
    when(service.jobs.get("acct", "table-1")).thenReturn(Optional.of(table));
    when(service.jobs.get("acct", "snapshot-1")).thenReturn(Optional.of(snapshot));
    when(service.jobs.childJobsPage("acct", "plan-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(table), ""));
    when(service.jobs.childJobsPage("acct", "table-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(snapshot), ""));
    when(service.jobs.childJobsPage("acct", "snapshot-1", 200, ""))
        .thenReturn(new ReconcileJobStore.ReconcileJobPage(java.util.List.of(fileGroup), ""));

    var response =
        service
            .cancelReconcileJob(
                CancelReconcileJobRequest.newBuilder().setJobId("plan-1").setReason("stop").build())
            .await()
            .indefinitely();

    assertEquals(true, response.getCancelled());
    verify(service.jobs).cancel("acct", "table-1", "stop");
    verify(service.jobs).cancel("acct", "snapshot-1", "stop");
    verify(service.jobs).cancel("acct", "file-1", "stop");
  }

  @Test
  void getReconcileJobUsesPreAggregatedSummaryWithoutChildTraversal() {
    var aggregateJob =
        new ReconcileJobStore.ReconcileJob(
            "plan-1",
            "acct",
            "connector-1",
            "JS_FAILED",
            "child-1: boom",
            10L,
            20L,
            3L,
            2L,
            4L,
            1L,
            5L,
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            6L,
            7L,
            8L,
            true,
            ReconcileScope.empty(),
            null,
            "",
            "worker-1",
            ReconcileJobKind.PLAN_CONNECTOR,
            null,
            null,
            null,
            ReconcileFileGroupTask.empty(),
            "");
    when(service.jobs.get("acct", "plan-1")).thenReturn(Optional.of(aggregateJob));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_FAILED, response.getState());
    assertEquals(3L, response.getTablesScanned());
    assertEquals(2L, response.getTablesChanged());
    assertEquals(4L, response.getViewsScanned());
    assertEquals(1L, response.getViewsChanged());
    assertEquals(5L, response.getErrors());
    assertEquals(6L, response.getSnapshotsProcessed());
    assertEquals(7L, response.getStatsProcessed());
    assertEquals(8L, response.getIndexesProcessed());
    assertEquals("worker-1", response.getExecutorId());
    verify(service.jobs, never()).childJobsPage("acct", "plan-1", 200, "");
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
            ai.floedb.floecat.capture.rpc.CapturePolicy.newBuilder()
                .addOutputs(ai.floedb.floecat.capture.rpc.CaptureOutput.CO_TABLE_STATS)
                .addOutputs(ai.floedb.floecat.capture.rpc.CaptureOutput.CO_FILE_STATS)
                .addOutputs(ai.floedb.floecat.capture.rpc.CaptureOutput.CO_COLUMN_STATS)
                .setDefaultColumnScope(DefaultColumnScope.DCS_FIRST_N)
                .setMaxDefaultColumns(32)
                .build())
        .build();
  }

  private static CaptureScope captureScopeWithDisabledColumnPolicy() {
    return CaptureScope.newBuilder()
        .setConnectorId(connectorId())
        .setCapturePolicy(
            CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_TABLE_STATS)
                .setDefaultColumnScope(DefaultColumnScope.DCS_FIRST_N)
                .setMaxDefaultColumns(32)
                .addColumns(CaptureColumnPolicy.newBuilder().setSelector("c1").build())
                .build())
        .build();
  }

  private static CaptureScope captureScopeWithDefaultColumnScope(DefaultColumnScope scope) {
    return CaptureScope.newBuilder()
        .setConnectorId(connectorId())
        .setCapturePolicy(
            CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_TABLE_STATS)
                .setDefaultColumnScope(scope)
                .setMaxDefaultColumns(32)
                .build())
        .build();
  }

  private static CaptureScope captureScopeWithDefaultColumnScopeValue(int scopeValue) {
    return CaptureScope.newBuilder()
        .setConnectorId(connectorId())
        .setCapturePolicy(
            CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_TABLE_STATS)
                .setDefaultColumnScopeValue(scopeValue)
                .setMaxDefaultColumns(32)
                .build())
        .build();
  }

  private static CaptureScope captureScopeWithMaxDefaultColumns(int maxDefaultColumns) {
    return CaptureScope.newBuilder()
        .setConnectorId(connectorId())
        .setCapturePolicy(
            CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_TABLE_STATS)
                .setDefaultColumnScope(DefaultColumnScope.DCS_FIRST_N)
                .setMaxDefaultColumns(maxDefaultColumns)
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
        0L,
        true,
        null,
        null,
        "",
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
