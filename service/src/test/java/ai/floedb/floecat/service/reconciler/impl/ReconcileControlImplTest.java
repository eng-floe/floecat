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
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
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
        .thenReturn(Optional.of(Connector.newBuilder().setResourceId(connectorId).build()));
  }

  @Test
  void captureNowRejectsSnapshotScopeWithoutSingleTableTarget() {
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
                                    .addDestinationSnapshotIds(10L)
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void startCaptureRejectsSnapshotScopeWithoutSingleNamespacePath() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        StartCaptureRequest.newBuilder()
                            .setScope(
                                CaptureScope.newBuilder()
                                    .setConnectorId(connectorId())
                                    .setDestinationTableDisplayName("orders")
                                    .addDestinationSnapshotIds(10L)
                                    .addDestinationNamespacePaths(
                                        NamespacePath.newBuilder().addSegments("db1").build())
                                    .addDestinationNamespacePaths(
                                        NamespacePath.newBuilder().addSegments("db2").build())
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void captureNowEnqueuesAndWaitsForTerminalJob() {
    when(service.jobs.enqueue(
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
                    .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                    .build())
            .await()
            .indefinitely();

    assertEquals(3L, response.getTablesScanned());
    assertEquals(2L, response.getTablesChanged());
    assertEquals(1L, response.getErrors());
    verify(service.jobs)
        .enqueue(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  @Test
  void captureNowFailsWhenQueuedJobFails() {
    when(service.jobs.enqueue(
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
                            .setScope(
                                CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INTERNAL, ex.getStatus().getCode());
  }

  @Test
  void captureNowTimesOutWhenJobDoesNotFinishWithinWaitBudget() {
    when(service.jobs.enqueue(
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
                            .setScope(
                                CaptureScope.newBuilder().setConnectorId(connectorId()).build())
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
  void captureNowDoesNotRequestExecutorCancellationWhenJobAlreadyCancelledInStore() {
    when(service.jobs.enqueue(
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
                        .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                        .setMaxWait(Duration.newBuilder().setSeconds(0).setNanos(1_000_000).build())
                        .build())
                .await()
                .indefinitely());

    verify(service.jobs, times(1))
        .cancel("acct", "job-1", "capture_now timed out while waiting for completion");
    verify(service.cancellations, never()).requestCancel(anyString());
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();
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
        errors,
        false,
        null,
        0L,
        0L,
        null,
        null,
        "");
  }
}
