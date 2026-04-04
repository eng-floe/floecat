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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
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
    service.reconcilerService = mock(ReconcilerService.class);
    service.cancellations = mock(ReconcileCancellationRegistry.class);
    service.settings = mock(ReconcilerSettingsStore.class);

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
  void startCapturePassesExecutionPolicyToQueue() {
    when(service.jobs.enqueuePlan(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_STATS),
            any(ReconcileScope.class),
            eq(
                ReconcileExecutionPolicy.of(
                    ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of())),
            eq("")))
        .thenReturn("job-123");

    var response =
        service
            .startCapture(
                StartCaptureRequest.newBuilder()
                    .setScope(CaptureScope.newBuilder().setConnectorId(connectorId()).build())
                    .setExecutionPolicy(
                        ai.floedb.floecat.reconciler.rpc.ExecutionPolicy.newBuilder()
                            .setExecutionClass(
                                ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_HEAVY)
                            .setLane("remote")
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertEquals("job-123", response.getJobId());
    verify(service.jobs)
        .enqueuePlan(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_STATS),
            any(ReconcileScope.class),
            eq(
                ReconcileExecutionPolicy.of(
                    ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of())),
            eq(""));
  }

  @Test
  void getReconcileJobIncludesExecutionPolicyAndExecutorId() {
    when(service.jobs.get("acct", "job-123"))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-123",
                    "acct",
                    "connector-1",
                    "JS_QUEUED",
                    "",
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_STATS,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.of(
                        ReconcileExecutionClass.INTERACTIVE,
                        "remote",
                        java.util.Map.of("tier", "gold")),
                    "remote-executor",
                    ReconcileJobKind.EXEC_CONNECTOR,
                    ReconcileTableTask.empty(),
                    "")));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("job-123").build())
            .await()
            .indefinitely();

    assertEquals(
        ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_INTERACTIVE,
        response.getExecutionPolicy().getExecutionClass());
    assertEquals("remote", response.getExecutionPolicy().getLane());
    assertEquals("gold", response.getExecutionPolicy().getAttributesOrThrow("tier"));
    assertEquals("remote-executor", response.getExecutorId());
  }

  @Test
  void getReconcileJobAggregatesPlanChildren() {
    var planJob =
        new ReconcileJobStore.ReconcileJob(
            "plan-1",
            "acct",
            "connector-1",
            "JS_SUCCEEDED",
            "Planned 2 table jobs",
            10L,
            20L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");
    var childQueued =
        new ReconcileJobStore.ReconcileJob(
            "child-1",
            "acct",
            "connector-1",
            "JS_QUEUED",
            "Queued",
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            ReconcileJobKind.EXEC_TABLE,
            ReconcileTableTask.of("ns", "table_a"),
            "plan-1");
    var childRunning =
        new ReconcileJobStore.ReconcileJob(
            "child-2",
            "acct",
            "connector-1",
            "JS_RUNNING",
            "Running",
            100L,
            0L,
            3L,
            1L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            7L,
            11L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "default_reconciler",
            ReconcileJobKind.EXEC_TABLE,
            ReconcileTableTask.of("ns", "table_b"),
            "plan-1");

    when(service.jobs.get("acct", "plan-1")).thenReturn(Optional.of(planJob));
    when(service.jobs.list("acct", 200, "", "connector-1", java.util.Set.of()))
        .thenReturn(
            new ReconcileJobStore.ReconcileJobPage(
                List.of(planJob, childQueued, childRunning), ""));

    var response =
        service
            .getReconcileJob(GetReconcileJobRequest.newBuilder().setJobId("plan-1").build())
            .await()
            .indefinitely();

    assertEquals(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING, response.getState());
    assertEquals(3L, response.getTablesScanned());
    assertEquals(1L, response.getTablesChanged());
    assertEquals(7L, response.getSnapshotsProcessed());
    assertEquals(11L, response.getStatsProcessed());
    assertEquals("default_reconciler", response.getExecutorId());
  }

  @Test
  void cancelReconcileJobCancelsPlanChildren() {
    var planCancelling =
        new ReconcileJobStore.ReconcileJob(
            "plan-1",
            "acct",
            "connector-1",
            "JS_CANCELLING",
            "Cancelling",
            10L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");
    var childQueued =
        new ReconcileJobStore.ReconcileJob(
            "child-1",
            "acct",
            "connector-1",
            "JS_QUEUED",
            "Queued",
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            ReconcileJobKind.EXEC_TABLE,
            ReconcileTableTask.of("ns", "table_a"),
            "plan-1");
    var childCancelling =
        new ReconcileJobStore.ReconcileJob(
            "child-2",
            "acct",
            "connector-1",
            "JS_CANCELLING",
            "Cancelling",
            100L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "default_reconciler",
            ReconcileJobKind.EXEC_TABLE,
            ReconcileTableTask.of("ns", "table_b"),
            "plan-1");
    var childCancelled =
        new ReconcileJobStore.ReconcileJob(
            "child-1",
            "acct",
            "connector-1",
            "JS_CANCELLED",
            "stop",
            0L,
            200L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_STATS,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            ReconcileJobKind.EXEC_TABLE,
            ReconcileTableTask.of("ns", "table_a"),
            "plan-1");

    when(service.jobs.cancel("acct", "plan-1", "stop")).thenReturn(Optional.of(planCancelling));
    when(service.jobs.list("acct", 200, "", "connector-1", java.util.Set.of()))
        .thenReturn(
            new ReconcileJobStore.ReconcileJobPage(
                List.of(planCancelling, childQueued, childCancelling), ""));
    when(service.jobs.cancel("acct", "child-1", "stop")).thenReturn(Optional.of(childCancelled));
    when(service.jobs.cancel("acct", "child-2", "stop")).thenReturn(Optional.of(childCancelling));
    when(service.jobs.get("acct", "plan-1")).thenReturn(Optional.of(planCancelling));

    var response =
        service
            .cancelReconcileJob(
                CancelReconcileJobRequest.newBuilder().setJobId("plan-1").setReason("stop").build())
            .await()
            .indefinitely();

    verify(service.jobs).cancel("acct", "child-1", "stop");
    verify(service.jobs).cancel("acct", "child-2", "stop");
    verify(service.cancellations).requestCancel("child-2");
    verify(service.cancellations).requestCancel("plan-1");
    assertEquals(true, response.getCancelled());
    assertEquals(
        ai.floedb.floecat.reconciler.rpc.JobState.JS_CANCELLING, response.getJob().getState());
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();
  }
}
