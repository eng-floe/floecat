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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.LookupTableRequest;
import ai.floedb.floecat.catalog.rpc.LookupTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DeleteConnectorResponse;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsResponse;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.floecat.connector.rpc.ValidateConnectorResponse;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsResponse;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsRequest;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileJobKind;
import ai.floedb.floecat.reconciler.rpc.ReconcileTableTask;
import ai.floedb.floecat.reconciler.rpc.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConnectorCliSupportTest {

  // Must be a valid UUID-formatted string so looksLikeUuid() returns true
  // and resolveConnectorId() uses the direct getConnector path.
  private static final String CONNECTOR_UUID = "00000000-0000-0000-0000-000000000001";

  private static ResourceId connectorId() {
    return ResourceId.newBuilder().setId(CONNECTOR_UUID).build();
  }

  // --- connectors (list all) ---

  @Test
  void connectorsListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorsToReturn.add(
          Connector.newBuilder().setDisplayName("my-conn").build());

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connectors",
          List.of(),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(buf.toString().contains("CONNECTOR_ID"), "expected header");
      assertTrue(buf.toString().contains("my-conn"), "expected display name");
    }
  }

  // --- connector list ---

  @Test
  void connectorListSubcommandPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("list"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(buf.toString().contains("CONNECTOR_ID"));
      assertEquals(1, h.connectorsService.listConnectorsCalls.get());
    }
  }

  // --- connector get ---

  @Test
  void connectorGetCallsServiceAndPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder().setResourceId(connectorId()).setDisplayName("test-conn").build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("get", CONNECTOR_UUID),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.connectorsService.getConnectorCalls.get());
      assertTrue(buf.toString().contains("CONNECTOR_ID"));
      assertTrue(buf.toString().contains("test-conn"));
    }
  }

  @Test
  void connectorGetPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("get"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- connector create ---

  @Test
  void connectorCreateCallsServiceWithRequiredFields() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder().setDisplayName("new-conn").build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("create", "new-conn", "ICEBERG", "s3://bucket", "src.ns", "dest-cat"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.connectorsService.createConnectorCalls.get());
      assertEquals("new-conn", h.connectorsService.lastCreateRequest.getSpec().getDisplayName());
    }
  }

  @Test
  void connectorCreatePrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("create", "only-one-arg"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- connector delete ---

  @Test
  void connectorDeleteCallsServiceAndPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setCatalogId(ResourceId.newBuilder().setId("catalog-1").build())
                      .build())
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("delete", CONNECTOR_UUID),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.connectorsService.deleteConnectorCalls.get());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void connectorDeletePrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("delete"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- connector validate ---

  @Test
  void connectorValidateCallsService() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("validate", "ICEBERG", "s3://bucket"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.connectorsService.validateConnectorCalls.get());
      assertTrue(
          buf.toString().contains("ok")
              || buf.toString().isEmpty()
              || buf.toString().contains("validate"));
    }
  }

  @Test
  void connectorValidatePrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("validate", "ICEBERG"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- connector trigger ---

  @Test
  void connectorTriggerCaptureModeRequiresExplicitOutputs() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder().setResourceId(connectorId()).build();

      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ConnectorCliSupport.handle(
                      "connector",
                      List.of("trigger", CONNECTOR_UUID, "--mode", "capture-only"),
                      new PrintStream(new ByteArrayOutputStream()),
                      h.connectorsStub,
                      h.reconcileControlStub,
                      h.directoryStub,
                      () -> "acct-1"));

      assertTrue(error.getMessage().contains("--capture is required"));
      assertEquals(0, h.reconcileControlService.startCaptureCalls.get());
    }
  }

  @Test
  void connectorTriggerColumnsCreatesScopedCaptureRequest() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setTableId(ResourceId.newBuilder().setId("table-1").build())
                      .setNamespace(NamespacePath.newBuilder().addSegments("ns").build())
                      .build())
              .build();
      h.directoryService.tableDisplayName = "events";
      h.snapshotService.currentSnapshotId = 42L;

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "capture-only",
              "--capture",
              "stats,index",
              "--columns",
              "c1,#7"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.snapshotStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      ScopedCaptureRequest captureRequest = request.getScope().getDestinationCaptureRequests(0);
      assertEquals("table-1", captureRequest.getTableId());
      assertEquals(42L, captureRequest.getSnapshotId());
      assertEquals(List.of("c1", "#7"), captureRequest.getColumnSelectorsList());
      assertEquals(4, request.getScope().getCapturePolicy().getOutputsCount());
      assertEquals("c1", request.getScope().getCapturePolicy().getColumns(0).getSelector());
      assertEquals("#7", request.getScope().getCapturePolicy().getColumns(1).getSelector());
    }
  }

  @Test
  void connectorTriggerCaptureFlagsCreateCapturePolicy() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder().setResourceId(connectorId()).build();

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "metadata-and-capture",
              "--capture",
              "stats,index"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals(4, request.getScope().getCapturePolicy().getOutputsCount());
    }
  }

  @Test
  void connectorTriggerExplicitSnapshotCreatesScopedCaptureRequest() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setTableId(ResourceId.newBuilder().setId("table-1").build())
                      .setNamespace(NamespacePath.newBuilder().addSegments("ns").build())
                      .build())
              .build();
      h.directoryService.tableDisplayName = "events";
      h.snapshotService.currentSnapshotId = 42L;

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "capture-only",
              "--capture",
              "stats,index",
              "--snapshot",
              "99",
              "--columns",
              "c1,#7"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.snapshotStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals(99L, request.getScope().getDestinationCaptureRequests(0).getSnapshotId());
    }
  }

  @Test
  void connectorTriggerExplicitSnapshotWithoutColumnsStillScopesCapture() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setTableId(ResourceId.newBuilder().setId("table-1").build())
                      .setNamespace(NamespacePath.newBuilder().addSegments("ns").build())
                      .build())
              .build();
      h.directoryService.tableDisplayName = "events";

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "capture-only",
              "--capture",
              "stats,index",
              "--snapshot",
              "99"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.snapshotStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals(1, request.getScope().getDestinationCaptureRequestsCount());
      assertEquals(99L, request.getScope().getDestinationCaptureRequests(0).getSnapshotId());
      assertEquals("table", request.getScope().getDestinationCaptureRequests(0).getTargetSpec());
      assertEquals(
          List.of(), request.getScope().getDestinationCaptureRequests(0).getColumnSelectorsList());
    }
  }

  @Test
  void connectorTriggerCurrentWithoutColumnsStillScopesCapture() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setTableId(ResourceId.newBuilder().setId("table-1").build())
                      .setNamespace(NamespacePath.newBuilder().addSegments("ns").build())
                      .build())
              .build();
      h.directoryService.tableDisplayName = "events";
      h.snapshotService.currentSnapshotId = 42L;

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "capture-only",
              "--capture",
              "stats,index",
              "--current"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.snapshotStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals(1, request.getScope().getDestinationCaptureRequestsCount());
      assertEquals(42L, request.getScope().getDestinationCaptureRequests(0).getSnapshotId());
      assertEquals("table", request.getScope().getDestinationCaptureRequests(0).getTargetSpec());
    }
  }

  @Test
  void connectorTriggerTableStatsAndIndexDoNotMarkColumnStatsRequested() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setTableId(ResourceId.newBuilder().setId("table-1").build())
                      .setNamespace(NamespacePath.newBuilder().addSegments("ns").build())
                      .build())
              .build();
      h.directoryService.tableDisplayName = "events";
      h.snapshotService.currentSnapshotId = 42L;

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "capture-only",
              "--capture",
              "table-stats,index",
              "--columns",
              "c1"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.snapshotStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals("c1", request.getScope().getCapturePolicy().getColumns(0).getSelector());
      assertEquals(false, request.getScope().getCapturePolicy().getColumns(0).getCaptureStats());
      assertEquals(true, request.getScope().getCapturePolicy().getColumns(0).getCaptureIndex());
    }
  }

  @Test
  void connectorTriggerDestViewCreatesViewScopedCapture() throws Exception {
    try (Harness h = new Harness()) {
      h.connectorsService.connectorToReturn =
          Connector.newBuilder()
              .setResourceId(connectorId())
              .setDestination(
                  DestinationTarget.newBuilder()
                      .setCatalogId(ResourceId.newBuilder().setId("catalog-1").build())
                      .build())
              .build();
      h.directoryService.resolvedViewId = "view-1";

      ConnectorCliSupport.handle(
          "connector",
          List.of(
              "trigger",
              CONNECTOR_UUID,
              "--mode",
              "metadata-and-capture",
              "--capture",
              "stats",
              "--dest-ns",
              "ns",
              "--dest-view",
              "orders_v"),
          new PrintStream(new ByteArrayOutputStream()),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      StartCaptureRequest request = h.reconcileControlService.lastStartCaptureRequest;
      assertEquals("view-1", request.getScope().getDestinationViewId());
      assertEquals("", request.getScope().getDestinationTableId());
    }
  }

  @Test
  void connectorTriggerPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("trigger"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- connector jobs ---

  @Test
  void connectorJobsPrintsParentTableOnlyByDefault() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.listJobsResponse =
          ListReconcileJobsResponse.newBuilder()
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-plan-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_CONNECTOR)
                      .setTablesScanned(2)
                      .setTablesChanged(0)
                      .setViewsScanned(1)
                      .setViewsChanged(0)
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
                      .build())
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-table-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_TABLE)
                      .setParentJobId("job-plan-1")
                      .setExecutorId("remote-executor-a")
                      .setTablesScanned(1)
                      .setTablesChanged(1)
                      .setTableTask(
                          ReconcileTableTask.newBuilder()
                              .setSourceNamespace("sales")
                              .setSourceTable("orders")
                              .setDestinationTableDisplayName("orders_curated")
                              .build())
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_QUEUED)
                      .build())
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("jobs"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.reconcileControlService.listReconcileJobsCalls.get());
      assertTrue(buf.toString().contains("JOB_ID"));
      assertTrue(buf.toString().contains("MODE"));
      assertTrue(buf.toString().contains("INDEXES"));
      assertTrue(buf.toString().contains("job-plan-1"));
      assertTrue(buf.toString().contains("0/2"));
      assertTrue(buf.toString().contains("0/1"));
      assertTrue(!buf.toString().contains("job-table-1"));
      assertTrue(!buf.toString().contains("sales.orders->orders_curated"));
    }
  }

  @Test
  void connectorJobsChildPrintsRecursiveTree() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.listJobsResponse =
          ListReconcileJobsResponse.newBuilder()
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-plan-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_CONNECTOR)
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
                      .build())
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-table-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_TABLE)
                      .setParentJobId("job-plan-1")
                      .setExecutorId("remote-executor-a")
                      .setTablesScanned(1)
                      .setTablesChanged(1)
                      .setTableTask(
                          ReconcileTableTask.newBuilder()
                              .setSourceNamespace("sales")
                              .setSourceTable("orders")
                              .setDestinationTableDisplayName("orders_curated")
                              .build())
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_QUEUED)
                      .build())
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-snapshot-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_SNAPSHOT)
                      .setParentJobId("job-table-1")
                      .setExecutorId("remote-snapshot-worker")
                      .setSnapshotsProcessed(2)
                      .setStatsProcessed(65)
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_SUCCEEDED)
                      .build())
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("jobs", "--child", "job-plan-1"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(buf.toString().contains("JOB_TREE"));
      assertTrue(buf.toString().contains("INDEXES"));
      assertTrue(buf.toString().contains("job-plan-1"));
      assertTrue(buf.toString().contains("\\- job-table-1"));
      assertTrue(buf.toString().contains("plan_table"));
      assertTrue(buf.toString().contains("sales.orders->orders_curated"));
      assertTrue(buf.toString().contains("\\- job-snapshot-1"));
      assertTrue(buf.toString().contains("plan_snapshot"));
      assertTrue(buf.toString().contains("2           65"));
    }
  }

  @Test
  void connectorJobPrintsStructuredDetailView() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.getJobResponse =
          ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
              .setJobId("job-table-1")
              .setConnectorId(CONNECTOR_UUID)
              .setKind(ReconcileJobKind.RJK_PLAN_TABLE)
              .setParentJobId("job-plan-1")
              .setExecutorId("remote-executor-a")
              .setTablesScanned(4)
              .setTablesChanged(1)
              .setTableTask(
                  ReconcileTableTask.newBuilder()
                      .setSourceNamespace("sales")
                      .setSourceTable("orders")
                      .setDestinationTableDisplayName("orders_curated")
                      .build())
              .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("job", "job-table-1"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.reconcileControlService.getReconcileJobCalls.get());
      assertTrue(buf.toString().contains("JOB_ID:       job-table-1"));
      assertTrue(buf.toString().contains("PARENT_JOB:   job-plan-1"));
      assertTrue(buf.toString().contains("STATE:        RUNNING"));
      assertTrue(buf.toString().contains("KIND:         plan_table"));
      assertTrue(buf.toString().contains("EXECUTOR:     remote-executor-a"));
      assertTrue(buf.toString().contains("TARGET:       sales.orders->orders_curated"));
      assertTrue(buf.toString().contains("INDEXES:      0"));
      assertTrue(buf.toString().contains("TABLES:       1 changed / 4 scanned"));
    }
  }

  @Test
  void connectorJobPrintsDiscoveryViewDestinationDisplayName() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.getJobResponse =
          ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
              .setJobId("job-view-1")
              .setConnectorId(CONNECTOR_UUID)
              .setKind(ReconcileJobKind.RJK_PLAN_VIEW)
              .setParentJobId("job-plan-1")
              .setExecutorId("remote-executor-a")
              .setViewsScanned(1)
              .setViewsChanged(1)
              .setViewTask(
                  ReconcileViewTask.newBuilder()
                      .setSourceNamespace("sales")
                      .setSourceView("orders_view")
                      .setDestinationNamespaceId("ns-123")
                      .setDestinationViewDisplayName("orders_curated_view")
                      .setMode("DISCOVERY")
                      .build())
              .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("job", "job-view-1"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(
          buf.toString().contains("TARGET:       sales.orders_view->ns-123.orders_curated_view"),
          "expected discovery view target to use destination display name");
    }
  }

  @Test
  void connectorJobsJsonPrintsFilteredJobs() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.listJobsResponse =
          ListReconcileJobsResponse.newBuilder()
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-plan-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_CONNECTOR)
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
                      .build())
              .addJobs(
                  ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
                      .setJobId("job-table-1")
                      .setConnectorId(CONNECTOR_UUID)
                      .setKind(ReconcileJobKind.RJK_PLAN_TABLE)
                      .setParentJobId("job-plan-1")
                      .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_QUEUED)
                      .build())
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("jobs", "--json"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(buf.toString().contains("\"jobId\": \"job-plan-1\""));
      assertTrue(!buf.toString().contains("\"jobId\": \"job-table-1\""));
    }
  }

  @Test
  void connectorJobJsonPrintsSingleJob() throws Exception {
    try (Harness h = new Harness()) {
      h.reconcileControlService.getJobResponse =
          ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.newBuilder()
              .setJobId("job-table-1")
              .setConnectorId(CONNECTOR_UUID)
              .setState(ai.floedb.floecat.reconciler.rpc.JobState.JS_RUNNING)
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("job", "job-table-1", "--json"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertTrue(buf.toString().contains("\"jobId\": \"job-table-1\""));
      assertTrue(buf.toString().contains("\"state\": \"JS_RUNNING\""));
    }
  }

  // --- connector cancel ---

  @Test
  void connectorCancelCallsService() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("cancel", "job-uuid-1"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.reconcileControlService.cancelReconcileJobCalls.get());
      assertEquals("job-uuid-1", h.reconcileControlService.lastCancelRequest.getJobId());
    }
  }

  // --- connector settings ---

  @Test
  void connectorSettingsGetCallsService() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("settings", "get"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.reconcileControlService.getReconcilerSettingsCalls.get());
    }
  }

  @Test
  void connectorSettingsUpdateCallsService() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("settings", "update", "--auto-enabled", "true"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");

      assertEquals(1, h.reconcileControlService.updateReconcilerSettingsCalls.get());
    }
  }

  // --- unknown subcommand / empty args ---

  @Test
  void connectorUnknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("unknown") || buf.toString().contains("usage:"));
    }
  }

  @Test
  void connectorEmptyArgsPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ConnectorCliSupport.handle(
          "connector",
          List.of(),
          new PrintStream(buf),
          h.connectorsStub,
          h.reconcileControlStub,
          h.directoryStub,
          () -> "acct-1");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingConnectorsService connectorsService;
    final CapturingReconcileControlService reconcileControlService;
    final CapturingDirectoryService directoryService;
    final CapturingSnapshotService snapshotService;
    final ConnectorsGrpc.ConnectorsBlockingStub connectorsStub;
    final ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControlStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;
    final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.connectorsService = new CapturingConnectorsService();
      this.reconcileControlService = new CapturingReconcileControlService();
      this.directoryService = new CapturingDirectoryService();
      this.snapshotService = new CapturingSnapshotService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(connectorsService)
              .addService(reconcileControlService)
              .addService(directoryService)
              .addService(snapshotService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.connectorsStub = ConnectorsGrpc.newBlockingStub(channel);
      this.reconcileControlStub = ReconcileControlGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
      this.snapshotStub = SnapshotServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingConnectorsService extends ConnectorsGrpc.ConnectorsImplBase {

    final AtomicInteger listConnectorsCalls = new AtomicInteger();
    final AtomicInteger getConnectorCalls = new AtomicInteger();
    final AtomicInteger createConnectorCalls = new AtomicInteger();
    final AtomicInteger deleteConnectorCalls = new AtomicInteger();
    final AtomicInteger validateConnectorCalls = new AtomicInteger();
    final List<Connector> connectorsToReturn = new ArrayList<>();
    Connector connectorToReturn = Connector.getDefaultInstance();
    CreateConnectorRequest lastCreateRequest;

    @Override
    public void listConnectors(
        ListConnectorsRequest request, StreamObserver<ListConnectorsResponse> responseObserver) {
      listConnectorsCalls.incrementAndGet();
      responseObserver.onNext(
          ListConnectorsResponse.newBuilder().addAllConnectors(connectorsToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getConnector(
        GetConnectorRequest request, StreamObserver<GetConnectorResponse> responseObserver) {
      getConnectorCalls.incrementAndGet();
      responseObserver.onNext(
          GetConnectorResponse.newBuilder().setConnector(connectorToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createConnector(
        CreateConnectorRequest request, StreamObserver<CreateConnectorResponse> responseObserver) {
      createConnectorCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(
          CreateConnectorResponse.newBuilder().setConnector(connectorToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteConnector(
        DeleteConnectorRequest request, StreamObserver<DeleteConnectorResponse> responseObserver) {
      deleteConnectorCalls.incrementAndGet();
      responseObserver.onNext(DeleteConnectorResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void validateConnector(
        ValidateConnectorRequest request,
        StreamObserver<ValidateConnectorResponse> responseObserver) {
      validateConnectorCalls.incrementAndGet();
      responseObserver.onNext(ValidateConnectorResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingReconcileControlService
      extends ReconcileControlGrpc.ReconcileControlImplBase {

    final AtomicInteger startCaptureCalls = new AtomicInteger();
    final AtomicInteger getReconcileJobCalls = new AtomicInteger();
    final AtomicInteger listReconcileJobsCalls = new AtomicInteger();
    final AtomicInteger cancelReconcileJobCalls = new AtomicInteger();
    final AtomicInteger getReconcilerSettingsCalls = new AtomicInteger();
    final AtomicInteger updateReconcilerSettingsCalls = new AtomicInteger();
    StartCaptureRequest lastStartCaptureRequest;
    CancelReconcileJobRequest lastCancelRequest;
    ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse getJobResponse =
        ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse.getDefaultInstance();
    ListReconcileJobsResponse listJobsResponse = ListReconcileJobsResponse.getDefaultInstance();

    @Override
    public void startCapture(
        StartCaptureRequest request, StreamObserver<StartCaptureResponse> responseObserver) {
      startCaptureCalls.incrementAndGet();
      lastStartCaptureRequest = request;
      responseObserver.onNext(StartCaptureResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void getReconcileJob(
        ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest request,
        StreamObserver<ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse> responseObserver) {
      getReconcileJobCalls.incrementAndGet();
      responseObserver.onNext(getJobResponse);
      responseObserver.onCompleted();
    }

    @Override
    public void listReconcileJobs(
        ListReconcileJobsRequest request,
        StreamObserver<ListReconcileJobsResponse> responseObserver) {
      listReconcileJobsCalls.incrementAndGet();
      responseObserver.onNext(listJobsResponse);
      responseObserver.onCompleted();
    }

    @Override
    public void cancelReconcileJob(
        CancelReconcileJobRequest request,
        StreamObserver<CancelReconcileJobResponse> responseObserver) {
      cancelReconcileJobCalls.incrementAndGet();
      lastCancelRequest = request;
      responseObserver.onNext(CancelReconcileJobResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void getReconcilerSettings(
        GetReconcilerSettingsRequest request,
        StreamObserver<GetReconcilerSettingsResponse> responseObserver) {
      getReconcilerSettingsCalls.incrementAndGet();
      responseObserver.onNext(GetReconcilerSettingsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void updateReconcilerSettings(
        UpdateReconcilerSettingsRequest request,
        StreamObserver<UpdateReconcilerSettingsResponse> responseObserver) {
      updateReconcilerSettingsCalls.incrementAndGet();
      responseObserver.onNext(UpdateReconcilerSettingsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {
    String tableDisplayName = "";
    String resolvedViewId = "view-default";

    @Override
    public void lookupCatalog(
        LookupCatalogRequest request, StreamObserver<LookupCatalogResponse> responseObserver) {
      responseObserver.onNext(LookupCatalogResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void lookupNamespace(
        LookupNamespaceRequest request, StreamObserver<LookupNamespaceResponse> responseObserver) {
      responseObserver.onNext(LookupNamespaceResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void lookupTable(
        LookupTableRequest request, StreamObserver<LookupTableResponse> responseObserver) {
      responseObserver.onNext(
          LookupTableResponse.newBuilder()
              .setName(ai.floedb.floecat.common.rpc.NameRef.newBuilder().setName(tableDisplayName))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveView(
        ResolveViewRequest request, StreamObserver<ResolveViewResponse> responseObserver) {
      responseObserver.onNext(
          ResolveViewResponse.newBuilder()
              .setResourceId(ResourceId.newBuilder().setId(resolvedViewId).build())
              .build());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingSnapshotService
      extends SnapshotServiceGrpc.SnapshotServiceImplBase {
    long currentSnapshotId = 1L;

    @Override
    public void getSnapshot(
        GetSnapshotRequest request, StreamObserver<GetSnapshotResponse> responseObserver) {
      responseObserver.onNext(
          GetSnapshotResponse.newBuilder()
              .setSnapshot(Snapshot.newBuilder().setSnapshotId(currentSnapshotId).build())
              .build());
      responseObserver.onCompleted();
    }
  }
}
