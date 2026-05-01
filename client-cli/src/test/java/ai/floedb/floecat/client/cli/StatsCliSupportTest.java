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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexCoverage;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsResponse;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowResponse;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StatsCliSupportTest {

  private static ResourceId tableId() {
    return ResourceId.newBuilder().setId("table-uuid-stats").build();
  }

  // --- stats table ---

  @Test
  void statsTablePrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.statisticsService.tableStatsToReturn =
          TargetStatsRecord.newBuilder()
              .setTableId(tableId())
              .setSnapshotId(7L)
              .setTarget(
                  ai.floedb.floecat.catalog.rpc.StatsTarget.newBuilder()
                      .setTable(TableStatsTarget.getDefaultInstance())
                      .build())
              .setTable(TableValueStats.newBuilder().setRowCount(100L).build())
              .build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("table", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      String out = buf.toString();
      assertTrue(out.contains("Table Stats:"), "expected header");
      assertTrue(out.contains("100"), "expected row_count");
    }
  }

  @Test
  void statsTableDefaultsToCurrentSnapshot() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("table", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      assertEquals(1, h.statisticsService.getTableStatsCalls.get());
      assertEquals(
          SpecialSnapshot.SS_CURRENT,
          h.statisticsService.lastTargetStatsRequest.getSnapshot().getSpecial());
    }
  }

  @Test
  void statsTableUsesExplicitSnapshot() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("table", "catalog.ns.tbl", "--snapshot", "42"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      assertEquals(42L, h.statisticsService.lastTargetStatsRequest.getSnapshot().getSnapshotId());
    }
  }

  @Test
  void statsTablePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("table"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- stats columns ---

  @Test
  void statsColumnsPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("columns", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      assertTrue(buf.toString().contains("CID"), "expected column header");
      assertEquals(1, h.statisticsService.listTargetStatsCalls.get());
      assertEquals(
          StatsTargetKind.STK_COLUMN,
          h.statisticsService.lastListTargetStatsRequest.getTargetKinds(0));
    }
  }

  @Test
  void statsColumnsPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("columns"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- stats files ---

  @Test
  void statsFilesPrintsNoFilesMessageWhenEmpty() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("files", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      assertTrue(buf.toString().contains("No file stats found."));
      assertEquals(1, h.statisticsService.listTargetStatsCalls.get());
      assertEquals(
          StatsTargetKind.STK_FILE,
          h.statisticsService.lastListTargetStatsRequest.getTargetKinds(0));
    }
  }

  @Test
  void statsFilesPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("files"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  @Test
  void statsIndexPrintsArtifactsAndUsesCurrentSnapshot() throws Exception {
    try (Harness h = new Harness()) {
      h.indexService.recordsToReturn =
          List.of(
              IndexArtifactRecord.newBuilder()
                  .setTableId(tableId())
                  .setSnapshotId(7L)
                  .setTarget(
                      IndexTarget.newBuilder()
                          .setFile(
                              IndexFileTarget.newBuilder()
                                  .setFilePath("s3://bucket/orders/data-000.parquet")
                                  .build())
                          .build())
                  .setArtifactUri("s3://bucket/orders/data-000.parquet.floe-index.parquet")
                  .setArtifactFormat("parquet")
                  .setState(IndexArtifactState.IAS_READY)
                  .setCoverage(
                      IndexCoverage.newBuilder()
                          .setRowsIndexed(100L)
                          .setLiveRowsIndexed(95L)
                          .setPagesIndexed(12L)
                          .setRowGroupsIndexed(2L)
                          .build())
                  .build());

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("index", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      String out = buf.toString();
      assertTrue(out.contains("STATE"), "expected index header");
      assertTrue(out.contains("s3://bucket/orders/data-000.parquet"), "expected file path");
      assertEquals(1, h.indexService.listIndexArtifactsCalls.get());
      assertEquals(
          SpecialSnapshot.SS_CURRENT,
          h.indexService.lastListIndexArtifactsRequest.getSnapshot().getSpecial());
    }
  }

  @Test
  void statsIndexesAliasSupportsExplicitSnapshotAndJson() throws Exception {
    try (Harness h = new Harness()) {
      h.indexService.recordsToReturn =
          List.of(
              IndexArtifactRecord.newBuilder()
                  .setTableId(tableId())
                  .setSnapshotId(99L)
                  .setTarget(
                      IndexTarget.newBuilder()
                          .setFile(IndexFileTarget.newBuilder().setFilePath("file.parquet").build())
                          .build())
                  .setArtifactUri("file.parquet.floe-index.parquet")
                  .setArtifactFormat("parquet")
                  .setState(IndexArtifactState.IAS_PENDING)
                  .build());

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("indexes", "catalog.ns.tbl", "--snapshot", "99", "--json"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      assertTrue(buf.toString().contains("\"artifactUri\""));
      assertEquals(99L, h.indexService.lastListIndexArtifactsRequest.getSnapshot().getSnapshotId());
    }
  }

  // --- stats unknown subcommand ---

  @Test
  void statsUnknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("unknown stats subcommand"));
    }
  }

  @Test
  void statsEmptyArgsPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      StatsCliSupport.handle(
          "stats",
          List.of(),
          new PrintStream(buf),
          h.statisticsStub,
          h.indexesStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  @Test
  void analyzeColumnsCreatesScopedCaptureRequest() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn =
          Table.newBuilder()
              .setResourceId(tableId())
              .setDisplayName("events")
              .setNamespaceId(ResourceId.newBuilder().setId("ns-1").build())
              .setUpstream(
                  UpstreamRef.newBuilder()
                      .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                      .build())
              .build();
      h.namespaceService.namespaceToReturn =
          Namespace.newBuilder().addParents("db").setDisplayName("analytics").build();
      h.snapshotService.currentSnapshotId = 42L;

      StatsCliSupport.handle(
          "analyze",
          List.of(
              "catalog.ns.tbl",
              "--mode",
              "capture-only",
              "--capture",
              "stats",
              "--columns",
              "c1,#7"),
          new PrintStream(new ByteArrayOutputStream()),
          h.statisticsStub,
          h.indexesStub,
          h.snapshotStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      CaptureNowRequest request = h.reconcileControlService.lastCaptureNowRequest;
      assertEquals(
          tableId().getId(), request.getScope().getDestinationCaptureRequests(0).getTableId());
      assertEquals(
          List.of("c1", "#7"),
          request.getScope().getDestinationCaptureRequests(0).getColumnSelectorsList());
      assertEquals(3, request.getScope().getCapturePolicy().getOutputsCount());
    }
  }

  @Test
  void analyzeDefaultsToStatsOnlyCapture() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn =
          Table.newBuilder()
              .setResourceId(tableId())
              .setDisplayName("events")
              .setNamespaceId(ResourceId.newBuilder().setId("ns-1").build())
              .setUpstream(
                  UpstreamRef.newBuilder()
                      .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                      .build())
              .build();

      StatsCliSupport.handle(
          "analyze",
          List.of("catalog.ns.tbl"),
          new PrintStream(new ByteArrayOutputStream()),
          h.statisticsStub,
          h.indexesStub,
          h.snapshotStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      CaptureNowRequest request = h.reconcileControlService.lastCaptureNowRequest;
      assertEquals(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY, request.getMode());
      assertEquals(
          java.util.Set.of(
              CaptureOutput.CO_TABLE_STATS,
              CaptureOutput.CO_FILE_STATS,
              CaptureOutput.CO_COLUMN_STATS),
          java.util.Set.copyOf(request.getScope().getCapturePolicy().getOutputsList()));
    }
  }

  @Test
  void analyzeCaptureFlagsAllowIndexOnly() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn =
          Table.newBuilder()
              .setResourceId(tableId())
              .setDisplayName("events")
              .setNamespaceId(ResourceId.newBuilder().setId("ns-1").build())
              .setUpstream(
                  UpstreamRef.newBuilder()
                      .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                      .build())
              .build();
      h.snapshotService.currentSnapshotId = 42L;

      StatsCliSupport.handle(
          "analyze",
          List.of(
              "catalog.ns.tbl", "--mode", "capture-only", "--capture", "index", "--columns", "c1"),
          new PrintStream(new ByteArrayOutputStream()),
          h.statisticsStub,
          h.indexesStub,
          h.snapshotStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      CaptureNowRequest request = h.reconcileControlService.lastCaptureNowRequest;
      assertEquals(
          List.of(CaptureOutput.CO_PARQUET_PAGE_INDEX),
          request.getScope().getCapturePolicy().getOutputsList());
      assertEquals("c1", request.getScope().getCapturePolicy().getColumns(0).getSelector());
      assertEquals(false, request.getScope().getCapturePolicy().getColumns(0).getCaptureStats());
      assertEquals(true, request.getScope().getCapturePolicy().getColumns(0).getCaptureIndex());
    }
  }

  @Test
  void analyzeExplicitSnapshotScopesRequestWithoutLookup() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn =
          Table.newBuilder()
              .setResourceId(tableId())
              .setDisplayName("events")
              .setNamespaceId(ResourceId.newBuilder().setId("ns-1").build())
              .setUpstream(
                  UpstreamRef.newBuilder()
                      .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                      .build())
              .build();
      h.namespaceService.namespaceToReturn =
          Namespace.newBuilder().addParents("db").setDisplayName("analytics").build();
      h.snapshotService.currentSnapshotId = 42L;

      StatsCliSupport.handle(
          "analyze",
          List.of(
              "catalog.ns.tbl",
              "--mode",
              "capture-only",
              "--capture",
              "stats",
              "--snapshot",
              "99",
              "--columns",
              "c1,#7"),
          new PrintStream(new ByteArrayOutputStream()),
          h.statisticsStub,
          h.indexesStub,
          h.snapshotStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      CaptureNowRequest request = h.reconcileControlService.lastCaptureNowRequest;
      assertEquals(99L, request.getScope().getDestinationCaptureRequests(0).getSnapshotId());
    }
  }

  @Test
  void analyzeExplicitSnapshotWithoutColumnsStillScopesCapture() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn =
          Table.newBuilder()
              .setResourceId(tableId())
              .setDisplayName("events")
              .setNamespaceId(ResourceId.newBuilder().setId("ns-1").build())
              .setUpstream(
                  UpstreamRef.newBuilder()
                      .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                      .build())
              .build();
      h.namespaceService.namespaceToReturn =
          Namespace.newBuilder().addParents("db").setDisplayName("analytics").build();

      StatsCliSupport.handle(
          "analyze",
          List.of(
              "catalog.ns.tbl", "--mode", "capture-only", "--capture", "stats", "--snapshot", "99"),
          new PrintStream(new ByteArrayOutputStream()),
          h.statisticsStub,
          h.indexesStub,
          h.snapshotStub,
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());

      CaptureNowRequest request = h.reconcileControlService.lastCaptureNowRequest;
      assertEquals(1, request.getScope().getDestinationCaptureRequestsCount());
      assertEquals(99L, request.getScope().getDestinationCaptureRequests(0).getSnapshotId());
      assertEquals(
          List.of(), request.getScope().getDestinationCaptureRequests(0).getColumnSelectorsList());
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingStatisticsService statisticsService;
    final CapturingIndexService indexService;
    final CapturingTableService tableService;
    final CapturingNamespaceService namespaceService;
    final CapturingSnapshotService snapshotService;
    final CapturingReconcileControlService reconcileControlService;
    final TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statisticsStub;
    final TableIndexServiceGrpc.TableIndexServiceBlockingStub indexesStub;
    final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub;
    final TableServiceGrpc.TableServiceBlockingStub tablesStub;
    final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespacesStub;
    final ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControlStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.statisticsService = new CapturingStatisticsService();
      this.indexService = new CapturingIndexService();
      this.tableService = new CapturingTableService();
      this.namespaceService = new CapturingNamespaceService();
      this.snapshotService = new CapturingSnapshotService();
      this.reconcileControlService = new CapturingReconcileControlService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(statisticsService)
              .addService(indexService)
              .addService(tableService)
              .addService(namespaceService)
              .addService(snapshotService)
              .addService(reconcileControlService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.statisticsStub = TableStatisticsServiceGrpc.newBlockingStub(channel);
      this.indexesStub = TableIndexServiceGrpc.newBlockingStub(channel);
      this.snapshotStub = SnapshotServiceGrpc.newBlockingStub(channel);
      this.tablesStub = TableServiceGrpc.newBlockingStub(channel);
      this.namespacesStub = NamespaceServiceGrpc.newBlockingStub(channel);
      this.reconcileControlStub = ReconcileControlGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingStatisticsService
      extends TableStatisticsServiceGrpc.TableStatisticsServiceImplBase {

    final AtomicInteger getTableStatsCalls = new AtomicInteger();
    final AtomicInteger listTargetStatsCalls = new AtomicInteger();
    GetTargetStatsRequest lastTargetStatsRequest;
    ListTargetStatsRequest lastListTargetStatsRequest;
    TargetStatsRecord tableStatsToReturn = TargetStatsRecord.getDefaultInstance();

    @Override
    public void getTargetStats(
        GetTargetStatsRequest request, StreamObserver<GetTargetStatsResponse> responseObserver) {
      getTableStatsCalls.incrementAndGet();
      lastTargetStatsRequest = request;
      responseObserver.onNext(
          GetTargetStatsResponse.newBuilder().setStats(tableStatsToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void listTargetStats(
        ListTargetStatsRequest request, StreamObserver<ListTargetStatsResponse> responseObserver) {
      listTargetStatsCalls.incrementAndGet();
      lastListTargetStatsRequest = request;
      responseObserver.onNext(ListTargetStatsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingIndexService
      extends TableIndexServiceGrpc.TableIndexServiceImplBase {
    final AtomicInteger listIndexArtifactsCalls = new AtomicInteger();
    ListIndexArtifactsRequest lastListIndexArtifactsRequest;
    List<IndexArtifactRecord> recordsToReturn = List.of();

    @Override
    public void listIndexArtifacts(
        ListIndexArtifactsRequest request,
        StreamObserver<ListIndexArtifactsResponse> responseObserver) {
      listIndexArtifactsCalls.incrementAndGet();
      lastListIndexArtifactsRequest = request;
      responseObserver.onNext(
          ListIndexArtifactsResponse.newBuilder().addAllRecords(recordsToReturn).build());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingTableService extends TableServiceGrpc.TableServiceImplBase {
    Table tableToReturn = Table.getDefaultInstance();

    @Override
    public void getTable(
        GetTableRequest request, StreamObserver<GetTableResponse> responseObserver) {
      responseObserver.onNext(GetTableResponse.newBuilder().setTable(tableToReturn).build());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingNamespaceService
      extends NamespaceServiceGrpc.NamespaceServiceImplBase {
    Namespace namespaceToReturn = Namespace.getDefaultInstance();

    @Override
    public void getNamespace(
        GetNamespaceRequest request, StreamObserver<GetNamespaceResponse> responseObserver) {
      responseObserver.onNext(
          GetNamespaceResponse.newBuilder().setNamespace(namespaceToReturn).build());
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

  private static final class CapturingReconcileControlService
      extends ReconcileControlGrpc.ReconcileControlImplBase {
    CaptureNowRequest lastCaptureNowRequest;

    @Override
    public void captureNow(
        CaptureNowRequest request, StreamObserver<CaptureNowResponse> responseObserver) {
      lastCaptureNowRequest = request;
      responseObserver.onNext(CaptureNowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
