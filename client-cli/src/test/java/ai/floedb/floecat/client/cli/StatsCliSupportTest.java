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

import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
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
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
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
          h.tablesStub,
          h.namespacesStub,
          h.reconcileControlStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingStatisticsService statisticsService;
    final TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statisticsStub;
    final TableServiceGrpc.TableServiceBlockingStub tablesStub;
    final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespacesStub;
    final ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControlStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.statisticsService = new CapturingStatisticsService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(statisticsService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.statisticsStub = TableStatisticsServiceGrpc.newBlockingStub(channel);
      // tables, namespaces, reconcileControl not exercised in stats tests — stubs point at the
      // same in-process server; RPC calls in analyze tests would need additional services added.
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
}
