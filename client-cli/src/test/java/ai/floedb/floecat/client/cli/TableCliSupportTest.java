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

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
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

class TableCliSupportTest {

  private static final String UUID_1 = "00000000-0000-0000-0000-000000000001";
  private static final String UUID_2 = "00000000-0000-0000-0000-000000000002";
  private static final String ACCT_ID = "00000000-0000-0000-0000-000000000099";

  private static Table table(String id, String displayName) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).build())
        .setDisplayName(displayName)
        .build();
  }

  // --- tables (list) ---

  @Test
  void tablesListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.directoryService.fqTablesToReturn.add(
          ResolveFQTablesResponse.Entry.newBuilder()
              .setResourceId(ResourceId.newBuilder().setId(UUID_1).build())
              .setName(
                  ai.floedb.floecat.common.rpc.NameRef.newBuilder()
                      .setCatalog("cat")
                      .addPath("ns")
                      .setName("my-table")
                      .build())
              .build());

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "tables",
          List.of("cat.ns"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      String out = buf.toString();
      assertTrue(out.contains("TABLE_ID"), "expected header");
      assertTrue(out.contains(UUID_1), "expected table id");
    }
  }

  @Test
  void tablesListPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "tables",
          List.of(),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- table create ---

  @Test
  void tableCreateSendsSpecAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn = table(UUID_2, "new-table");
      h.directoryService.resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();
      h.directoryService.resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("create", "cat.ns.new-table", "--desc", "My table"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      assertEquals(1, h.tableService.createTableCalls.get());
      assertEquals("new-table", h.tableService.lastCreateRequest.getSpec().getDisplayName());
      assertEquals("My table", h.tableService.lastCreateRequest.getSpec().getDescription());
      assertTrue(buf.toString().contains(UUID_2));
    }
  }

  @Test
  void tableCreatePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("create"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- table get ---

  @Test
  void tableGetByUuidPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn = table(UUID_1, "my-table");
      h.directoryService.resolvedTableId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("get", UUID_1),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      assertEquals(1, h.tableService.getTableCalls.get());
      assertTrue(buf.toString().contains(UUID_1));
    }
  }

  @Test
  void tableGetPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("get"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- table delete ---

  @Test
  void tableDeleteByUuidPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("delete", UUID_1),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      assertEquals(1, h.tableService.deleteTableCalls.get());
      assertEquals(UUID_1, h.tableService.lastDeleteRequest.getTableId().getId());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void tableDeletePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("delete"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- table unknown subcommand ---

  @Test
  void unknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "table",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());
      assertTrue(buf.toString().contains("unknown subcommand"));
    }
  }

  // --- describe ---

  @Test
  void describeTablePrintsTableDetail() throws Exception {
    try (Harness h = new Harness()) {
      h.tableService.tableToReturn = table(UUID_1, "described-table");
      h.directoryService.resolvedTableId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "describe",
          List.of("table", "cat.ns.described-table"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      assertTrue(buf.toString().contains("Table:"));
      assertTrue(buf.toString().contains("described-table"));
    }
  }

  // --- resolve ---

  @Test
  void resolveTablePrintsId() throws Exception {
    try (Harness h = new Harness()) {
      h.directoryService.resolvedTableId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      TableCliSupport.handle(
          "resolve",
          List.of("table", "cat.ns.my-table"),
          new PrintStream(buf),
          h.tableStub,
          h.directoryStub,
          () -> ACCT_ID,
          id -> ResourceId.getDefaultInstance());

      assertTrue(buf.toString().contains("table id:"));
      assertTrue(buf.toString().contains(UUID_1));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingTableService tableService;
    final CapturingDirectoryService directoryService;
    final TableServiceGrpc.TableServiceBlockingStub tableStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.tableService = new CapturingTableService();
      this.directoryService = new CapturingDirectoryService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(tableService)
              .addService(directoryService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.tableStub = TableServiceGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingTableService extends TableServiceGrpc.TableServiceImplBase {

    final AtomicInteger createTableCalls = new AtomicInteger();
    final AtomicInteger getTableCalls = new AtomicInteger();
    final AtomicInteger deleteTableCalls = new AtomicInteger();
    Table tableToReturn = Table.getDefaultInstance();
    CreateTableRequest lastCreateRequest;
    GetTableRequest lastGetRequest;
    DeleteTableRequest lastDeleteRequest;

    @Override
    public void createTable(
        CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
      createTableCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(CreateTableResponse.newBuilder().setTable(tableToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getTable(
        GetTableRequest request, StreamObserver<GetTableResponse> responseObserver) {
      getTableCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(GetTableResponse.newBuilder().setTable(tableToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateTable(
        UpdateTableRequest request, StreamObserver<UpdateTableResponse> responseObserver) {
      responseObserver.onNext(UpdateTableResponse.newBuilder().setTable(tableToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteTable(
        DeleteTableRequest request, StreamObserver<DeleteTableResponse> responseObserver) {
      deleteTableCalls.incrementAndGet();
      lastDeleteRequest = request;
      responseObserver.onNext(DeleteTableResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {

    ResourceId resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();
    ResourceId resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_1).build();
    ResourceId resolvedTableId = ResourceId.newBuilder().setId(UUID_1).build();
    final List<ResolveFQTablesResponse.Entry> fqTablesToReturn = new ArrayList<>();

    @Override
    public void resolveCatalog(
        ResolveCatalogRequest request, StreamObserver<ResolveCatalogResponse> responseObserver) {
      responseObserver.onNext(
          ResolveCatalogResponse.newBuilder().setResourceId(resolvedCatalogId).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveNamespace(
        ResolveNamespaceRequest request,
        StreamObserver<ResolveNamespaceResponse> responseObserver) {
      responseObserver.onNext(
          ResolveNamespaceResponse.newBuilder().setResourceId(resolvedNamespaceId).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveTable(
        ResolveTableRequest request, StreamObserver<ResolveTableResponse> responseObserver) {
      responseObserver.onNext(
          ResolveTableResponse.newBuilder().setResourceId(resolvedTableId).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveFQTables(
        ResolveFQTablesRequest request, StreamObserver<ResolveFQTablesResponse> responseObserver) {
      responseObserver.onNext(
          ResolveFQTablesResponse.newBuilder().addAllTables(fqTablesToReturn).build());
      responseObserver.onCompleted();
    }
  }
}
