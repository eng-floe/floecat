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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.ListAccountsResponse;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsResponse;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.query.rpc.RenewQueryResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CliCommandExecutorTest {

  // --- routing: service-call verification ---

  @Test
  void catalogsRouted() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute("catalogs"));
      assertEquals(1, h.catalogService.listCatalogsCalls.get());
    }
  }

  @Test
  void namespaceSubcommandRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("namespaces");
      assertTrue(buf.toString().contains("usage: namespaces"));
    }
  }

  @Test
  void tableSubcommandRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("tables");
      assertTrue(buf.toString().contains("usage: tables"));
    }
  }

  @Test
  void viewSubcommandRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("views");
      assertTrue(buf.toString().contains("usage: views"));
    }
  }

  @Test
  void connectorsRouted() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute("connectors"));
      assertEquals(1, h.connectorService.listConnectorsCalls.get());
    }
  }

  @Test
  void accountCommandRouted() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute("account list"));
      assertEquals(1, h.accountService.listAccountsCalls.get());
    }
  }

  @Test
  void queryRouted() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute("query renew q-123"));
      assertEquals(1, h.queryService.renewQueryCalls.get());
    }
  }

  // --- routing: output-based verification (usage printed → handler reached) ---

  @Test
  void catalogSubcommandRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("catalog");
      assertTrue(buf.toString().contains("usage: catalog"));
    }
  }

  @Test
  void snapshotRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("snapshot");
      assertTrue(buf.toString().contains("usage: snapshot"));
    }
  }

  @Test
  void statsRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("stats");
      assertTrue(buf.toString().contains("usage: stats"));
    }
  }

  @Test
  void analyzeRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("analyze");
      assertTrue(buf.toString().contains("usage: analyze"));
    }
  }

  @Test
  void constraintsRouted() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      h.executorWithOutput(buf).execute("constraints");
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- contract: never-throw, stateless, unknown, empty ---

  @Test
  void unknownCommandPrintsMessage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      assertFalse(h.executorWithOutput(buf).execute("bogus-command"));
      assertTrue(buf.toString().contains("Unknown command"));
    }
  }

  @Test
  void emptyInputIsNoOp() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute(""));
      assertTrue(h.executor.execute("   "));
      assertEquals(0, h.catalogService.listCatalogsCalls.get());
    }
  }

  @Test
  void noThrowOnGrpcError() throws Exception {
    try (Harness h = new Harness()) {
      h.catalogService.shouldThrow.set(true);
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      assertFalse(h.executorWithOutput(buf).execute("catalogs"));
      assertTrue(buf.toString().contains("error:") || buf.toString().contains("NOT_FOUND"));
    }
  }

  @Test
  void statelessAcrossCalls() throws Exception {
    try (Harness h = new Harness()) {
      assertTrue(h.executor.execute("catalogs"));
      assertTrue(h.executor.execute("catalogs"));
      assertEquals(2, h.catalogService.listCatalogsCalls.get());
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final MinimalCatalogService catalogService = new MinimalCatalogService();
    final MinimalNamespaceService namespaceService = new MinimalNamespaceService();
    final MinimalTableService tableService = new MinimalTableService();
    final MinimalViewService viewService = new MinimalViewService();
    final MinimalConnectorService connectorService = new MinimalConnectorService();
    final MinimalAccountService accountService = new MinimalAccountService();
    final MinimalQueryService queryService = new MinimalQueryService();
    final CliCommandExecutor executor;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(catalogService)
              .addService(namespaceService)
              .addService(tableService)
              .addService(viewService)
              .addService(connectorService)
              .addService(accountService)
              .addService(queryService)
              .addService(new NullSnapshotService())
              .addService(new NullDirectoryService())
              .addService(new NullIndexService())
              .addService(new NullConstraintsService())
              .addService(new NullReconcileControlService())
              .addService(new NullQueryScanService())
              .addService(new NullQuerySchemaService())
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.executor = buildExecutor(new PrintStream(new ByteArrayOutputStream()));
    }

    CliCommandExecutor executorWithOutput(ByteArrayOutputStream buf) {
      return buildExecutor(new PrintStream(buf));
    }

    private CliCommandExecutor buildExecutor(PrintStream out) {
      return CliCommandExecutor.builder()
          .out(out)
          .accounts(AccountServiceGrpc.newBlockingStub(channel))
          .catalogs(CatalogServiceGrpc.newBlockingStub(channel))
          .directory(DirectoryServiceGrpc.newBlockingStub(channel))
          .namespaces(NamespaceServiceGrpc.newBlockingStub(channel))
          .tables(TableServiceGrpc.newBlockingStub(channel))
          .viewService(ViewServiceGrpc.newBlockingStub(channel))
          .connectors(ConnectorsGrpc.newBlockingStub(channel))
          .reconcileControl(ReconcileControlGrpc.newBlockingStub(channel))
          .snapshots(SnapshotServiceGrpc.newBlockingStub(channel))
          .statistics(TableStatisticsServiceGrpc.newBlockingStub(channel))
          .indexes(TableIndexServiceGrpc.newBlockingStub(channel))
          .constraintsService(TableConstraintsServiceGrpc.newBlockingStub(channel))
          .queries(QueryServiceGrpc.newBlockingStub(channel))
          .queryScan(QueryScanServiceGrpc.newBlockingStub(channel))
          .querySchema(QuerySchemaServiceGrpc.newBlockingStub(channel))
          .getAccountId(() -> "acct-1")
          .build();
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  // --- capturing services ---

  private static final class MinimalCatalogService
      extends CatalogServiceGrpc.CatalogServiceImplBase {
    final AtomicInteger listCatalogsCalls = new AtomicInteger();
    final AtomicBoolean shouldThrow = new AtomicBoolean(false);

    @Override
    public void listCatalogs(
        ListCatalogsRequest request, StreamObserver<ListCatalogsResponse> responseObserver) {
      listCatalogsCalls.incrementAndGet();
      if (shouldThrow.get()) {
        responseObserver.onError(
            Status.NOT_FOUND.withDescription("test error").asRuntimeException());
        return;
      }
      responseObserver.onNext(ListCatalogsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalNamespaceService
      extends NamespaceServiceGrpc.NamespaceServiceImplBase {
    @Override
    public void listNamespaces(
        ListNamespacesRequest request, StreamObserver<ListNamespacesResponse> responseObserver) {
      responseObserver.onNext(ListNamespacesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalTableService extends TableServiceGrpc.TableServiceImplBase {
    @Override
    public void listTables(
        ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
      responseObserver.onNext(ListTablesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalViewService extends ViewServiceGrpc.ViewServiceImplBase {
    @Override
    public void listViews(
        ListViewsRequest request, StreamObserver<ListViewsResponse> responseObserver) {
      responseObserver.onNext(ListViewsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalConnectorService extends ConnectorsGrpc.ConnectorsImplBase {
    final AtomicInteger listConnectorsCalls = new AtomicInteger();

    @Override
    public void listConnectors(
        ListConnectorsRequest request, StreamObserver<ListConnectorsResponse> responseObserver) {
      listConnectorsCalls.incrementAndGet();
      responseObserver.onNext(ListConnectorsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalAccountService
      extends AccountServiceGrpc.AccountServiceImplBase {
    final AtomicInteger listAccountsCalls = new AtomicInteger();

    @Override
    public void listAccounts(
        ListAccountsRequest request, StreamObserver<ListAccountsResponse> responseObserver) {
      listAccountsCalls.incrementAndGet();
      responseObserver.onNext(ListAccountsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class MinimalQueryService extends QueryServiceGrpc.QueryServiceImplBase {
    final AtomicInteger renewQueryCalls = new AtomicInteger();

    @Override
    public void renewQuery(
        RenewQueryRequest request, StreamObserver<RenewQueryResponse> responseObserver) {
      renewQueryCalls.incrementAndGet();
      responseObserver.onNext(
          RenewQueryResponse.newBuilder().setQueryId(request.getQueryId()).build());
      responseObserver.onCompleted();
    }
  }

  // --- null services (return defaults, never asserted) ---

  private static final class NullSnapshotService
      extends SnapshotServiceGrpc.SnapshotServiceImplBase {}

  private static final class NullDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {}

  private static final class NullIndexService
      extends TableIndexServiceGrpc.TableIndexServiceImplBase {}

  private static final class NullConstraintsService
      extends TableConstraintsServiceGrpc.TableConstraintsServiceImplBase {}

  private static final class NullReconcileControlService
      extends ReconcileControlGrpc.ReconcileControlImplBase {}

  private static final class NullQueryScanService
      extends QueryScanServiceGrpc.QueryScanServiceImplBase {}

  private static final class NullQuerySchemaService
      extends QuerySchemaServiceGrpc.QuerySchemaServiceImplBase {}
}
