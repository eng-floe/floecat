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

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
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

class ViewCliSupportTest {

  private static final String UUID_1 = "00000000-0000-0000-0000-000000000001";
  private static final String UUID_2 = "00000000-0000-0000-0000-000000000002";
  private static final String ACCT_ID = "00000000-0000-0000-0000-000000000099";

  private static View view(String id, String displayName) {
    return View.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).build())
        .setDisplayName(displayName)
        .addSqlDefinitions(ViewSqlDefinition.newBuilder().setSql("SELECT 1").setDialect("ansi"))
        .addSqlDefinitions(ViewSqlDefinition.newBuilder().setSql("SELECT `id`").setDialect("spark"))
        .build();
  }

  // --- views (list) ---

  @Test
  void viewsListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.viewService.viewsToList.add(view(UUID_1, "my-view"));
      h.directoryService.resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "views",
          List.of("cat.ns"),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);

      String out = buf.toString();
      assertTrue(out.contains("VIEW_ID"), "expected header");
      assertTrue(out.contains(UUID_1), "expected view id");
      assertTrue(out.contains("my-view"), "expected display name");
    }
  }

  @Test
  void viewsListPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "views", List.of(), new PrintStream(buf), h.viewStub, h.directoryStub, () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- view create ---

  @Test
  void viewCreateSendsSpecAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.viewService.viewToReturn = view(UUID_2, "new-view");
      h.directoryService.resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();
      h.directoryService.resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("create", "cat.ns.new-view", "--sql", "SELECT 1", "--desc", "My view"),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.viewService.createViewCalls.get());
      assertEquals("new-view", h.viewService.lastCreateRequest.getSpec().getDisplayName());
      assertEquals(1, h.viewService.lastCreateRequest.getSpec().getSqlDefinitionsCount());
      assertEquals(
          "SELECT 1", h.viewService.lastCreateRequest.getSpec().getSqlDefinitions(0).getSql());
      assertEquals(
          "ansi", h.viewService.lastCreateRequest.getSpec().getSqlDefinitions(0).getDialect());
      assertEquals("My view", h.viewService.lastCreateRequest.getSpec().getDescription());
      assertTrue(buf.toString().contains(UUID_2));
    }
  }

  @Test
  void viewCreatePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("create"),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- view get ---

  @Test
  void viewGetByUuidPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.viewService.viewToReturn = view(UUID_1, "acme-view");
      h.directoryService.resolvedViewId = ResourceId.newBuilder().setId(UUID_1).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("get", UUID_1),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.viewService.getViewCalls.get());
      assertTrue(buf.toString().contains(UUID_1));
      assertTrue(buf.toString().contains("[ansi] SELECT 1"));
      assertTrue(buf.toString().contains("[spark] SELECT `id`"));
    }
  }

  @Test
  void viewGetPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view", List.of("get"), new PrintStream(buf), h.viewStub, h.directoryStub, () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- view delete ---

  @Test
  void viewDeleteByUuidPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("delete", UUID_1),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.viewService.deleteViewCalls.get());
      assertEquals(UUID_1, h.viewService.lastDeleteRequest.getViewId().getId());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void viewDeletePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("delete"),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- unknown subcommand ---

  @Test
  void unknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ViewCliSupport.handle(
          "view",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.viewStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("unknown subcommand"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingViewService viewService;
    final CapturingDirectoryService directoryService;
    final ViewServiceGrpc.ViewServiceBlockingStub viewStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.viewService = new CapturingViewService();
      this.directoryService = new CapturingDirectoryService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(viewService)
              .addService(directoryService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.viewStub = ViewServiceGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingViewService extends ViewServiceGrpc.ViewServiceImplBase {

    final AtomicInteger createViewCalls = new AtomicInteger();
    final AtomicInteger getViewCalls = new AtomicInteger();
    final AtomicInteger deleteViewCalls = new AtomicInteger();
    final List<View> viewsToList = new ArrayList<>();
    View viewToReturn = View.getDefaultInstance();
    CreateViewRequest lastCreateRequest;
    GetViewRequest lastGetRequest;
    DeleteViewRequest lastDeleteRequest;

    @Override
    public void listViews(
        ListViewsRequest request, StreamObserver<ListViewsResponse> responseObserver) {
      responseObserver.onNext(ListViewsResponse.newBuilder().addAllViews(viewsToList).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createView(
        CreateViewRequest request, StreamObserver<CreateViewResponse> responseObserver) {
      createViewCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(CreateViewResponse.newBuilder().setView(viewToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getView(GetViewRequest request, StreamObserver<GetViewResponse> responseObserver) {
      getViewCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(GetViewResponse.newBuilder().setView(viewToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateView(
        UpdateViewRequest request, StreamObserver<UpdateViewResponse> responseObserver) {
      responseObserver.onNext(UpdateViewResponse.newBuilder().setView(viewToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteView(
        DeleteViewRequest request, StreamObserver<DeleteViewResponse> responseObserver) {
      deleteViewCalls.incrementAndGet();
      lastDeleteRequest = request;
      responseObserver.onNext(DeleteViewResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {

    ResourceId resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();
    ResourceId resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_1).build();
    ResourceId resolvedViewId = ResourceId.newBuilder().setId(UUID_1).build();

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
    public void resolveView(
        ResolveViewRequest request, StreamObserver<ResolveViewResponse> responseObserver) {
      responseObserver.onNext(
          ResolveViewResponse.newBuilder().setResourceId(resolvedViewId).build());
      responseObserver.onCompleted();
    }
  }
}
