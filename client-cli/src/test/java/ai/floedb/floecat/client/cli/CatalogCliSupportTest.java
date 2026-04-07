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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogResponse;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CatalogCliSupportTest {

  private static final String UUID_1 = "00000000-0000-0000-0000-000000000001";
  private static final String UUID_2 = "00000000-0000-0000-0000-000000000002";
  private static final String ACCT_ID = "00000000-0000-0000-0000-000000000099";

  private static Catalog catalog(String id, String displayName) {
    return Catalog.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).build())
        .setDisplayName(displayName)
        .build();
  }

  // --- catalogs list ---

  @Test
  void catalogsListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.catalogService.catalogsToList.add(catalog(UUID_1, "my-catalog"));

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalogs",
          List.of(),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});

      String out = buf.toString();
      assertTrue(out.contains("CATALOG_ID"), "expected header");
      assertTrue(out.contains(UUID_1), "expected catalog id");
      assertTrue(out.contains("my-catalog"), "expected display name");
    }
  }

  // --- catalog use ---

  @Test
  void catalogUseResolvesAndSetsCurrentCatalog() throws Exception {
    try (Harness h = new Harness()) {
      h.directoryService.resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();
      AtomicReference<String> current = new AtomicReference<>("");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("use", "my-catalog"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          current::set);

      assertEquals("my-catalog", current.get(), "should set current catalog");
      assertTrue(buf.toString().contains("catalog set:"), "expected confirmation");
      assertTrue(buf.toString().contains(UUID_1), "expected catalog id in output");
    }
  }

  @Test
  void catalogUsePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("use"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- catalog create ---

  @Test
  void catalogCreateSendsSpecAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.catalogService.catalogToReturn = catalog(UUID_2, "new-cat");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("create", "new-cat", "--desc", "My catalog"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});

      assertEquals(1, h.catalogService.createCatalogCalls.get());
      assertEquals("new-cat", h.catalogService.lastCreateRequest.getSpec().getDisplayName());
      assertEquals("My catalog", h.catalogService.lastCreateRequest.getSpec().getDescription());
      assertTrue(buf.toString().contains(UUID_2));
    }
  }

  @Test
  void catalogCreatePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("create"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- catalog get ---

  @Test
  void catalogGetByUuidPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.catalogService.catalogToReturn = catalog(UUID_1, "acme");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("get", UUID_1),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});

      assertEquals(1, h.catalogService.getCatalogCalls.get());
      assertTrue(buf.toString().contains(UUID_1));
    }
  }

  @Test
  void catalogGetPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("get"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- catalog delete ---

  @Test
  void catalogDeleteByUuidPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("delete", UUID_1),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});

      assertEquals(1, h.catalogService.deleteCatalogCalls.get());
      assertEquals(UUID_1, h.catalogService.lastDeleteRequest.getCatalogId().getId());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void catalogDeletePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("delete"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- unknown subcommand ---

  @Test
  void unknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      CatalogCliSupport.handle(
          "catalog",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.catalogsStub,
          h.directoryStub,
          () -> ACCT_ID,
          ignored -> {});
      assertTrue(buf.toString().contains("unknown subcommand"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingCatalogService catalogService;
    final CapturingDirectoryService directoryService;
    final CatalogServiceGrpc.CatalogServiceBlockingStub catalogsStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.catalogService = new CapturingCatalogService();
      this.directoryService = new CapturingDirectoryService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(catalogService)
              .addService(directoryService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.catalogsStub = CatalogServiceGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingCatalogService
      extends CatalogServiceGrpc.CatalogServiceImplBase {

    final AtomicInteger createCatalogCalls = new AtomicInteger();
    final AtomicInteger getCatalogCalls = new AtomicInteger();
    final AtomicInteger deleteCatalogCalls = new AtomicInteger();
    final List<Catalog> catalogsToList = new ArrayList<>();
    Catalog catalogToReturn = Catalog.getDefaultInstance();
    CreateCatalogRequest lastCreateRequest;
    GetCatalogRequest lastGetRequest;
    DeleteCatalogRequest lastDeleteRequest;

    @Override
    public void listCatalogs(
        ListCatalogsRequest request, StreamObserver<ListCatalogsResponse> responseObserver) {
      responseObserver.onNext(
          ListCatalogsResponse.newBuilder().addAllCatalogs(catalogsToList).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createCatalog(
        CreateCatalogRequest request, StreamObserver<CreateCatalogResponse> responseObserver) {
      createCatalogCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(
          CreateCatalogResponse.newBuilder().setCatalog(catalogToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getCatalog(
        GetCatalogRequest request, StreamObserver<GetCatalogResponse> responseObserver) {
      getCatalogCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(GetCatalogResponse.newBuilder().setCatalog(catalogToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateCatalog(
        UpdateCatalogRequest request, StreamObserver<UpdateCatalogResponse> responseObserver) {
      responseObserver.onNext(
          UpdateCatalogResponse.newBuilder().setCatalog(catalogToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteCatalog(
        DeleteCatalogRequest request, StreamObserver<DeleteCatalogResponse> responseObserver) {
      deleteCatalogCalls.incrementAndGet();
      lastDeleteRequest = request;
      responseObserver.onNext(DeleteCatalogResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {

    ResourceId resolvedCatalogId = ResourceId.newBuilder().setId(UUID_1).build();

    @Override
    public void resolveCatalog(
        ResolveCatalogRequest request, StreamObserver<ResolveCatalogResponse> responseObserver) {
      responseObserver.onNext(
          ResolveCatalogResponse.newBuilder().setResourceId(resolvedCatalogId).build());
      responseObserver.onCompleted();
    }
  }
}
