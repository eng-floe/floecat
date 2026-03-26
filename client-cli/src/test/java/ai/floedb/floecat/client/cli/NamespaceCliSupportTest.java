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

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
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

class NamespaceCliSupportTest {

  private static final String UUID_NS = "00000000-0000-0000-0000-000000000010";
  private static final String UUID_CAT = "00000000-0000-0000-0000-000000000011";
  private static final String ACCT_ID = "00000000-0000-0000-0000-000000000099";

  private static Namespace namespace(String id, String leaf) {
    return Namespace.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).build())
        .setDisplayName(leaf)
        .build();
  }

  // --- namespaces list ---

  @Test
  void namespacesListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.namespaceService.namespacesToList.add(namespace(UUID_NS, "myns"));
      h.directoryService.resolvedCatalogId = ResourceId.newBuilder().setId(UUID_CAT).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespaces",
          List.of("my-catalog"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);

      String out = buf.toString();
      assertTrue(out.contains("NAMESPACE_ID"), "expected header");
      assertTrue(out.contains(UUID_NS), "expected namespace id");
      assertTrue(out.contains("myns"), "expected display name");
    }
  }

  @Test
  void namespacesListPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespaces",
          List.of(),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- namespace create ---

  @Test
  void namespaceCreateSendsSpecAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.namespaceService.namespaceToReturn = namespace(UUID_NS, "myns");
      h.directoryService.resolvedCatalogId = ResourceId.newBuilder().setId(UUID_CAT).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("create", "my-catalog.myns", "--desc", "A namespace"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.namespaceService.createNamespaceCalls.get());
      assertEquals("myns", h.namespaceService.lastCreateRequest.getSpec().getDisplayName());
      assertEquals("A namespace", h.namespaceService.lastCreateRequest.getSpec().getDescription());
      assertTrue(buf.toString().contains(UUID_NS));
    }
  }

  @Test
  void namespaceCreatePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("create"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- namespace get ---

  @Test
  void namespaceGetByUuidCallsServiceAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.namespaceService.namespaceToReturn = namespace(UUID_NS, "myns");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("get", UUID_NS),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.namespaceService.getNamespaceCalls.get());
      assertTrue(buf.toString().contains(UUID_NS));
    }
  }

  @Test
  void namespaceGetPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("get"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- namespace delete ---

  @Test
  void namespaceDeleteByFqCallsServiceAndPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      h.directoryService.resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_NS).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("delete", "my-catalog.myns"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);

      assertEquals(1, h.namespaceService.deleteNamespaceCalls.get());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void namespaceDeletePrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      NamespaceCliSupport.handle(
          "namespace",
          List.of("delete"),
          new PrintStream(buf),
          h.namespacesStub,
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
      NamespaceCliSupport.handle(
          "namespace",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.namespacesStub,
          h.directoryStub,
          () -> ACCT_ID);
      assertTrue(buf.toString().contains("unknown subcommand"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingNamespaceService namespaceService;
    final CapturingDirectoryService directoryService;
    final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespacesStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.namespaceService = new CapturingNamespaceService();
      this.directoryService = new CapturingDirectoryService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(namespaceService)
              .addService(directoryService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.namespacesStub = NamespaceServiceGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingNamespaceService
      extends NamespaceServiceGrpc.NamespaceServiceImplBase {

    final AtomicInteger createNamespaceCalls = new AtomicInteger();
    final AtomicInteger getNamespaceCalls = new AtomicInteger();
    final AtomicInteger deleteNamespaceCalls = new AtomicInteger();
    final List<Namespace> namespacesToList = new ArrayList<>();
    Namespace namespaceToReturn = Namespace.getDefaultInstance();
    CreateNamespaceRequest lastCreateRequest;

    @Override
    public void listNamespaces(
        ListNamespacesRequest request, StreamObserver<ListNamespacesResponse> responseObserver) {
      responseObserver.onNext(
          ListNamespacesResponse.newBuilder().addAllNamespaces(namespacesToList).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createNamespace(
        CreateNamespaceRequest request, StreamObserver<CreateNamespaceResponse> responseObserver) {
      createNamespaceCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(
          CreateNamespaceResponse.newBuilder().setNamespace(namespaceToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getNamespace(
        GetNamespaceRequest request, StreamObserver<GetNamespaceResponse> responseObserver) {
      getNamespaceCalls.incrementAndGet();
      responseObserver.onNext(
          GetNamespaceResponse.newBuilder().setNamespace(namespaceToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void updateNamespace(
        UpdateNamespaceRequest request, StreamObserver<UpdateNamespaceResponse> responseObserver) {
      responseObserver.onNext(
          UpdateNamespaceResponse.newBuilder().setNamespace(namespaceToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteNamespace(
        DeleteNamespaceRequest request, StreamObserver<DeleteNamespaceResponse> responseObserver) {
      deleteNamespaceCalls.incrementAndGet();
      responseObserver.onNext(DeleteNamespaceResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {

    ResourceId resolvedCatalogId = ResourceId.newBuilder().setId(UUID_CAT).build();
    ResourceId resolvedNamespaceId = ResourceId.newBuilder().setId(UUID_NS).build();

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
  }
}
