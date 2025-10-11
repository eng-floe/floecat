package ai.floedb.metacat.service.it;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;

@QuarkusTest
class NamespaceIT {
  @io.quarkus.grpc.GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Test
  void listAndGetNamespaces() {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setRef(ref).build());

    var list = access.listNamespaces(ListNamespacesRequest.newBuilder()
      .setCatalogId(r.getResourceId())
      .build());

    assertTrue(list.getNamespacesCount() >= 2);

    var any = list.getNamespaces(0);
    var got = access.getNamespace(GetNamespaceRequest.newBuilder()
      .setNamespaceId(any.getResourceId())
      .build());

    assertEquals(any.getResourceId().getId(), got.getNamespace().getResourceId().getId());
  }
}
