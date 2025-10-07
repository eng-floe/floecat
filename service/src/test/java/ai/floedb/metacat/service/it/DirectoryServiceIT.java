package ai.floedb.metacat.service.it;

import java.util.List;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.common.rpc.ResourceId;

@QuarkusTest
class DirectoryServiceIT {
  @GrpcClient("directory")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Test
  void resolveAndLookupCatalog() {
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setDisplayName("sales").build());
    assertEquals("t-0001", r.getResourceId().getTenantId());

    var l = directory.lookupCatalog(LookupCatalogRequest.newBuilder()
      .setResourceId(r.getResourceId()).build());
    assertTrue(l.getDisplayName().equals("sales") || l.getDisplayName().isEmpty());
  }

  private ResourceId resolveCatalogId(String name) {
    var r = directory.resolveCatalog(
      ResolveCatalogRequest.newBuilder().setDisplayName(name).build());
    return r.getResourceId();
  }

  @Test
  void resolveAndLookupNamespace() {
    var salesCat = resolveCatalogId("sales");

    var ref = NamespaceRef.newBuilder()
      .setCatalogId(salesCat)
      .addNamespacePath("staging")
      .addNamespacePath("2025")
      .build();

    var ns = directory.resolveNamespace(
      ResolveNamespaceRequest.newBuilder().setRef(ref).build());

    var lookup = directory.lookupNamespace(
      LookupNamespaceRequest.newBuilder().setResourceId(ns.getResourceId()).build());

    assertEquals(salesCat, lookup.getRef().getCatalogId());
    assertEquals(List.of("staging","2025"), lookup.getRef().getNamespacePathList());
  }
}