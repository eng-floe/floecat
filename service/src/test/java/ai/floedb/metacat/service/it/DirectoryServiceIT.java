package ai.floedb.metacat.service.it;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class DirectoryServiceIT {
  @GrpcClient("directory")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Test
  void resolveAndLookup() {
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
        .setDisplayName("sales").build());
    assertEquals("t-0001", r.getResourceId().getTenantId());

    var l = directory.lookupCatalog(LookupCatalogRequest.newBuilder()
        .setResourceId(r.getResourceId()).build());
    assertTrue(l.getDisplayName().equals("sales") || l.getDisplayName().isEmpty());
  }
}