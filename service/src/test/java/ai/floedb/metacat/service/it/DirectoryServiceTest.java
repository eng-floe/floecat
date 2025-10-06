package ai.floedb.metacat.service.it;

import ai.floedb.metacat.catalog.rpc.*;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class DirectoryServiceTest {
  @io.quarkus.grpc.GrpcClient("directory")
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