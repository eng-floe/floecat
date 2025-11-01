package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

@QuarkusTest
class NamespaceIT {
  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void listAndGetNamespaces() {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());

    var list =
        namespace.listNamespaces(
            ListNamespacesRequest.newBuilder().setCatalogId(r.getResourceId()).build());

    assertTrue(list.getNamespacesCount() >= 2);

    var any = list.getNamespaces(0);
    var got =
        namespace.getNamespace(
            GetNamespaceRequest.newBuilder().setNamespaceId(any.getResourceId()).build());

    assertEquals(any.getResourceId().getId(), got.getNamespace().getResourceId().getId());
  }
}
