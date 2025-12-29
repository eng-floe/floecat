package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

@QuarkusTest
class NamespaceIT {
  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
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
    var ref = NameRef.newBuilder().setCatalog("examples").build();
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());

    var list =
        namespace.listNamespaces(
            ListNamespacesRequest.newBuilder().setCatalogId(r.getResourceId()).build());

    assertTrue(list.getNamespacesCount() >= 1);

    var any = list.getNamespaces(0);
    var got =
        namespace.getNamespace(
            GetNamespaceRequest.newBuilder().setNamespaceId(any.getResourceId()).build());

    assertEquals(any.getResourceId().getId(), got.getNamespace().getResourceId().getId());
  }
}
