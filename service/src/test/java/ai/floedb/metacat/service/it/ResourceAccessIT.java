package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ResourceAccessIT {
  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Test
  void listCatalogs() {
    var resp = catalog.listCatalogs(ListCatalogsRequest.newBuilder().build());
    assertTrue(resp.getCatalogsCount() >= 2, "Expected seeded catalogs");
  }

  @Test
  void getCatalogNotFound() throws Exception {
    var salesId = TestSupport.resolveCatalogId(directory, "sales");
    var sales =
        catalog.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(salesId).build());
    assertEquals("sales", sales.getCatalog().getDisplayName());

    var missingRid = salesId.toBuilder().setId("00000000-0000-0000-0000-000000000000").build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.getCatalog(
                    GetCatalogRequest.newBuilder().setCatalogId(missingRid).build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }
}
