package ai.floedb.metacat.service.it;

import ai.floedb.metacat.catalog.rpc.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import com.google.protobuf.Any;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class CatalogServiceTest {
  @io.quarkus.grpc.GrpcClient("catalog")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;
  @io.quarkus.grpc.GrpcClient("directory")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Test
  void listCatalogs_returnsSeeded() {
    var resp = catalog.listCatalogs(ListCatalogsRequest.newBuilder().build());
    assertTrue(resp.getCatalogsCount() >= 2, "Expected seeded catalogs");
  }

  @Test
  void getCatalog_returnsSeeded() {
    var req = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setDisplayName("sales").build());
    var resp = catalog.getCatalog(GetCatalogRequest.newBuilder().setResourceId(req.getResourceId()).build());
    assertEquals("sales", resp.getCatalog().getDisplayName());
  }

  @Test
  void getCatalog_notFound_hasCommonError() throws Exception {
    var resolved = directory.resolveCatalog(
        ResolveCatalogRequest.newBuilder().setDisplayName("sales").build());

    var missingRid = resolved.getResourceId().toBuilder()
        .setId("00000000-0000-0000-0000-000000000000")
        .build();

    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
        catalog.getCatalog(GetCatalogRequest.newBuilder().setResourceId(missingRid).build()));

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());

    var rpcStatus = StatusProto.fromThrowable(ex);
    assertNotNull(rpcStatus);

    ai.floedb.metacat.common.rpc.Error mcErr = null;
    for (Any any : rpcStatus.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        mcErr = any.unpack(ai.floedb.metacat.common.rpc.Error.class);
        break;
      }
    }
    assertNotNull(mcErr);
    assertEquals("NOT_FOUND", mcErr.getCode());
    assertTrue(mcErr.getMessage().contains("catalog"));
  }
}