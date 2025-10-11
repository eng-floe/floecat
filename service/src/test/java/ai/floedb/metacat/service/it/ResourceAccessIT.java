package ai.floedb.metacat.service.it;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import com.google.protobuf.Any;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;

@QuarkusTest
class ResourceAccessIT {
  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub resourceAccess;
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Test
  void listCatalogs_returnsSeeded() {
    var resp = resourceAccess.listCatalogs(ListCatalogsRequest.newBuilder().build());
    assertTrue(resp.getCatalogsCount() >= 2, "Expected seeded catalogs");
  }

  @Test
  void getCatalog_returnsSeeded() {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var req = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setRef(ref).build());
    var resp = resourceAccess.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(req.getResourceId()).build());
    assertEquals("sales", resp.getCatalog().getDisplayName());
  }

  @Test
  void getCatalog_notFound_hasCommonError() throws Exception {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var resolved = directory.resolveCatalog(
      ResolveCatalogRequest.newBuilder().setRef(ref).build());

    var missingRid = resolved.getResourceId().toBuilder()
      .setId("00000000-0000-0000-0000-000000000000")
      .build();

    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
      resourceAccess.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(missingRid).build()));

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
    assertEquals(ErrorCode.MC_NOT_FOUND, mcErr.getCode());
    assertTrue(mcErr.getMessage().contains("Catalog not found"));
  }
}