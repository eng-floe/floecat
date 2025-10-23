package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.catalog.rpc.ResourceMutationGrpc;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CatalogMutationIT {
  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Test
  void CatalogExists() throws Exception {
    TestSupport.createCatalog(mutation, catalogPrefix + "cat1", "cat1");

    StatusRuntimeException catExists =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.createCatalog(mutation, catalogPrefix + "cat1", "cat1"));

    TestSupport.assertGrpcAndMc(
        catExists,
        Status.Code.ABORTED,
        ErrorCode.MC_CONFLICT,
        "Catalog \"" + catalogPrefix + "cat1\" already exists");
  }

  @Test
  void catalogCreateUpdateDelete() throws Exception {
    String tenantId = TestSupport.seedTenantId(directory, "sales");

    var c1 = TestSupport.createCatalog(mutation, catalogPrefix + "cat_pre", "desc");
    var id = c1.getResourceId();

    assertEquals(ResourceKind.RK_CATALOG, id.getKind());
    assertEquals(tenantId, id.getTenantId());
    assertTrue(id.getId().matches("^[0-9a-fA-F-]{36}$"), "id must look like UUID");

    var m1 =
        mutation
            .updateCatalog(
                UpdateCatalogRequest.newBuilder()
                    .setCatalogId(id)
                    .setSpec(
                        CatalogSpec.newBuilder()
                            .setDisplayName(catalogPrefix + "cat_pre")
                            .setDescription("desc")
                            .build())
                    .build())
            .getMeta();

    var resolved =
        directory.resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(catalogPrefix + "cat_pre"))
                .build());
    assertEquals(id.getId(), resolved.getResourceId().getId());

    var spec2 =
        CatalogSpec.newBuilder()
            .setDisplayName(catalogPrefix + "cat_pre_2")
            .setDescription("desc2")
            .build();
    var updOk =
        mutation.updateCatalog(
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setSpec(spec2)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());
    assertEquals(catalogPrefix + "cat_pre_2", updOk.getCatalog().getDisplayName());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                mutation.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(id)
                        .setSpec(
                            CatalogSpec.newBuilder().setDisplayName(catalogPrefix + "cat_pre_3"))
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(123456L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var m2 = updOk.getMeta();
    var delOk =
        mutation.deleteCatalog(
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setRequireEmpty(true)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m2.getPointerVersion())
                        .setExpectedEtag(m2.getEtag())
                        .build())
                .build());
    assertEquals(m2.getPointerKey(), delOk.getMeta().getPointerKey());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveCatalog(
                    ResolveCatalogRequest.newBuilder()
                        .setRef(NameRef.newBuilder().setCatalog(catalogPrefix + "cat_pre_2"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }

  @Test
  void catalogCreateIdempotent() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-1").build();
    var spec =
        CatalogSpec.newBuilder()
            .setDisplayName(catalogPrefix + "idem_cat")
            .setDescription("x")
            .build();

    var r1 =
        mutation.createCatalog(
            CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        mutation.createCatalog(
            CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getCatalog().getResourceId().getId(), r2.getCatalog().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void catalogCreateIdempotencyMismatch() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-2").build();

    mutation.createCatalog(
        CreateCatalogRequest.newBuilder()
            .setSpec(CatalogSpec.newBuilder().setDisplayName(catalogPrefix + "idem_cat2").build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                mutation.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(
                            CatalogSpec.newBuilder()
                                .setDisplayName(catalogPrefix + "idem_cat2_DIFFERENT")
                                .build())
                        .setIdempotency(key)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }
}
