package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.List;

import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;

@QuarkusTest
class ResourceMutationIT {

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  private final Clock clock = Clock.systemUTC();

  private static ai.floedb.metacat.common.rpc.Error unpackMcError(StatusRuntimeException ex) throws Exception {
    var st = StatusProto.fromThrowable(ex);
    if (st == null) return null;
    for (Any any : st.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        return any.unpack(ai.floedb.metacat.common.rpc.Error.class);
      }
    }
    return null;
  }

  private static ResourceId rid(String tenantId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setTenantId(tenantId).setId(id).setKind(kind).build();
  }

  @Test
  void fullMutationFlow_catalog_namespace_table_CRUD_and_requireEmpty() throws Exception {
    var ref = NameRef.newBuilder().setCatalog("sales").build();
    var seeded = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    var tenantId = seeded.getResourceId().getTenantId();

    var catName = "it_mutation_cat_" + clock.millis();
    var createCat = mutation.createCatalog(
    CreateCatalogRequest.newBuilder()
      .setSpec(CatalogSpec.newBuilder()
        .setDisplayName(catName)
        .setDescription("IT cat")
        .build())
      .build());

    var seededTenant = seeded.getResourceId().getTenantId();

    var createdRid = createCat.getCatalog().getResourceId();

    assertEquals(seededTenant, createdRid.getTenantId(), "Create must set tenant_id from PrincipalContext");

    assertNotEquals(catName, createdRid.getId(), "ResourceId.id must be a UUID, not the display_name");
    assertTrue(createdRid.getId().matches("^[0-9a-fA-F-]{36}$"), "ResourceId.id should look like a UUID");

    assertEquals(ResourceKind.RK_CATALOG, createdRid.getKind());

    assertEquals(catName, createCat.getCatalog().getDisplayName());
    var catRid = createCat.getCatalog().getResourceId();
    var catId = catRid.getId();

    ref = NameRef.newBuilder().setCatalog(catName).build();
    var resolvedCat = directory.resolveCatalog(
      ResolveCatalogRequest.newBuilder().setRef(ref).build());
    assertEquals(catId, resolvedCat.getResourceId().getId());

    var gotCat = access.getCatalog(GetCatalogRequest.newBuilder()
      .setResourceId(catRid)
      .build());
    assertEquals(catName, gotCat.getCatalog().getDisplayName());

    var nsName = "it_schema";
    var nsCreate = mutation.createNamespace(
      CreateNamespaceRequest.newBuilder()
        .setSpec(NamespaceSpec.newBuilder()
          .setCatalogId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
          .setDisplayName(nsName)
          .addAllPath(List.of("db_it", "schema_it"))
          .setDescription("IT ns")
          .build())
        .build());

    var nsId = nsCreate.getNamespace().getResourceId().getId();
    assertEquals(nsName, nsCreate.getNamespace().getDisplayName());

    ResolveNamespaceRequest req = ResolveNamespaceRequest.newBuilder()
      .setRef(NameRef.newBuilder()
        .setCatalog(catName)
        .addAllPath(List.of("db_it", "schema_it"))
        .build())
      .build();

    ResolveNamespaceResponse nsResolved = directory.resolveNamespace(req);

    assertEquals(nsId, nsResolved.getResourceId().getId());

    var tblName = "orders_it";
    var tblCreate = mutation.createTable(
      CreateTableRequest.newBuilder()
        .setSpec(TableSpec.newBuilder()
          .setCatalogId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
          .setNamespaceId(rid(tenantId, nsId, ResourceKind.RK_NAMESPACE))
          .setDisplayName(tblName)
          .setDescription("IT table")
          .setRootUri("s3://bucket/prefix/it")
          .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
          .build())
        .build());

    var tbl = tblCreate.getTable();
    var tblId = tbl.getResourceId().getId();
    assertEquals(tblName, tbl.getDisplayName());

    var tResolve = directory.resolveTable(
      ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder()
          .setCatalog(catName)
          .addAllPath(List.of("db_it", "schema_it"))
          .setName(tblName)
          .build())
        .build());

    assertEquals(tblId, tResolve.getResourceId().getId());

    var gotTbl = access.getTableDescriptor(GetTableDescriptorRequest.newBuilder()
      .setResourceId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
      .build());
    assertEquals(tblName, gotTbl.getTable().getDisplayName());

    var updatedSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"}]}";
    var upd = mutation.updateTableSchema(
      UpdateTableSchemaRequest.newBuilder()
        .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
        .setSchemaJson(updatedSchema)
        .build());
    assertEquals(updatedSchema, upd.getTable().getSchemaJson());

    var newTblName = "orders_it_renamed";
    var rn = mutation.renameTable(
      RenameTableRequest.newBuilder()
        .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
        .setNewDisplayName(newTblName)
        .build());
    assertEquals(newTblName, rn.getTable().getDisplayName());

    var tResolveNew = directory.resolveTable(
      ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder()
            .setCatalog(catName)
            .addAllPath(List.of("db_it", "schema_it"))
            .setName(newTblName)
            .build())
        .build());
    assertEquals(tblId, tResolveNew.getResourceId().getId());

    StatusRuntimeException exOld = assertThrows(StatusRuntimeException.class, () ->
      directory.resolveTable(
        ResolveTableRequest.newBuilder()
          .setRef(NameRef.newBuilder()
            .setCatalog(catName)
            .addAllPath(List.of("db_it", "schema_it"))
            .setName(tblName)
            .build())
          .build()));
    assertEquals(Status.Code.NOT_FOUND, exOld.getStatus().getCode());

    StatusRuntimeException nsDelFail = assertThrows(StatusRuntimeException.class, () ->
      mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
        .setResourceId(rid(tenantId, nsId, ResourceKind.RK_NAMESPACE))
        .setRequireEmpty(true)
        .build()));
    assertEquals(Status.Code.ABORTED, nsDelFail.getStatus().getCode());
    var mcErr = unpackMcError(nsDelFail);
    assertNotNull(mcErr);
    assertEquals("ABORTED", mcErr.getCode());
    assertTrue(mcErr.getMessage().toLowerCase().contains("not empty"));

    mutation.deleteTable(DeleteTableRequest.newBuilder()
      .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
      .build());

    StatusRuntimeException tblGone = assertThrows(StatusRuntimeException.class, () ->
      directory.resolveTable(
        ResolveTableRequest.newBuilder()
          .setRef(NameRef.newBuilder()
            .setCatalog(catId)
            .addAllPath(List.of("db_it","schema_it"))
            .setName(newTblName)
            .build())
          .build()));
    assertEquals(Status.Code.NOT_FOUND, tblGone.getStatus().getCode());

    mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
      .setResourceId(rid(tenantId, nsId, ResourceKind.RK_NAMESPACE))
      .setRequireEmpty(true)
      .build());

    mutation.deleteCatalog(DeleteCatalogRequest.newBuilder()
      .setResourceId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
      .setRequireEmpty(true)
      .build());

    var catNameRef = NameRef.newBuilder().setCatalog(catName).build();
    StatusRuntimeException catGone = assertThrows(StatusRuntimeException.class, () ->
      directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(catNameRef).build()));
    assertEquals(Status.Code.NOT_FOUND, catGone.getStatus().getCode());
  }
}