package ai.floedb.metacat.service.it;

import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.util.List;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;

/**
 * End-to-end mutation tests:
 *  - Create Catalog, Namespace, Table
 *  - Resolve via Directory
 *  - Update/Rename table
 *  - require_empty checks on namespace delete
 *  - Delete table + verify it's no longer resolvable
 */
@QuarkusTest
class ResourceMutationIT {

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  private final Clock clock = Clock.systemUTC();

  // --- helpers ---------------------------------------------------------------

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

  // --- tests ----------------------------------------------------------------

  @Test
  void fullMutationFlow_catalog_namespace_table_CRUD_and_requireEmpty() throws Exception {
    // Assumption: test tenant seeded by server; pick tenant from a seeded catalog lookup
    // Resolve a seeded catalog just to get tenant_id for new resources
    var seeded = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setDisplayName("sales").build());
    var tenantId = seeded.getResourceId().getTenantId();

    // -----------------------------------------------------------------------
    // 1) Create a new catalog
    // -----------------------------------------------------------------------
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

    // Must carry correct tenant
    assertEquals(seededTenant, createdRid.getTenantId(), "Create must set tenant_id from PrincipalContext");

    // Must be a UUID (not the display name)
    assertNotEquals(catName, createdRid.getId(), "ResourceId.id must be a UUID, not the display_name");
    assertTrue(createdRid.getId().matches("^[0-9a-fA-F-]{36}$"), "ResourceId.id should look like a UUID");

    // Kind must be RK_CATALOG
    assertEquals(ResourceKind.RK_CATALOG, createdRid.getKind());

    assertEquals(catName, createCat.getCatalog().getDisplayName());
    var catRid = createCat.getCatalog().getResourceId();  // <-- use this (UUID id)
    var catId = catRid.getId();

    // Directory resolve by name should find it (sanity check)
    var resolvedCat = directory.resolveCatalog(
      ResolveCatalogRequest.newBuilder().setDisplayName(catName).build());
    assertEquals(catId, resolvedCat.getResourceId().getId()); // if your Directory returns UUIDs; 
    // if it does not, don't assert equality here—just assert not null.

    // ResourceAccess GetCatalog should use the UUID rid from CreateCatalogResponse
    var gotCat = access.getCatalog(GetCatalogRequest.newBuilder()
        .setResourceId(catRid)
        .build());
    assertEquals(catName, gotCat.getCatalog().getDisplayName());

    // -----------------------------------------------------------------------
    // 2) Create a namespace under that catalog
    // -----------------------------------------------------------------------
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

    // Directory resolve namespace by path should work (path join semantics are implementation-defined)
    var nsResolved = directory.resolveNamespace(
      ResolveNamespaceRequest.newBuilder()
        .setRef(NamespaceRef.newBuilder()
          .setCatalogId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
          .addAllNamespacePath(List.of("db_it","schema_it"))
          .build())
        .build());
    assertEquals(nsId, nsResolved.getResourceId().getId());

    // -----------------------------------------------------------------------
    // 3) Create a table
    // -----------------------------------------------------------------------
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

    // Directory resolve table by FQ name should work
    var tResolve = directory.resolveTable(
      ResolveTableRequest.newBuilder()
        .setName(NameRef.newBuilder()
          .setCatalog(catId)
          .addAllNamespacePath(List.of(nsId))
          .setName(tblName)
          .build())
        .build());

    assertEquals(tblId, tResolve.getResourceId().getId());

    // ResourceAccess GetTableDescriptor returns it
    var gotTbl = access.getTableDescriptor(GetTableDescriptorRequest.newBuilder()
        .setCatalogId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
        .setNamespaceId(rid(tenantId, nsId, ResourceKind.RK_NAMESPACE))
        .setResourceId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
        .build());
    assertEquals(tblName, gotTbl.getTable().getDisplayName());

    // -----------------------------------------------------------------------
    // 4) Update table schema
    // -----------------------------------------------------------------------
    var updatedSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"}]}";
    var upd = mutation.updateTableSchema(
        UpdateTableSchemaRequest.newBuilder()
            .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
            .setSchemaJson(updatedSchema)
            .build());
    assertEquals(updatedSchema, upd.getTable().getSchemaJson());

    // -----------------------------------------------------------------------
    // 5) Rename table
    // -----------------------------------------------------------------------
    var newTblName = "orders_it_renamed";
    var rn = mutation.renameTable(
        RenameTableRequest.newBuilder()
            .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
            .setNewDisplayName(newTblName)
            .build());
    assertEquals(newTblName, rn.getTable().getDisplayName());

    // Resolve by new name should work; old name should NOT
    var tResolveNew = directory.resolveTable(
        ResolveTableRequest.newBuilder()
            .setName(NameRef.newBuilder()
                .setCatalog(catId)
                .addAllNamespacePath(List.of(nsId))
                .setName(newTblName)
                .build())
            .build());
    assertEquals(tblId, tResolveNew.getResourceId().getId());

    StatusRuntimeException exOld = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setName(NameRef.newBuilder()
                    .setCatalog(catId)
                    .addAllNamespacePath(List.of(nsId))
                    .setName(tblName)
                    .build())
                .build()));
    assertEquals(Status.Code.NOT_FOUND, exOld.getStatus().getCode());

    // -----------------------------------------------------------------------
    // 6) require_empty on namespace delete should FAIL while the table exists
    // -----------------------------------------------------------------------
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

    // -----------------------------------------------------------------------
    // 7) Delete table, then namespace delete succeeds
    // -----------------------------------------------------------------------
    mutation.deleteTable(DeleteTableRequest.newBuilder()
        .setTableId(rid(tenantId, tblId, ResourceKind.RK_TABLE))
        .build());

    // Resolving the table should now be NOT_FOUND
    StatusRuntimeException tblGone = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setName(NameRef.newBuilder()
                    .setCatalog(catId)
                    .addAllNamespacePath(List.of("db_it","schema_it"))
                    .setName(newTblName)
                    .build())
                .build()));
    assertEquals(Status.Code.NOT_FOUND, tblGone.getStatus().getCode());

    // Namespace delete (require_empty) now succeeds
    mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
        .setResourceId(rid(tenantId, nsId, ResourceKind.RK_NAMESPACE))
        .setRequireEmpty(true)
        .build());

    // -----------------------------------------------------------------------
    // 8) Finally, delete the catalog (require_empty should pass if no namespaces remain)
    // -----------------------------------------------------------------------
    mutation.deleteCatalog(DeleteCatalogRequest.newBuilder()
        .setResourceId(rid(tenantId, catId, ResourceKind.RK_CATALOG))
        .setRequireEmpty(true)
        .build());

    // Looking it up by name should now fail
    StatusRuntimeException catGone = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setDisplayName(catName).build()));
    assertEquals(Status.Code.NOT_FOUND, catGone.getStatus().getCode());
  }
}