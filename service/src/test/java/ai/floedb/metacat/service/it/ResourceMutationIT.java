package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
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

  @Test
  void fullMutationFlow_crud_rename_update_requireEmpty_and_errors() throws Exception {
    String tenantId = TestSupport.seedTenantId(directory, "sales");

    String catName = "it_mutation_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(mutation, catName, "IT cat");
    ResourceId catId = cat.getResourceId();

    assertEquals(ResourceKind.RK_CATALOG, catId.getKind());
    assertEquals(tenantId, catId.getTenantId());
    assertTrue(catId.getId().matches("^[0-9a-fA-F-]{36}$"), "id must look like UUID");

    assertEquals(catId.getId(), TestSupport.resolveCatalogId(directory, catName).getId());
    assertEquals(catName, access.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(catId).build())
      .getCatalog().getDisplayName());

    var nsPath = List.of("db_it","schema_it");
    String nsLeaf = "it_schema";
    Namespace ns = TestSupport.createNamespace(mutation, catId, nsLeaf, nsPath, "IT ns");
    ResourceId nsId = ns.getResourceId();
    var nsFullPath = new ArrayList<>(nsPath); nsFullPath.add(nsLeaf);
    assertEquals(nsId.getId(), TestSupport.resolveNamespaceId(directory, catName, nsFullPath).getId());

    String schema = """
      {"type":"struct","fields":[{"name":"id","type":"long"}]}
      """.trim();
    TableDescriptor tbl = TestSupport.createTable(
      mutation, catId, nsId, "orders_it", "s3://bucket/prefix/it", schema, "IT table");
    ResourceId tblId = tbl.getResourceId();
    assertEquals(tblId.getId(),
      TestSupport.resolveTableId(directory, catName, nsFullPath, "orders_it").getId());

    String schemaV2 = """
      {"type":"struct","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}
      """.trim();
    TableDescriptor upd = TestSupport.updateSchema(mutation, tblId, schemaV2);
    assertEquals(schemaV2, upd.getSchemaJson());

    String newName = "orders_it_renamed";
    TableDescriptor renamed = TestSupport.renameTable(mutation, tblId, newName);
    assertEquals(newName, renamed.getDisplayName());
    assertEquals(tblId.getId(),
      TestSupport.resolveTableId(directory, catName, nsFullPath, newName).getId());
    
    StatusRuntimeException oldName404 = assertThrows(StatusRuntimeException.class, () ->
      TestSupport.resolveTableId(directory, catName, nsFullPath, "orders_it"));
    TestSupport.assertGrpcAndMc(oldName404, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, null);

    StatusRuntimeException nsDelBlocked = assertThrows(StatusRuntimeException.class, () ->
      mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
        .setNamespaceId(nsId)
        .setRequireEmpty(true)
        .build()));
    TestSupport.assertGrpcAndMc(nsDelBlocked, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, 
      "Namespace \"db_it/schema_it/it_schema\" contains tables and/or children.");

    TestSupport.deleteTable(mutation, nsId, tblId);

    StatusRuntimeException tblGone = assertThrows(StatusRuntimeException.class, () ->
      TestSupport.resolveTableId(directory, catName, nsFullPath, newName));
    TestSupport.assertGrpcAndMc(tblGone, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, null);

    TestSupport.deleteNamespace(mutation, nsId, true);
    TestSupport.deleteCatalog(mutation, catId, true);

    StatusRuntimeException catGone = assertThrows(StatusRuntimeException.class, () ->
      TestSupport.resolveCatalogId(directory, catName));
    TestSupport.assertGrpcAndMc(catGone, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }
}
