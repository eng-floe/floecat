package ai.floedb.metacat.service.it;

import java.util.ArrayList;
import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@QuarkusTest
class TableMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  String T_PREFIX = this.getClass().getSimpleName() + "_";

  @Test
  void Table_rename_and_updateSchema_with_preconditions() throws Exception {
    // --- Arrange catalog + namespace ---
    String tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, T_PREFIX + "cat1", "tcat1");
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_tbl","schema_tbl");
    var nsLeaf = "it_ns";
    var ns   = TestSupport.createNamespace(mutation, cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    // Resolve namespace path sanity
    var nsPath = new ArrayList<>(parents); 
    nsPath.add(nsLeaf);
    var nsResolved = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
        .build());
    assertEquals(nsId.getId(), nsResolved.getResourceId().getId());

    // --- Create a table under the namespace ---
    var tbl = TestSupport.createTable(
        mutation, cat.getResourceId(), nsId,
        "orders", "s3://bucket/orders", "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}", "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    // Resolve table sanity (assuming Directory supports table resolution by full path)
    var tblResolved = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath).setName("orders"))
        .build());
    assertEquals(tblId.getId(), tblResolved.getResourceId().getId());

    // --- Rename table (happy path) ---
    var beforeRename = TestSupport.metaForTable(ptr, blob, tblId);
    var r1 = mutation.renameTable(RenameTableRequest.newBuilder()
        .setTableId(tblId)
        .setNewDisplayName("orders_v2")
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(beforeRename.getPointerVersion())
            .setExpectedEtag(beforeRename.getEtag())
            .build())
        .build());
    var m1 = r1.getMeta();
    assertTrue(m1.getPointerVersion() > beforeRename.getPointerVersion());

    // New resolve must succeed
    var resolvedRenamed = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath).setName("orders_v2"))
        .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    var nfOld = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder()
            .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath).setName("orders"))
            .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    // --- Rename table with STALE precondition should fail ---
    var staleRename = assertThrows(StatusRuntimeException.class, () ->
        mutation.renameTable(RenameTableRequest.newBuilder()
            .setTableId(tblId)
            .setNewDisplayName("orders_v3")
            .setPrecondition(Precondition.newBuilder()
                .setExpectedVersion(beforeRename.getPointerVersion()) // stale
                .setExpectedEtag(beforeRename.getEtag())              // stale
                .build())
            .build()));
    TestSupport.assertGrpcAndMc(staleRename, Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    // --- Update schema (happy path) ---
    var beforeSchema = TestSupport.metaForTable(ptr, blob, tblId);
    var newSchema = "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"ts\",\"type\":\"timestamp\"}]}";
    var s1 = mutation.updateTableSchema(UpdateTableSchemaRequest.newBuilder()
        .setTableId(tblId)
        .setSchemaJson(newSchema)
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(beforeSchema.getPointerVersion())
            .setExpectedEtag(beforeSchema.getEtag())
            .build())
        .build());
    var sm1 = s1.getMeta();
    assertTrue(sm1.getPointerVersion() > beforeSchema.getPointerVersion());

    var readTbl = access.getTableDescriptor(GetTableDescriptorRequest.newBuilder().setTableId(tblId).build());
    assertEquals(newSchema, readTbl.getTable().getSchemaJson());

    var staleSchema = assertThrows(StatusRuntimeException.class, () ->
        mutation.updateTableSchema(UpdateTableSchemaRequest.newBuilder()
            .setTableId(tblId)
            .setSchemaJson("{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}") // anything
            .setPrecondition(Precondition.newBuilder()
                .setExpectedVersion(beforeSchema.getPointerVersion()) // stale
                .setExpectedEtag(beforeSchema.getEtag())              // stale
                .build())
            .build()));
    TestSupport.assertGrpcAndMc(staleSchema, Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    // --- No-op rename should not break (policy: allow with preconditions) ---
    var currentMeta = TestSupport.metaForTable(ptr, blob, tblId);
    var noop = mutation.renameTable(RenameTableRequest.newBuilder()
        .setTableId(tblId)
        .setNewDisplayName("orders_v2") // same name
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(currentMeta.getPointerVersion())
            .setExpectedEtag(currentMeta.getEtag())
            .build())
        .build());
    assertNotNull(noop.getMeta().getPointerKey()); // version bump policy may vary
  }
}
