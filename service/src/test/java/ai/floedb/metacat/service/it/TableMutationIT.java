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
import ai.floedb.metacat.common.rpc.PageRequest;
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

  String tablePrefix = this.getClass().getSimpleName() + "_";

  @Test
  void table_rename_and_updateSchema_with_preconditions() throws Exception {
    var cat = TestSupport.createCatalog(mutation, tablePrefix + "cat1", "tcat1");
    String tenantId = TestSupport.seedTenantId(directory, tablePrefix + "cat1");
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_tbl","schema_tbl");
    var nsLeaf = "it_ns";
    var ns = TestSupport.createNamespace(mutation,
        cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var nsPath = new ArrayList<>(parents); 
    nsPath.add(nsLeaf);
    var nsResolved = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
        .build());
    assertEquals(nsId.getId(), nsResolved.getResourceId().getId());

    var tbl = TestSupport.createTable(
        mutation, cat.getResourceId(), nsId,
        "orders", "s3://bucket/orders",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}", "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    var tblResolved = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
            .addAllPath(nsPath).setName("orders"))
        .build());
    assertEquals(tblId.getId(), tblResolved.getResourceId().getId());

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
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
            .addAllPath(nsPath).setName("orders_v2"))
        .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    var nfOld = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder()
            .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
                .addAllPath(nsPath).setName("orders"))
            .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

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

    var beforeSchema = TestSupport.metaForTable(ptr, blob, tblId);
    var newSchema = "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}"
        + ",{\"name\":\"ts\",\"type\":\"timestamp\"}]}";
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

    var readTbl = access.getTableDescriptor(
        GetTableDescriptorRequest.newBuilder().setTableId(tblId).build());
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

    var before = TestSupport.metaForTable(ptr, blob, tblId);
    var noop = mutation.renameTable(RenameTableRequest.newBuilder()
        .setTableId(tblId)
        .setNewDisplayName("orders_v2") // same name
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(before.getPointerVersion())
            .setExpectedEtag(before.getEtag())
            .build())
        .build());
    assertNotNull(noop.getMeta().getPointerKey());

    assertEquals(before.getPointerVersion(), noop.getMeta().getPointerVersion(),
        "version should not bump on identical rename");
    assertEquals(before.getEtag(), noop.getMeta().getEtag(),
        "etag should not change on identical rename");
  }

  @Test
  void table_move_with_preconditions() throws Exception {
    var catName = tablePrefix + "cat2";
    var cat = TestSupport.createCatalog(mutation, catName, "tcat2");
    String tenantId = TestSupport.seedTenantId(directory, catName);
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_tbl","schema_tbl");
    var nsLeaf = "it_ns";
    var ns = TestSupport.createNamespace(mutation,
        cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var parents2 = List.of("db_tbl","schema_tbl");
    var nsLeaf2 = "it_ns2";
    var ns2 = TestSupport.createNamespace(mutation,
        cat.getResourceId(), nsLeaf2, parents2, "ns for tables");
    var nsId2 = ns2.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId2.getKind());

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);
    var nsResolved = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
        .build());
    assertEquals(nsId.getId(), nsResolved.getResourceId().getId());

    var nsPath2 = new ArrayList<>(parents2);
    nsPath2.add(nsLeaf2);
    var nsResolved2 = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath2))
        .build());
    assertEquals(nsId2.getId(), nsResolved2.getResourceId().getId());

    var tbl = TestSupport.createTable(
        mutation, cat.getResourceId(), nsId,
            "orders", "s3://bucket/orders",
                "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}", "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    var tblResolved = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
            .addAllPath(nsPath).setName("orders"))
        .build());
    assertEquals(tblId.getId(), tblResolved.getResourceId().getId());

    var beforeRename = TestSupport.metaForTable(ptr, blob, tblId);
    var r1 = mutation.moveTable(MoveTableRequest.newBuilder()
        .setTableId(tblId)
        .setNewNamespaceId(nsId2)
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(beforeRename.getPointerVersion())
            .setExpectedEtag(beforeRename.getEtag())
            .build())
        .build());
    var m1 = r1.getMeta();
    assertTrue(m1.getPointerVersion() > beforeRename.getPointerVersion());

    // New resolve must succeed
    var resolvedRenamed = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
            .addAllPath(nsPath2).setName("orders"))
        .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    var nfOld = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder()
            .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
                .addAllPath(nsPath).setName("orders"))
            .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    beforeRename = TestSupport.metaForTable(ptr, blob, tblId);
    var r2 = mutation.moveTable(MoveTableRequest.newBuilder()
        .setTableId(tblId)
        .setNewDisplayName("orders_v2")
        .setNewNamespaceId(nsId2)
        .setPrecondition(Precondition.newBuilder()
            .setExpectedVersion(beforeRename.getPointerVersion())
            .setExpectedEtag(beforeRename.getEtag())
            .build())
        .build());
    var m2 = r2.getMeta();
    assertTrue(m2.getPointerVersion() > beforeRename.getPointerVersion());

    resolvedRenamed = directory.resolveTable(ResolveTableRequest.newBuilder()
        .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
            .addAllPath(nsPath2).setName("orders_v2"))
        .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    nfOld = assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder()
            .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName())
                .addAllPath(nsPath).setName("orders"))
            .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
  }

  @Test
  void snapshot_create_delete() throws Exception {
    var catName = tablePrefix + "snap1";
    var cat = TestSupport.createCatalog(mutation, catName, "snap1");
    String tenantId = TestSupport.seedTenantId(directory, catName);
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_tbl","schema_tbl");
    var nsLeaf = "it_ns";
    var ns = TestSupport.createNamespace(
        mutation, cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var tbl = TestSupport.createTable(
        mutation, cat.getResourceId(), nsId,
            "orders", "s3://bucket/orders",
                "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}", "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    for (int i = 0; i < 100; i++) {
      TestSupport.createSnapshot(mutation, tbl.getResourceId(), i,
          System.currentTimeMillis() + i * 1_000L);
    }  
    
    ListSnapshotsRequest req = ListSnapshotsRequest.newBuilder()
        .setTableId(tblId)
        .setPage(PageRequest.newBuilder().setPageSize(1000).build())
        .build();
    ListSnapshotsResponse resp = access.listSnapshots(req);
    assertEquals(100, resp.getSnapshotsCount());
    assertTrue(resp.getPage().getNextPageToken().isEmpty());

    List<Snapshot> snaps = resp.getSnapshotsList();
    for (int i = 0; i < 100; i++) {
      assertEquals(i, snaps.get(i).getSnapshotId());
    }  
  }
}
