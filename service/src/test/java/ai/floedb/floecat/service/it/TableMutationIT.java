package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TableMutationIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  private String tablePrefix = this.getClass().getSimpleName() + "_";

  private static final Schema SCHEMA_V1 =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Schema SCHEMA_V2 =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "qty", Types.IntegerType.get()));

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void tableRenameUpdate() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat1", "tcat1");

    var parents = List.of("db_tbl", "schema_tbl");
    var nsLeaf = "it_ns";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);
    var nsResolved =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
                .build());
    assertEquals(nsId.getId(), nsResolved.getResourceId().getId());

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            nsId,
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    var tblResolved =
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath)
                        .setName("orders"))
                .build());
    assertEquals(tblId.getId(), tblResolved.getResourceId().getId());

    var beforeRename = TestSupport.metaForTable(ptr, blob, tblId);
    FieldMask mask = FieldMask.newBuilder().addPaths("display_name").build();
    TableSpec spec = TableSpec.newBuilder().setDisplayName("orders_v2").build();
    var r1 =
        table.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tblId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeRename.getPointerVersion())
                        .setExpectedEtag(beforeRename.getEtag())
                        .build())
                .build());
    var m1 = r1.getMeta();
    assertTrue(m1.getPointerVersion() > beforeRename.getPointerVersion());

    // New resolve must succeed
    var resolvedRenamed =
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath)
                        .setName("orders_v2"))
                .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    var nfOld =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveTable(
                    ResolveTableRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("orders"))
                        .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    TableSpec staleSpec = TableSpec.newBuilder().setDisplayName("orders_v3").build();
    var staleRename =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                table.updateTable(
                    UpdateTableRequest.newBuilder()
                        .setTableId(tblId)
                        .setSpec(staleSpec)
                        .setUpdateMask(mask)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeRename.getPointerVersion()) // stale
                                .setExpectedEtag(beforeRename.getEtag()) // stale
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleRename, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var beforeSchema = TestSupport.metaForTable(ptr, blob, tblId);
    var newSchema =
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}"
            + ",{\"name\":\"ts\",\"type\":\"timestamp\"}]}";

    FieldMask mask_schema = FieldMask.newBuilder().addPaths("schema_json").build();
    TableSpec schemaSpec = TableSpec.newBuilder().setSchemaJson(newSchema).build();
    var s1 =
        table.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tblId)
                .setSpec(schemaSpec)
                .setUpdateMask(mask_schema)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeSchema.getPointerVersion())
                        .setExpectedEtag(beforeSchema.getEtag())
                        .build())
                .build());
    var sm1 = s1.getMeta();
    assertTrue(sm1.getPointerVersion() > beforeSchema.getPointerVersion());

    var readTbl = table.getTable(GetTableRequest.newBuilder().setTableId(tblId).build());
    assertEquals(newSchema, readTbl.getTable().getSchemaJson());

    TableSpec staleSchemaSpec =
        TableSpec.newBuilder()
            .setSchemaJson("{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}")
            .build();
    var staleSchema =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                table.updateTable(
                    UpdateTableRequest.newBuilder()
                        .setTableId(tblId)
                        .setSpec(staleSchemaSpec) // anything
                        .setUpdateMask(mask_schema)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(beforeSchema.getPointerVersion()) // stale
                                .setExpectedEtag(beforeSchema.getEtag()) // stale
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        staleSchema, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    TableSpec sameNameSpec = TableSpec.newBuilder().setDisplayName("orders_v2").build();
    var before = TestSupport.metaForTable(ptr, blob, tblId);
    var noop =
        table.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tblId)
                .setSpec(sameNameSpec)
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(before.getPointerVersion())
                        .setExpectedEtag(before.getEtag())
                        .build())
                .build());
    assertNotNull(noop.getMeta().getPointerKey());

    assertEquals(
        before.getPointerVersion(),
        noop.getMeta().getPointerVersion(),
        "version should not bump on identical rename");
    assertEquals(
        before.getEtag(), noop.getMeta().getEtag(), "etag should not change on identical rename");
  }

  @Test
  void tableDeleteIsIdempotent() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_del", "tcat-del");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db_tbl"), "ns for del");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    assertDoesNotThrow(
        () ->
            table.deleteTable(
                DeleteTableRequest.newBuilder().setTableId(tbl.getResourceId()).build()));
    assertDoesNotThrow(
        () ->
            table.deleteTable(
                DeleteTableRequest.newBuilder().setTableId(tbl.getResourceId()).build()));
  }

  @Test
  void createTableIdempotencyMismatchOnSchema() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_idem", "idem");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db_tbl"), "ns for idem");

    var upstream =
        UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).setUri("s3://b/p").build();

    var key = IdempotencyKey.newBuilder().setKey(tablePrefix + "k-table-1").build();

    var specA =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setDisplayName("idem_tbl")
            .setUpstream(upstream)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .build();

    var specB =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setDisplayName("idem_tbl")
            .setUpstream(upstream)
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"qty\",\"type\":\"int\"}]}")
            .build();

    table.createTable(CreateTableRequest.newBuilder().setSpec(specA).setIdempotency(key).build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                table.createTable(
                    CreateTableRequest.newBuilder().setSpec(specB).setIdempotency(key).build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void tableMove() throws Exception {
    var catName = tablePrefix + "cat2";
    var cat = TestSupport.createCatalog(catalog, catName, "tcat2");

    var parents = List.of("db_tbl", "schema_tbl");
    var nsLeaf = "it_ns";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var parents2 = List.of("db_tbl", "schema_tbl");
    var nsLeaf2 = "it_ns2";
    var ns2 =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf2, parents2, "ns for tables");
    var nsId2 = ns2.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId2.getKind());

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);
    var nsResolved =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath))
                .build());
    assertEquals(nsId.getId(), nsResolved.getResourceId().getId());

    var nsPath2 = new ArrayList<>(parents2);
    nsPath2.add(nsLeaf2);
    var nsResolved2 =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(cat.getDisplayName()).addAllPath(nsPath2))
                .build());
    assertEquals(nsId2.getId(), nsResolved2.getResourceId().getId());

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            nsId,
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    var tblResolved =
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath)
                        .setName("orders"))
                .build());
    assertEquals(tblId.getId(), tblResolved.getResourceId().getId());

    var beforeRename = TestSupport.metaForTable(ptr, blob, tblId);

    FieldMask mask = FieldMask.newBuilder().addPaths("namespace_id").build();
    TableSpec newNamespaceSpec = TableSpec.newBuilder().setNamespaceId(nsId2).build();
    var r1 =
        table.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tblId)
                .setSpec(newNamespaceSpec)
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeRename.getPointerVersion())
                        .setExpectedEtag(beforeRename.getEtag())
                        .build())
                .build());
    var m1 = r1.getMeta();
    assertTrue(m1.getPointerVersion() > beforeRename.getPointerVersion());

    // New resolve must succeed
    var resolvedRenamed =
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath2)
                        .setName("orders"))
                .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    var nfOld =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveTable(
                    ResolveTableRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("orders"))
                        .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");

    beforeRename = TestSupport.metaForTable(ptr, blob, tblId);

    mask = FieldMask.newBuilder().addPaths("display_name").build();
    TableSpec newTableNameSpec =
        TableSpec.newBuilder().setDisplayName("orders_v2").setNamespaceId(nsId2).build();
    var r2 =
        table.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tblId)
                .setSpec(newTableNameSpec)
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(beforeRename.getPointerVersion())
                        .setExpectedEtag(beforeRename.getEtag())
                        .build())
                .build());
    var m2 = r2.getMeta();
    assertTrue(m2.getPointerVersion() > beforeRename.getPointerVersion());

    resolvedRenamed =
        directory.resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(cat.getDisplayName())
                        .addAllPath(nsPath2)
                        .setName("orders_v2"))
                .build());
    assertEquals(tblId.getId(), resolvedRenamed.getResourceId().getId());

    // Old path must be NOT_FOUND
    nfOld =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveTable(
                    ResolveTableRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(cat.getDisplayName())
                                .addAllPath(nsPath)
                                .setName("orders"))
                        .build()));
    TestSupport.assertGrpcAndMc(nfOld, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
  }

  @Test
  void snapshotCreate() throws Exception {
    var catName = tablePrefix + "snap1";
    var cat = TestSupport.createCatalog(catalog, catName, "snap1");

    var parents = List.of("db_tbl", "schema_tbl");
    var nsLeaf = "it_ns";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for tables");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            nsId,
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    for (int i = 0; i < 100; i++) {
      TestSupport.createSnapshot(
          snapshot, tbl.getResourceId(), i, System.currentTimeMillis() + i * 1_000L);
    }

    ListSnapshotsRequest req =
        ListSnapshotsRequest.newBuilder()
            .setTableId(tblId)
            .setPage(PageRequest.newBuilder().setPageSize(1000).build())
            .build();
    ListSnapshotsResponse resp = snapshot.listSnapshots(req);
    assertEquals(100, resp.getSnapshotsCount());
    assertTrue(resp.getPage().getNextPageToken().isEmpty());

    List<Snapshot> snaps = resp.getSnapshotsList();
    for (int i = 0; i < 100; i++) {
      assertEquals(99 - i, snaps.get(i).getSnapshotId());
    }
  }

  @Test
  void snapshotStoresSchemaJsonPerVersion() {
    // create table with schema v1
    var cat = TestSupport.createCatalog(catalog, "snapcat", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(SCHEMA_V1),
            "desc");

    // snapshot1 captures v1
    var snap1 =
        TestSupport.createSnapshot(snapshot, tbl.getResourceId(), 1L, System.currentTimeMillis());

    // update schema to v2, take snapshot2
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSpec(
                TableSpec.newBuilder()
                    .setSchemaJson(SchemaParser.toJson(SCHEMA_V2))
                    .setUpstream(tbl.getUpstream()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());
    var snap2 =
        TestSupport.createSnapshot(snapshot, tbl.getResourceId(), 2L, System.currentTimeMillis());

    // fetch snapshots and assert schemas persisted
    assertEquals(
        SchemaParser.toJson(SCHEMA_V1),
        snapshot
            .getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId()))
                    .build())
            .getSnapshot()
            .getSchemaJson());
    assertEquals(
        SchemaParser.toJson(SCHEMA_V2),
        snapshot
            .getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap2.getSnapshotId()))
                    .build())
            .getSnapshot()
            .getSchemaJson());
  }

  @Test
  void snapshotCreateMismatch() throws Exception {
    var cat = TestSupport.createCatalog(catalog, "snapcat_mismatch", "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(SCHEMA_V1),
            "desc");

    long snapId = 42L;
    var spec1 =
        SnapshotSpec.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSnapshotId(snapId)
            .setUpstreamCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .build();
    snapshot.createSnapshot(CreateSnapshotRequest.newBuilder().setSpec(spec1).build());

    var spec2 =
        SnapshotSpec.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSnapshotId(snapId)
            .setUpstreamCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"x\",\"type\":\"double\"}]}")
            .build();
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                snapshot.createSnapshot(CreateSnapshotRequest.newBuilder().setSpec(spec2).build()));
    TestSupport.assertGrpcAndMc(ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, null);
    var mc = TestSupport.unpackMcError(ex);
    assertNotNull(mc);
    assertEquals("snapshot.mismatch", mc.getMessageKey());
  }
}
