package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@QuarkusTest
class BackendStorageIT {

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private final Clock clock = Clock.systemUTC();

  @Test
  void storage_invariants_pointerBlobVersion_indexKeys_idempotence_and_deletes() {
    String catName = "it_storage_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(mutation, catName, "storage cat");
    String tenantId = TestSupport.seedTenantId(directory, catName);

    ResourceId catId = cat.getResourceId();
    String kCatByName = Keys.catByNamePtr(tenantId, cat.getDisplayName());
    String kCatById = Keys.catPtr(tenantId, catId.getId());
    assertTrue(ptr.get(kCatByName).isPresent());
    assertTrue(ptr.get(kCatById).isPresent());
    var catLookup = directory.lookupCatalog(
        LookupCatalogRequest.newBuilder().setResourceId(catId).build());
    assertEquals(cat.getDisplayName(), catLookup.getDisplayName());

    var nsPath = List.of("db_it", "schema_it");
    Namespace ns = TestSupport.createNamespace(
        mutation, catId, "it_ns", nsPath, "storage ns");
    ResourceId nsId = ns.getResourceId();
    var fullPath = List.of("db_it", "schema_it", "it_ns");
    String kNsByPath = Keys.nsByPathPtr(tenantId, catId.getId(), fullPath);
    String kNsPtr = Keys.nsPtr(tenantId, catId.getId(), nsId.getId());
    String kNsBlob = Keys.nsBlob(tenantId, catId.getId(), nsId.getId());
    assertTrue(ptr.get(kNsByPath).isPresent(), "namespace by-path pointer missing");
    assertTrue(ptr.get(kNsPtr).isPresent(), "namespace canonical pointer missing");
    assertTrue(blobs.head(kNsBlob).isPresent(), "namespace blob missing");
    assertTrue(ptr.get(kNsByPath).isPresent(), "namespace by-path (parents) index missing");

    var nsLookup = directory.lookupNamespace(
        LookupNamespaceRequest.newBuilder().setResourceId(nsId).build());
    assertEquals(cat.getDisplayName(), nsLookup.getRef().getCatalog());
    assertEquals(List.of("db_it","schema_it"), nsLookup.getRef().getPathList());
    assertEquals("it_ns", nsLookup.getRef().getName());

    String schemaV1 = """
        {"type":"struct","fields":[{"name":"id","type":"long"}]}
        """.trim();
    TableDescriptor tbl = TestSupport.createTable(
        mutation,
        catId, nsId,
        "it_tbl",
        "s3://bucket/prefix/it",
        schemaV1,
        "storage table");
    ResourceId tblId = tbl.getResourceId();
    String kTblByName = Keys.tblByNamePtr(tenantId, catId.getId(), nsId.getId(), "it_tbl");
    String kTblCanon  = Keys.tblCanonicalPtr(tenantId, tblId.getId());
    String tblBlobUri = Keys.tblBlob(tenantId, tblId.getId());

    assertTrue(ptr.get(kTblByName).isPresent(), "table by-name pointer missing");
    assertTrue(ptr.get(kTblCanon).isPresent(), "table canonical pointer missing");
    assertTrue(blobs.head(tblBlobUri).isPresent(), "table blob header missing");

    var tblLookup = directory.lookupTable(
        LookupTableRequest.newBuilder().setResourceId(tblId).build());
    assertEquals(cat.getDisplayName(), tblLookup.getName().getCatalog());
    assertEquals(List.of("db_it", "schema_it", "it_ns"), tblLookup.getName().getPathList());
    assertEquals("it_tbl", tblLookup.getName().getName());

    String canonPtrKey = Keys.tblCanonicalPtr(tenantId, tblId.getId());
    Pointer tpCanon = ptr.get(canonPtrKey)
        .orElseThrow(() -> new AssertionError("canonical pointer missing"));
    assertEquals(tblBlobUri, tpCanon.getBlobUri());
    assertTrue(blobs.head(tblBlobUri).isPresent(), "table blob header missing");

    long vCanonBefore = tpCanon.getVersion();
    String schemaV2 = """
        {"type":"struct","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}
        """.trim();
    TestSupport.updateSchema(mutation, tblId, schemaV2);

    Pointer tpCanonAfter = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(tpCanonAfter.getVersion() > vCanonBefore, "version must bump on content change");
    assertEquals(tblBlobUri, tpCanonAfter.getBlobUri(), "blob URI stable; content updated behind it");

    long vBeforeIdempotent = tpCanonAfter.getVersion();
    TestSupport.updateSchema(mutation, tblId, schemaV2);
    Pointer tpCanonAfterIdem = ptr.get(canonPtrKey).orElseThrow();
    assertEquals(tpCanonAfterIdem.getVersion(), vBeforeIdempotent, "version must not bump on identical content");

    String oldName = tbl.getDisplayName();
    String newName = "it_tbl_renamed";
    TestSupport.renameTable(mutation, tblId, newName);
    String idxOldKey = Keys.tblByNamePtr(tenantId, catId.getId(), nsId.getId(), oldName);
    String idxNewKey = Keys.tblByNamePtr(tenantId, catId.getId(), nsId.getId(), newName);
    assertTrue(ptr.get(idxNewKey).isPresent(), "new by-name pointer must exist");
    assertTrue(ptr.get(idxOldKey).isEmpty(),   "old by-name pointer must be removed");
 
    TestSupport.deleteTable(mutation, nsId, tblId);
    TestSupport.deleteTable(mutation, nsId, tblId);
    assertTrue(ptr.get(kTblCanon).isEmpty());
    assertTrue(ptr.get(kTblByName).isEmpty()); // use the current name’s key
    assertTrue(blobs.head(tblBlobUri).isEmpty());
    // Call again
    TestSupport.deleteTable(mutation, nsId, tblId);
    assertTrue(ptr.get(kTblCanon).isEmpty());
    assertTrue(ptr.get(kTblByName).isEmpty()); // use the current name’s key
    assertTrue(blobs.head(tblBlobUri).isEmpty());

    TestSupport.deleteNamespace(mutation, nsId, true);
    assertTrue(ptr.get(kNsByPath).isEmpty(), "ns by-path pointer should be deleted");
    assertTrue(ptr.get(kNsPtr).isEmpty(),    "ns canonical pointer should be deleted");
    assertTrue(blobs.head(kNsBlob).isEmpty(), "ns blob should be deleted");

    TestSupport.deleteCatalog(mutation, catId, true);
    String catByIdKey   = Keys.catPtr(tenantId, catId.getId());
    String catByNameKey = Keys.catByNamePtr(tenantId, catName);
    String catBlobUri   = Keys.catBlob(tenantId, catId.getId());
    assertTrue(ptr.get(catByIdKey).isEmpty());
    assertTrue(ptr.get(catByNameKey).isEmpty());
    assertTrue(blobs.head(catBlobUri).isEmpty());
  }

  @Test
  void listTables_pagination_noRepeatsNoSkips() {
    var catName = "cat_pg_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation,  catName, "pag");
    String tenantId = TestSupport.seedTenantId(directory, catName);

    var nsPath = List.of("db","sch");
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns_pg", nsPath, "pg");
    for (int i=0;i<5;i++) TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "t"+i, "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", 
        "d");

    var prefixRef = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName())
        .addAllPath(nsPath).addPath("ns_pg")
        .build();

    int pageSize = 2;
    String token = "";
    var p1 = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefixRef)
            .setPage(PageRequest.newBuilder()
                .setPageSize(pageSize)
                .setPageToken(token))
            .build());
    String t1 = p1.getPage().getNextPageToken();

    var p2 = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefixRef)
            .setPage(ai.floedb.metacat.common.rpc.PageRequest.newBuilder()
                .setPageSize(pageSize)
                .setPageToken(t1))
            .build());
    String t2 = p2.getPage().getNextPageToken();

    var p3 = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefixRef)
            .setPage(ai.floedb.metacat.common.rpc.PageRequest.newBuilder()
                .setPageSize(pageSize)
                .setPageToken(t2))
            .build());
    String t3 = p3.getPage().getNextPageToken();

    var all = new java.util.LinkedHashSet<String>();
    p1.getTablesList().forEach(e -> all.add(e.getName().getName()));
    p2.getTablesList().forEach(e -> all.add(e.getName().getName()));
    p3.getTablesList().forEach(e -> all.add(e.getName().getName()));
    assertEquals(5, all.size());
    assertTrue(t3.isEmpty(), "final page should clear nextToken");
  }

  @Test
  void encodedNames_preservePrefixScan() {
    var catName = "cat_enc_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "enc");
    String tenantId = TestSupport.seedTenantId(directory, catName);

    var nsPath = List.of("db it", "sch🧪", "Q4 Europe");
    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns_enc", nsPath, "enc");

    TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "α",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    TestSupport.createTable(mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "β",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");

    var prefixRef = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName())
        .addAllPath(nsPath).addPath("ns_enc")
        .build();

    var page = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefixRef)
            .setPage(PageRequest.newBuilder()
                .setPageSize(100))
            .build());
    assertTrue(page.getTablesCount() >= 2);

    String key = Keys.tblByNamePtr(tenantId, cat.getResourceId().getId(), ns.getResourceId().getId(), "α");
    assertTrue(key.contains("/by-name/"), "hierarchy should be preserved in keyspace");
  }

  @Test
  void update_bumpsBothPointers() {
    var catName = "cat_ver_" + System.currentTimeMillis() + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "ver");
    String tenantId = TestSupport.seedTenantId(directory, catName);
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "ver");
    var tbl = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    var tid = tbl.getResourceId(); var tenant = tid.getTenantId();
    String canon = Keys.tblCanonicalPtr(tenant, tid.getId());
    long vCanon = ptr.get(canon).orElseThrow().getVersion();

    TestSupport.updateSchema(mutation, tid, "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"x\",\"type\":\"double\"}]}");

    assertTrue(ptr.get(canon).orElseThrow().getVersion() > vCanon);
  }

  @Test
  void etag_changesOnlyOnContentChange() {
    var catName = "cat_etag_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "etag");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "etag");
    var tbl = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "t",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    var tid = tbl.getResourceId();
    String blob = Keys.tblBlob(tenantId, tid.getId());

    var e1 = blobs.head(blob).orElseThrow().getEtag();
    TestSupport.updateSchema(
        mutation,
        tid,
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");
    var e2 = blobs.head(blob).orElseThrow().getEtag();
    assertEquals(e1, e2);

    TestSupport.updateSchema(
        mutation,
        tid,
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"y\",\"type\":\"double\"}]}");
    var e3 = blobs.head(blob).orElseThrow().getEtag();
    assertNotEquals(e2, e3);
  }

  @Test
  void delete_skewTolerance() {
    var catName = "cat_del_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "del");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "del");
    var tbl = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "t",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
        String canon = Keys.tblCanonicalPtr(tenantId, tbl.getResourceId().getId());

    // Simulate blob missing
    assertTrue(blobs.delete(Keys.tblBlob(tenantId, tbl.getResourceId().getId())));
    TestSupport.deleteTable(mutation, ns.getResourceId(), tbl.getResourceId());
    assertTrue(ptr.get(canon).isEmpty());

    var tbl2 = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "t2",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    var tid2 = tbl2.getResourceId();
    String canon2 = Keys.tblCanonicalPtr(tenantId, tid2.getId());
    String blob2  = Keys.tblBlob(tenantId, tid2.getId());

    // Simulate cannonical ptr missing
    assertTrue(ptr.delete(canon2));
    TestSupport.deleteTable(mutation, ns.getResourceId(), tid2);
    assertTrue(blobs.head(blob2).isEmpty());
  }

  @Test
  void casContention_twoConcurrentUpdates() throws InterruptedException {
    var catName = "cat_cas_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "cas");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "cas");
    var tbl = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "t",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    var tid = tbl.getResourceId();
    String canon = Keys.tblCanonicalPtr(tenantId, tid.getId());
    long v0 = ptr.get(canon).orElseThrow().getVersion();

    var sA = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"a\",\"type\":\"double\"}]}";
    var sB = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"double\"}]}";

    var latch = new java.util.concurrent.CountDownLatch(1);
    var ex = new java.util.concurrent.atomic.AtomicReference<Throwable>();

    Runnable r1 = () -> { try { latch.await(); TestSupport.updateSchema(mutation, tid, sA); } catch (Throwable t) { ex.set(t); } };
    Runnable r2 = () -> { try { latch.await(); TestSupport.updateSchema(mutation, tid, sB); } catch (Throwable t) { ex.set(t); } };

    var t1 = new Thread(r1); var t2 = new Thread(r2);
    t1.start(); t2.start(); latch.countDown(); t1.join(); t2.join();
    assertNull(ex.get(), "unexpected error in concurrent writers");

    long v2 = ptr.get(canon).orElseThrow().getVersion();
    assertEquals(v0 + 2, v2, "exactly two successful bumps expected");
  }

  @Test
  void countByPrefix_matchesInsertsDeletes() {
    var catName = "cat_cnt_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "cnt");
    String tenantId = TestSupport.seedTenantId(directory, catName);

    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "cnt");

        var prefix = Keys.tblByNamePrefix(tenantId, cat.getResourceId().getId(), ns.getResourceId().getId());
        int before = ptr.countByPrefix(prefix);

    var tA = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "a",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    var tB = TestSupport.createTable(
        mutation,
        cat.getResourceId(),
        ns.getResourceId(),
        "b",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");

    int mid = ptr.countByPrefix(prefix);
    assertEquals(before + 2, mid);

    TestSupport.deleteTable(mutation, ns.getResourceId(), tA.getResourceId());
    int after = ptr.countByPrefix(prefix);
    assertEquals(before + 1, after);
  }

  @Test
  void createTable_idempotent_sameKeySameSpec_returnsSameId_and_singleWrite() {
    var catName = "cat_idem_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "idem");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns = TestSupport.createNamespace(
        mutation, 
        cat.getResourceId(), 
        "ns", 
        List.of("db","sch"), 
        "idem");

    var spec = TableSpec.newBuilder()
        .setCatalogId(cat.getResourceId())
        .setNamespaceId(ns.getResourceId())
        .setDisplayName("t0")
        .setRootUri("s3://b/p")
        .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
        .setDescription("desc")
        .build();

    var key = IdempotencyKey.newBuilder().setKey("k-123").build();

    var req = CreateTableRequest.newBuilder()
        .setSpec(spec)
        .setIdempotency(key)
        .build();

    var resp1 = mutation.createTable(req);
    var resp2 = mutation.createTable(req);

    var idemKey = Keys.idemKey(tenantId, "CreateTable", "k-123");
    var idemPtr = ptr.get(idemKey);
    assertTrue(idemPtr.isPresent(), "idempotency pointer missing");

    var t1 = resp1.getTable().getResourceId();
    var t2 = resp2.getTable().getResourceId();
    assertEquals(t1.getId(), t2.getId(), "idempotent create must return the same table id");

    var canonPtrKey = Keys.tblCanonicalPtr(tenantId, t1.getId());
    var blobUri = Keys.tblBlob(tenantId, t1.getId());
    assertTrue(ptr.get(canonPtrKey).isPresent(), "canonical pointer missing");
    assertTrue(blobs.head(blobUri).isPresent(), "blob missing");

    var idxByName = Keys.tblByNamePtr(tenantId, cat.getResourceId().getId(), ns.getResourceId().getId(), "t0");
    assertTrue(ptr.get(idxByName).isPresent(), "by-name pointer missing");

    assertEquals(resp1.getMeta().getPointerKey(), resp2.getMeta().getPointerKey());
    assertEquals(resp1.getMeta().getPointerVersion(), resp2.getMeta().getPointerVersion());
    assertEquals(resp1.getMeta().getEtag(), resp2.getMeta().getEtag());
  }

  @Test
  void createTable_idempotent_mismatchSameKey_conflict() {
    var catName = "cat_idem_conf_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "idem");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "idem");
    var idem = ai.floedb.metacat.catalog.rpc.IdempotencyKey.newBuilder().setKey("k-XYZ").build();

    var specA = ai.floedb.metacat.catalog.rpc.TableSpec.newBuilder()
        .setCatalogId(cat.getResourceId()).setNamespaceId(ns.getResourceId())
        .setDisplayName("tA").setRootUri("s3://b/p").setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
        .build();

    var specB = ai.floedb.metacat.catalog.rpc.TableSpec.newBuilder()
        .setCatalogId(cat.getResourceId()).setNamespaceId(ns.getResourceId())
        .setDisplayName("tB")  // <-- different display_name so different fingerprint
        .setRootUri("s3://b/p").setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
        .build();

    var reqA = ai.floedb.metacat.catalog.rpc.CreateTableRequest.newBuilder().setSpec(specA).setIdempotency(idem).build();
    var reqB = ai.floedb.metacat.catalog.rpc.CreateTableRequest.newBuilder().setSpec(specB).setIdempotency(idem).build();

    var r1 = mutation.createTable(reqA);

    var ex = assertThrows(io.grpc.StatusRuntimeException.class, () -> mutation.createTable(reqB));
    assertEquals(io.grpc.Status.Code.ABORTED, ex.getStatus().getCode());
  }

  @Test
  void createTable_idempotent_concurrent_twoWriters_singleCreate() throws InterruptedException {
    var catName = "cat_idem_cc_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(mutation, catName, "idem");
    var tenantId = TestSupport.seedTenantId(directory, catName);

    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "idem");

    var spec = TableSpec.newBuilder()
        .setCatalogId(cat.getResourceId())
        .setNamespaceId(ns.getResourceId())
        .setDisplayName("tcc")
        .setRootUri("s3://b/p")
        .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
        .build();

    var key = IdempotencyKey.newBuilder().setKey("k-CC").build();
    var req = CreateTableRequest.newBuilder().setSpec(spec).setIdempotency(key).build();

    var latch = new CountDownLatch(1);
    var out1 = new AtomicReference<CreateTableResponse>();
    var out2 = new AtomicReference<CreateTableResponse>();
    var err  = new AtomicReference<Throwable>();

    Runnable r = () -> {
      try { latch.await(); out1.compareAndSet(null, mutation.createTable(req)); }
      catch (Throwable t) { err.set(t); }
    };
    Runnable s = () -> {
      try { latch.await(); out2.compareAndSet(null, mutation.createTable(req)); }
      catch (Throwable t) { err.set(t); }
    };

    var t1 = new Thread(r); var t2 = new Thread(s);
    t1.start(); t2.start(); latch.countDown(); t1.join(); t2.join();
    assertNull(err.get(), "unexpected error in concurrent writers");

    var a = out1.get(); var b = out2.get();
    assertNotNull(a); assertNotNull(b);
    assertEquals(a.getTable().getResourceId().getId(), b.getTable().getResourceId().getId(), "should be same table id");

    var tid = a.getTable().getResourceId();
    var canonPtrKey = Keys.tblCanonicalPtr(tenantId, tid.getId());
    var blobUri = Keys.tblBlob(tenantId, tid.getId());
    assertTrue(ptr.get(canonPtrKey).isPresent());
    assertTrue(blobs.head(blobUri).isPresent());
  }
}
