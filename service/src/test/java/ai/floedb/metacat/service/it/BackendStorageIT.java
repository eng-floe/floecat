package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.List;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
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
  @Inject NameIndexRepository nameIndex;

  private final Clock clock = Clock.systemUTC();

  @Test
  void storage_invariants_pointerBlobVersion_indexKeys_idempotence_and_deletes() {
    String tenantId = TestSupport.seedTenantId(directory, "sales");

    String catName = "it_storage_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(mutation, catName, "storage cat");
    ResourceId catId = cat.getResourceId();
    String kCatByName = Keys.idxCatByName(tenantId, cat.getDisplayName());
    String kCatById = Keys.idxCatById(tenantId, catId.getId());
    assertTrue(ptr.get(kCatByName).isPresent());
    assertTrue(ptr.get(kCatById).isPresent());
    var nrById = nameIndex.getCatalogById(tenantId, catId.getId()).orElseThrow();
    assertEquals(cat.getDisplayName(), nrById.getCatalog());
    assertEquals(catId.getId(), nrById.getResourceId().getId());

    var nsPath = List.of("db_it", "schema_it");
    Namespace ns = TestSupport.createNamespace(
        mutation, catId, "it_ns", nsPath, "storage ns");
    ResourceId nsId = ns.getResourceId();
    String fullPath = String.join("/", nsPath) + "/it_ns";
    var fullListPath =  List.of("db_it", "schema_it", "it_ns");
    String kNsByPath = Keys.idxNsByPath(tenantId, catId.getId(), fullPath);
    String kNsById = Keys.idxNsById(tenantId, nsId.getId());
    String kOwner = Keys.idxNsOwnerById(tenantId, nsId.getId());
    assertTrue(ptr.get(kNsByPath).isPresent(), "namespace by-path (parents) index missing");
    assertTrue(ptr.get(kNsById).isPresent(), "namespace by-id index missing");
    assertTrue(ptr.get(kOwner).isPresent(), "namespace owner index missing");
    var nsRef = nameIndex.getNamespaceById(tenantId, nsId.getId()).orElseThrow();
    assertEquals(List.of("db_it","schema_it","it_ns"), nsRef.getPathList());
    assertEquals(cat.getDisplayName(), nsRef.getCatalog());
    assertEquals(nsId.getId(), nsRef.getResourceId().getId());
    var ownerRid = nameIndex.getNamespaceOwner(tenantId, nsId.getId()).orElseThrow();
    assertEquals(catId.getId(), ownerRid.getId());

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
    String kTblByName = Keys.idxTblByName(tenantId, catId.getId(), fullListPath, "it_tbl");
    String kTblByNs = Keys.idxTblByNamespaceLeaf(tenantId, nsId.getId(), "it_tbl");
    String kTblById = Keys.idxTblById(tenantId, tblId.getId());

    assertTrue(ptr.get(kTblByName).isPresent(), "table by-name index missing");
    assertTrue(ptr.get(kTblByNs).isPresent(), "table by-namespace index missing");
    assertTrue(ptr.get(kTblById).isPresent(), "table by-id index missing");
    var tblRefById = nameIndex.getTableById(tenantId, tblId.getId()).orElseThrow();
    assertEquals(cat.getDisplayName(), tblRefById.getCatalog());
    assertEquals(fullListPath, tblRefById.getPathList());
    assertEquals(tbl.getDisplayName(), tblRefById.getName());
    assertEquals(tblId.getId(), tblRefById.getResourceId().getId());

    String canonPtrKey = Keys.tblCanonicalPtr(tenantId, tblId.getId());
    String nsIdxPtrKey = Keys.tblPtr(tenantId, catId.getId(), nsId.getId(), tblId.getId());
    String tblBlobUri = Keys.tblBlob(tenantId, tblId.getId());

    Pointer tpCanon = ptr.get(canonPtrKey)
        .orElseThrow(() -> new AssertionError("canonical pointer missing"));
    Pointer tpNsIdx = ptr.get(nsIdxPtrKey)
        .orElseThrow(() -> new AssertionError("ns-index pointer missing"));
    assertEquals(tblBlobUri, tpCanon.getBlobUri());
    assertEquals(tblBlobUri, tpNsIdx.getBlobUri());
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
    String idxOldKey = Keys.idxTblByName(tenantId, catId.getId(), fullListPath, oldName);
    String idxNewKey = Keys.idxTblByName(tenantId, catId.getId(), fullListPath, newName);
    assertTrue(ptr.get(idxNewKey).isPresent(), "new name-index pointer must exist");
    assertTrue(ptr.get(idxOldKey).isEmpty(), "old name-index pointer must be removed");
    assertTrue(ptr.get(Keys.idxTblByNamespaceLeaf(tenantId, nsId.getId(), newName)).isPresent());
    assertTrue(ptr.get(Keys.idxTblById(tenantId, tblId.getId())).isPresent());

    TestSupport.deleteTable(mutation, nsId, tblId);
    assertTrue(ptr.get(idxNewKey).isEmpty(), "table by-name index should be deleted after table delete");
    assertTrue(ptr.get(canonPtrKey).isEmpty(), "canonical pointer should be deleted");
    assertTrue(ptr.get(nsIdxPtrKey).isEmpty(), "ns-index pointer should be deleted");
    assertTrue(blobs.head(tblBlobUri).isEmpty(), "table blob should be deleted");
    // Call again
    TestSupport.deleteTable(mutation, nsId, tblId);
    assertTrue(ptr.get(idxNewKey).isEmpty());
    assertTrue(ptr.get(canonPtrKey).isEmpty());
    assertTrue(ptr.get(nsIdxPtrKey).isEmpty());
    assertTrue(blobs.head(tblBlobUri).isEmpty());

    TestSupport.deleteNamespace(mutation, nsId, true);
    String nsByIdKey = Keys.idxNsById(tenantId, nsId.getId());
    String nsOwnerKey = Keys.idxNsOwnerById(tenantId, nsId.getId());
    String nsByPathKey = Keys.idxNsByPath(tenantId, catId.getId(), fullPath);
    String nsPtrKey = Keys.nsPtr(tenantId, catId.getId(), nsId.getId());
    String nsBlobUri = Keys.nsBlob(tenantId, catId.getId(), nsId.getId());
    assertTrue(ptr.get(nsByIdKey).isEmpty(), "ns by-id index should be deleted");
    assertTrue(ptr.get(nsOwnerKey).isEmpty(), "ns owner index should be deleted");
    assertTrue(ptr.get(nsByPathKey).isEmpty(), "ns by-path index should be deleted");
    assertTrue(ptr.get(nsPtrKey).isEmpty(), "namespace pointer should be deleted");
    assertTrue(blobs.head(nsBlobUri).isEmpty(), "namespace blob should be deleted");
    // Again
    assertTrue(ptr.get(nsByIdKey).isEmpty());
    assertTrue(ptr.get(nsOwnerKey).isEmpty());
    assertTrue(ptr.get(nsByPathKey).isEmpty());
    assertTrue(ptr.get(nsPtrKey).isEmpty());
    assertTrue(blobs.head(nsBlobUri).isEmpty());

    TestSupport.deleteCatalog(mutation, catId, true);
    String catByIdKey = Keys.idxCatById(tenantId, catId.getId());
    String catByNameKey = Keys.idxCatByName(tenantId, catName);
    String catPtrKey = Keys.catPtr(tenantId, catId.getId());
    String catBlobUri = Keys.catBlob(tenantId, catId.getId());
    assertTrue(ptr.get(catByIdKey).isEmpty(), "catalog by-id index should be deleted");
    assertTrue(ptr.get(catByNameKey).isEmpty(), "catalog by-name index should be deleted");
    assertTrue(ptr.get(catPtrKey).isEmpty(), "catalog pointer should be deleted");
    assertTrue(blobs.head(catBlobUri).isEmpty(), "catalog blob should be deleted");
    // Again
    assertTrue(ptr.get(catByIdKey).isEmpty());
    assertTrue(ptr.get(catByNameKey).isEmpty());
    assertTrue(ptr.get(catPtrKey).isEmpty());
    assertTrue(blobs.head(catBlobUri).isEmpty());
  }

  @Test
  void listTables_pagination_noRepeatsNoSkips() {
    String tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_pg_" + System.currentTimeMillis(), "pg");
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
        .setCatalog(cat.getDisplayName()).addAllPath(nsPath).build();

    String token = "";
    StringBuilder next = new StringBuilder();
    var p1 = nameIndex.listTablesByPrefix(tenantId, prefixRef, 2, token, next);
    String t1 = next.toString();

    next.setLength(0);
    var p2 = nameIndex.listTablesByPrefix(tenantId, prefixRef, 2, t1, next);
    String t2 = next.toString();

    next.setLength(0);
    var p3 = nameIndex.listTablesByPrefix(tenantId, prefixRef, 2, t2, next);
    String t3 = next.toString();

    var all = new java.util.LinkedHashSet<>(p1);
    all.addAll(p2); all.addAll(p3);
    assertEquals(5, all.size());
    assertTrue(t3.isEmpty(), "final page should clear nextToken");
  }

  @Test
  void encodedNames_preservePrefixScan() {
    String tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_enc_" + System.currentTimeMillis(), "enc");
    var nsPath = List.of("db it", "sch🧪", "Q4 Europe");
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns_enc", nsPath, "enc");

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

    var prefixRef = ai.floedb.metacat.common.rpc.NameRef.newBuilder()
        .setCatalog(cat.getDisplayName()).addAllPath(nsPath).build();

    StringBuilder next = new StringBuilder();
    var page = nameIndex.listTablesByPrefix(tenantId, prefixRef, 100, "", next);
    assertTrue(page.size() >= 2);
    var fq = String.join("/", cat.getDisplayName(), String.join("/", nsPath), "α");
    String key = Keys.idxTblByName(tenantId, cat.getResourceId().getId(), nsPath, "α");
    assertTrue(key.contains("/by-name/"), "hierarchy should be preserved in keyspace");
  }

  @Test
  void update_bumpsBothPointers() {
    var cat = TestSupport.createCatalog(mutation, "cat_ver_" + System.currentTimeMillis(), "ver");
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "ver");
    var tbl = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    var tid = tbl.getResourceId(); var tenant = tid.getTenantId();
    var canon = Keys.tblCanonicalPtr(tenant, tid.getId());
    var nsIdx = Keys.tblPtr(tenant, cat.getResourceId().getId(), ns.getResourceId().getId(), tid.getId());

    long vCanon = ptr.get(canon).orElseThrow().getVersion();
    long vIdx   = ptr.get(nsIdx).orElseThrow().getVersion();

    TestSupport.updateSchema(mutation, tid, "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"x\",\"type\":\"double\"}]}");

    assertTrue(ptr.get(canon).orElseThrow().getVersion() > vCanon);
    assertTrue(ptr.get(nsIdx).orElseThrow().getVersion() > vIdx);
  }

  @Test
  void etag_changesOnlyOnContentChange() {
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_etag_" + System.currentTimeMillis(), "etag");
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
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_del_" + System.currentTimeMillis(), "del");
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
    var tid = tbl.getResourceId();
    String canon = Keys.tblCanonicalPtr(tenantId, tid.getId());
    String blob = Keys.tblBlob(tenantId, tid.getId());

    assertTrue(blobs.delete(blob));
    TestSupport.deleteTable(mutation, ns.getResourceId(), tid);
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
    assertTrue(ptr.delete(canon2));
    TestSupport.deleteTable(mutation, ns.getResourceId(), tid2);
    assertTrue(blobs.head(blob2).isEmpty());
  }

  @Test
  void casContention_twoConcurrentUpdates() throws InterruptedException {
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_cas_" + System.currentTimeMillis(), "cas");
    var ns  = TestSupport.createNamespace(
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
    String tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_cnt_" + System.currentTimeMillis(), "cnt");
    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "cnt");

    var prefix = Keys.idxTblByNamePrefix(tenantId, cat.getResourceId().getId(), List.of("db","sch", "ns"));
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
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_idem_" + System.currentTimeMillis(), "idem");
    var ns  = TestSupport.createNamespace(
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

    var idxByName = Keys.idxTblByName(tenantId, cat.getResourceId().getId(), List.of("db","sch", "ns"), "t0");
    assertTrue(ptr.get(idxByName).isPresent(), "by-name index missing");

    assertEquals(resp1.getMeta().getPointerKey(), resp2.getMeta().getPointerKey());
    assertEquals(resp1.getMeta().getPointerVersion(), resp2.getMeta().getPointerVersion());
    assertEquals(resp1.getMeta().getEtag(), resp2.getMeta().getEtag());
  }

  @Test
  void createTable_idempotent_mismatchSameKey_conflict() {
    var cat = TestSupport.createCatalog(mutation, "cat_idem_conf_" + System.currentTimeMillis(), "idem");
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
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_idem_cc_" + System.currentTimeMillis(), "idem");
    var ns  = TestSupport.createNamespace(
        mutation,
        cat.getResourceId(),
        "ns",
        List.of("db","sch"),
        "idem");

    var spec = ai.floedb.metacat.catalog.rpc.TableSpec.newBuilder()
        .setCatalogId(cat.getResourceId())
        .setNamespaceId(ns.getResourceId())
        .setDisplayName("tcc")
        .setRootUri("s3://b/p")
        .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
        .build();

    var key = ai.floedb.metacat.catalog.rpc.IdempotencyKey.newBuilder().setKey("k-CC").build();
    var req = ai.floedb.metacat.catalog.rpc.CreateTableRequest.newBuilder().setSpec(spec).setIdempotency(key).build();

    var latch = new java.util.concurrent.CountDownLatch(1);
    var out1 = new java.util.concurrent.atomic.AtomicReference<ai.floedb.metacat.catalog.rpc.CreateTableResponse>();
    var out2 = new java.util.concurrent.atomic.AtomicReference<ai.floedb.metacat.catalog.rpc.CreateTableResponse>();
    var err  = new java.util.concurrent.atomic.AtomicReference<Throwable>();

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
    var blobUri     = Keys.tblBlob(tenantId, tid.getId());
    assertTrue(ptr.get(canonPtrKey).isPresent());
    assertTrue(blobs.head(blobUri).isPresent());
  }
}
