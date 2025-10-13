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

    var nsPath = List.of("db_it", "schema_it");
    Namespace ns = TestSupport.createNamespace(mutation, catId, "it_ns", nsPath, "storage ns");
    ResourceId nsId = ns.getResourceId();

    String schemaV1 = """
        {"type":"struct","fields":[{"name":"id","type":"long"}]}
        """.trim();
    TableDescriptor tbl = TestSupport.createTable(
        mutation, catId, nsId, "it_tbl", "s3://bucket/prefix/it", schemaV1, "storage table");
    ResourceId tblId = tbl.getResourceId();

    String canonPtrKey = Keys.tblCanonicalPtr(tenantId, tblId.getId());
    String nsIdxPtrKey = Keys.tblPtr(tenantId, catId.getId(), nsId.getId(), tblId.getId());
    String tblBlobUri  = Keys.tblBlob(tenantId, tblId.getId());

    Pointer tpCanon = ptr.get(canonPtrKey).orElseThrow(() -> new AssertionError("canonical pointer missing"));
    Pointer tpNsIdx = ptr.get(nsIdxPtrKey).orElseThrow(() -> new AssertionError("ns-index pointer missing"));
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
    assertEquals(vBeforeIdempotent, tpCanonAfterIdem.getVersion(), "version must NOT bump on identical content");

    String oldName = tbl.getDisplayName();
    String newName = "it_tbl_renamed";
    TestSupport.renameTable(mutation, tblId, newName);

    String oldFq = String.join("/", catName, String.join("/", nsPath), oldName);
    String newFq = String.join("/", catName, String.join("/", nsPath), newName);

    String idxOldKey = Keys.idxTblByName(tenantId, oldFq);
    String idxNewKey = Keys.idxTblByName(tenantId, newFq);

    assertTrue(ptr.get(idxNewKey).isPresent(), "new name-index pointer must exist");
    assertTrue(ptr.get(idxOldKey).isEmpty(), "old name-index pointer must be removed");

    TestSupport.deleteTable(mutation, nsId, tblId);
    assertTrue(ptr.get(canonPtrKey).isEmpty(), "canonical pointer should be deleted");
    assertTrue(ptr.get(nsIdxPtrKey).isEmpty(), "ns-index pointer should be deleted");
    assertTrue(blobs.head(tblBlobUri).isEmpty(), "table blob should be deleted");

    TestSupport.deleteNamespace(mutation, nsId, true);
    String nsPtrKey = Keys.nsPtr(tenantId, catId.getId(), nsId.getId());
    String nsBlobUri = Keys.nsBlob(tenantId, catId.getId(), nsId.getId());
    assertTrue(ptr.get(nsPtrKey).isEmpty(), "namespace pointer should be deleted");
    assertTrue(blobs.head(nsBlobUri).isEmpty(), "namespace blob should be deleted");

    TestSupport.deleteCatalog(mutation, catId, true);
    String catPtrKey = Keys.catPtr(tenantId, catId.getId());
    String catBlobUri = Keys.catBlob(tenantId, catId.getId());
    assertTrue(ptr.get(catPtrKey).isEmpty(), "catalog pointer should be deleted");
    assertTrue(blobs.head(catBlobUri).isEmpty(), "catalog blob should be deleted");
  }

  @Test
  void listTables_pagination_noRepeatsNoSkips() {
    String tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_pg_" + System.currentTimeMillis(), "pg");
    var nsPath = List.of("db","sch");
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns_pg", nsPath, "pg");
    for (int i=0;i<5;i++) TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t"+i, "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");

    var prefixRef = ai.floedb.metacat.common.rpc.NameRef.newBuilder()
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
    var nsPath = List.of("db it", "sch🧪", "Q4 Europe"); // spaces + unicode
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns_enc", nsPath, "enc");

    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "α", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "β", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");

    var prefixRef = ai.floedb.metacat.common.rpc.NameRef.newBuilder()
        .setCatalog(cat.getDisplayName()).addAllPath(nsPath).build();

    StringBuilder next = new StringBuilder();
    var page = nameIndex.listTablesByPrefix(tenantId, prefixRef, 100, "", next);
    assertTrue(page.size() >= 2);
    var fq = String.join("/", cat.getDisplayName(), String.join("/", nsPath), "α");
    String key = ai.floedb.metacat.service.repo.util.Keys.idxTblByName(tenantId, fq);
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
    var tbl = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    var tid = tbl.getResourceId();
    String blob = Keys.tblBlob(tenantId, tid.getId());

    var e1 = blobs.head(blob).orElseThrow().getEtag();
    TestSupport.updateSchema(mutation, tid, "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"); // same
    var e2 = blobs.head(blob).orElseThrow().getEtag();
    assertEquals(e1, e2);

    TestSupport.updateSchema(mutation, tid, "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"y\",\"type\":\"double\"}]}");
    var e3 = blobs.head(blob).orElseThrow().getEtag();
    assertNotEquals(e2, e3);
  }

  @Test
  void delete_skewTolerance() {
    var tenantId = TestSupport.seedTenantId(directory, "sales");
    var cat = TestSupport.createCatalog(mutation, "cat_del_" + System.currentTimeMillis(), "del");
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "del");
    var tbl = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    var tid = tbl.getResourceId();
    String canon = Keys.tblCanonicalPtr(tenantId, tid.getId());
    String blob = Keys.tblBlob(tenantId, tid.getId());

    assertTrue(blobs.delete(blob));
    TestSupport.deleteTable(mutation, ns.getResourceId(), tid);
    assertTrue(ptr.get(canon).isEmpty());

    var tbl2 = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t2", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
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
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "cas");
    var tbl = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "t", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
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
    var ns  = TestSupport.createNamespace(mutation, cat.getResourceId(), "ns", List.of("db","sch"), "cnt");

    var prefix = Keys.idxTblByName(tenantId, String.join("/", cat.getDisplayName(), "db","sch",""));
    int before = ptr.countByPrefix(prefix);

    var tA = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "a", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");
    var tB = TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(), "b", "s3://b/p", "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}", "d");

    int mid = ptr.countByPrefix(prefix);
    assertEquals(before + 2, mid);

    TestSupport.deleteTable(mutation, ns.getResourceId(), tA.getResourceId());
    int after = ptr.countByPrefix(prefix);
    assertEquals(before + 1, after);
  }
}
