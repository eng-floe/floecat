/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class BackendStorageIT {

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private final Clock clock = Clock.systemUTC();

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void storageInvariants() {
    String catName = "it_storage_cat_" + clock.millis();
    Catalog cat = TestSupport.createCatalog(catalog, catName, "storage cat");

    ResourceId catId = cat.getResourceId();
    String keyCatByName =
        Keys.catalogPointerByName(cat.getResourceId().getAccountId(), cat.getDisplayName());
    String keyCatById = Keys.catalogPointerById(cat.getResourceId().getAccountId(), catId.getId());
    assertTrue(ptr.get(keyCatByName).isPresent());
    assertTrue(ptr.get(keyCatById).isPresent());
    var catLookup =
        directory.lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(catId).build());
    assertEquals(cat.getDisplayName(), catLookup.getDisplayName());

    var nsPath = List.of("db_it", "schema_it");
    Namespace ns = TestSupport.createNamespace(namespace, catId, "it_ns", nsPath, "storage ns");
    ResourceId nsId = ns.getResourceId();
    var fullPath = List.of("db_it", "schema_it", "it_ns");
    String keyNsByPath =
        Keys.namespacePointerByPath(cat.getResourceId().getAccountId(), catId.getId(), fullPath);
    String keyNsPtr = Keys.namespacePointerById(cat.getResourceId().getAccountId(), nsId.getId());
    assertTrue(ptr.get(keyNsByPath).isPresent(), "namespace by-path pointer missing");
    var nsPtr = ptr.get(keyNsPtr).orElseThrow();
    assertTrue(blobs.head(nsPtr.getBlobUri()).isPresent(), "namespace blob missing");
    assertTrue(ptr.get(keyNsByPath).isPresent(), "namespace by-path (parents) index missing");

    var nsLookup =
        directory.lookupNamespace(LookupNamespaceRequest.newBuilder().setResourceId(nsId).build());
    assertEquals(cat.getDisplayName(), nsLookup.getRef().getCatalog());
    assertEquals(List.of("db_it", "schema_it"), nsLookup.getRef().getPathList());
    assertEquals("it_ns", nsLookup.getRef().getName());

    String schemaV1 =
        """
        {"type":"struct","fields":[{"name":"id","type":"long"}]}
        """
            .trim();
    Table tbl =
        TestSupport.createTable(
            table, catId, nsId, "it_tbl", "s3://bucket/prefix/it", schemaV1, "storage table");
    ResourceId tblId = tbl.getResourceId();
    String keyTblByName =
        Keys.tablePointerByName(
            cat.getResourceId().getAccountId(), catId.getId(), nsId.getId(), "it_tbl");
    String keyTblCanon = Keys.tablePointerById(cat.getResourceId().getAccountId(), tblId.getId());

    assertTrue(ptr.get(keyTblByName).isPresent(), "table by-name pointer missing");
    var tblPtr = ptr.get(keyTblCanon).orElseThrow();
    assertTrue(blobs.head(tblPtr.getBlobUri()).isPresent(), "table blob header missing");

    var tblLookup =
        directory.lookupTable(LookupTableRequest.newBuilder().setResourceId(tblId).build());
    assertEquals(cat.getDisplayName(), tblLookup.getName().getCatalog());
    assertEquals(List.of("db_it", "schema_it", "it_ns"), tblLookup.getName().getPathList());
    assertEquals("it_tbl", tblLookup.getName().getName());

    String canonPtrKey = Keys.tablePointerById(tblId.getAccountId(), tblId.getId());
    Pointer tpCanon =
        ptr.get(canonPtrKey).orElseThrow(() -> new AssertionError("canonical pointer missing"));
    assertTrue(blobs.head(tpCanon.getBlobUri()).isPresent(), "table blob header missing");
    String blobBefore = tpCanon.getBlobUri();

    long verCanonBefore = tpCanon.getVersion();
    String schemaV2 =
        """
        {"type":"struct","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}
        """
            .trim();
    TestSupport.updateSchema(table, tblId, schemaV2);

    Pointer tpCanonAfter = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(tpCanonAfter.getVersion() > verCanonBefore, "version must bump on content change");
    assertNotEquals(
        blobBefore, tpCanonAfter.getBlobUri(), "blob URI should change when content changes");

    long verBeforeIdempotent = tpCanonAfter.getVersion();
    TestSupport.updateSchema(table, tblId, schemaV2);
    Pointer tpCanonAfterIdem = ptr.get(canonPtrKey).orElseThrow();
    assertEquals(
        tpCanonAfterIdem.getVersion(),
        verBeforeIdempotent,
        "version must not bump on identical content");
    assertEquals(
        tpCanonAfter.getBlobUri(),
        tpCanonAfterIdem.getBlobUri(),
        "blob URI should remain stable on identical content");

    String oldName = tbl.getDisplayName();
    String newName = "it_tbl_renamed";
    TestSupport.renameTable(table, tblId, newName);
    String idxOldKey =
        Keys.tablePointerByName(catId.getAccountId(), catId.getId(), nsId.getId(), oldName);
    String idxNewKey =
        Keys.tablePointerByName(catId.getAccountId(), catId.getId(), nsId.getId(), newName);
    assertTrue(ptr.get(idxNewKey).isPresent(), "new by-name pointer must exist");
    assertTrue(ptr.get(idxOldKey).isEmpty(), "old by-name pointer must be removed");

    TestSupport.deleteTable(table, nsId, tblId);
    assertTrue(ptr.get(keyTblCanon).isEmpty());
    assertTrue(ptr.get(keyTblByName).isEmpty());

    TestSupport.deleteNamespace(namespace, nsId, true);
    assertTrue(ptr.get(keyNsByPath).isEmpty(), "ns by-path pointer should be deleted");
    assertTrue(ptr.get(keyNsPtr).isEmpty(), "ns canonical pointer should be deleted");

    var schemaNsId =
        TestSupport.resolveNamespaceId(
            directory, cat.getDisplayName(), List.of("db_it", "schema_it"));
    TestSupport.deleteNamespace(namespace, schemaNsId, true);

    var dbNsId = TestSupport.resolveNamespaceId(directory, cat.getDisplayName(), List.of("db_it"));
    TestSupport.deleteNamespace(namespace, dbNsId, true);

    TestSupport.deleteCatalog(catalog, catId, true);
    String catByIdKey = Keys.catalogPointerById(catId.getAccountId(), catId.getId());
    String catByNameKey = Keys.catalogPointerByName(catId.getAccountId(), catName);
    assertTrue(ptr.get(catByIdKey).isEmpty());
    assertTrue(ptr.get(catByNameKey).isEmpty());
  }

  @Test
  void listTablesPagination() {
    var catName = "cat_pg_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "pag");

    var nsPath = List.of("db", "sch");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns_pg", nsPath, "pg");
    for (int i = 0; i < 5; i++) {
      TestSupport.createTable(
          table,
          cat.getResourceId(),
          ns.getResourceId(),
          "t" + i,
          "s3://b/p",
          "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
          "d");
    }

    var prefixRef =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(nsPath)
            .addPath("ns_pg")
            .build();

    int pageSize = 2;
    String token = "";
    var p1 =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefixRef)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(token))
                .build());
    String t1 = p1.getPage().getNextPageToken();

    var p2 =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefixRef)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(t1))
                .build());
    String t2 = p2.getPage().getNextPageToken();

    var p3 =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefixRef)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(t2))
                .build());

    var all = new LinkedHashSet<String>();
    p1.getTablesList().forEach(e -> all.add(e.getName().getName()));
    p2.getTablesList().forEach(e -> all.add(e.getName().getName()));
    p3.getTablesList().forEach(e -> all.add(e.getName().getName()));
    assertEquals(5, all.size());
    String t3 = p3.getPage().getNextPageToken();
    assertTrue(t3.isEmpty(), "final page should clear nextToken");
  }

  @Test
  void encodedNamesPreservePrefixScan() {
    var catName = "cat_enc_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "enc");

    var nsPath = List.of("db it", "sch🧪", "Q4 Europe");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "ns_enc", nsPath, "enc");

    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "α",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "β",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");

    var prefixRef =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(nsPath)
            .addPath("ns_enc")
            .build();

    var page =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefixRef)
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());
    assertTrue(page.getTablesCount() >= 2);

    String key =
        Keys.tablePointerByName(
            cat.getResourceId().getAccountId(),
            cat.getResourceId().getId(),
            ns.getResourceId().getId(),
            "α");
    assertTrue(key.contains("/by-name/"), "hierarchy should be preserved in keyspace");
  }

  @Test
  void updateBumpsBothPointers() {
    var catName = "cat_ver_" + System.currentTimeMillis() + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "ver");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "ver");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");
    var tid = tbl.getResourceId();
    var account = tid.getAccountId();
    String canon = Keys.tablePointerById(account, tid.getId());
    long verCanon = ptr.get(canon).orElseThrow().getVersion();

    TestSupport.updateSchema(
        table,
        tid,
        "{\"type\":\"struct\",\"fields\""
            + ":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"x\",\"type\":\"double\"}]}");

    assertTrue(ptr.get(canon).orElseThrow().getVersion() > verCanon);
  }

  @Test
  void etagChangesOnlyOnContentChange() {
    var catName = "cat_etag_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "etag");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "etag");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");
    var tid = tbl.getResourceId();
    var blob =
        ptr.get(Keys.tablePointerById(tid.getAccountId(), tid.getId())).orElseThrow().getBlobUri();
    var e1 = blobs.head(blob).orElseThrow().getEtag();
    TestSupport.updateSchema(
        table, tid, "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");
    var blob2 =
        ptr.get(Keys.tablePointerById(tid.getAccountId(), tid.getId())).orElseThrow().getBlobUri();
    var e2 = blobs.head(blob2).orElseThrow().getEtag();
    assertEquals(e1, e2);

    TestSupport.updateSchema(
        table,
        tid,
        "{\"type\":\"struct\",\"fields\""
            + ":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"y\",\"type\":\"double\"}]}");

    var blob3 =
        ptr.get(Keys.tablePointerById(tid.getAccountId(), tid.getId())).orElseThrow().getBlobUri();
    var e3 = blobs.head(blob3).orElseThrow().getEtag();
    assertNotEquals(e2, e3);
  }

  @Test
  void deleteSkewTolerance() throws Exception {
    var catName = "cat_del_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "del");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "del");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");

    Keys.tablePointerById(tbl.getResourceId().getAccountId(), tbl.getResourceId().getId());

    var blobUri =
        ptr.get(
                Keys.tablePointerById(
                    tbl.getResourceId().getAccountId(), tbl.getResourceId().getId()))
            .orElseThrow()
            .getBlobUri();
    assertTrue(blobs.delete(blobUri));
    table.deleteTable(DeleteTableRequest.newBuilder().setTableId(tbl.getResourceId()).build());

    var tbl2 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t2",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");
    var tid2 = tbl2.getResourceId();
    String canon2 = Keys.tablePointerById(cat.getResourceId().getAccountId(), tid2.getId());

    // Simulate canonical ptr missing
    assertTrue(ptr.delete(canon2));
    table.deleteTable(DeleteTableRequest.newBuilder().setTableId(tid2).build());
  }

  @Test
  void casContentionTwoConcurrentUpdates() throws InterruptedException {
    var catName = "cat_cas_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "cas");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "cas");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "t",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");
    var tid = tbl.getResourceId();

    var schemaA =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}"
            + ",{\"name\":\"a\",\"type\":\"double\"}]}";
    var schemaB =
        "{\"type\":\"struct\",\"fields\""
            + ":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"double\"}]}";

    var latch = new CountDownLatch(1);
    var errs = new CopyOnWriteArrayList<Throwable>();
    Runnable r1 =
        () -> {
          try {
            latch.await();
            TestSupport.updateSchema(table, tid, schemaA);
          } catch (Throwable t) {
            errs.add(t);
          }
        };

    Runnable r2 =
        () -> {
          try {
            latch.await();
            TestSupport.updateSchema(table, tid, schemaB);
          } catch (Throwable t) {
            errs.add(t);
          }
        };

    String canon = Keys.tablePointerById(cat.getResourceId().getAccountId(), tid.getId());
    long v0 = ptr.get(canon).orElseThrow().getVersion();

    var t1 = new Thread(r1);
    var t2 = new Thread(r2);
    t1.start();
    t2.start();
    latch.countDown();
    t1.join();
    t2.join();

    assertTrue(errs.size() <= 1, "at most one writer should fail");
    if (!errs.isEmpty()) {
      var sre = (io.grpc.StatusRuntimeException) errs.get(0);
      assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, sre.getStatus().getCode());
    }

    long v2 = ptr.get(canon).orElseThrow().getVersion();
    assertTrue(v2 == v0 + 1 || v2 == v0 + 2, "one or two successful bumps expected");
  }

  @Test
  void countByPrefixMatchesInsertsDeletes() {
    var catName = "cat_cnt_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "cnt");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "cnt");

    var prefix =
        Keys.tablePointerByNamePrefix(
            cat.getResourceId().getAccountId(),
            cat.getResourceId().getId(),
            ns.getResourceId().getId());
    int before = ptr.countByPrefix(prefix);

    var tableA =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "a",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "d");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "b",
        "s3://b/p",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
        "d");

    int mid = ptr.countByPrefix(prefix);
    assertEquals(before + 2, mid);

    TestSupport.deleteTable(table, ns.getResourceId(), tableA.getResourceId());
    int after = ptr.countByPrefix(prefix);
    assertEquals(before + 1, after);
  }

  @Test
  void createTableIdempotent() {
    var catName = "cat_idem_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "idem");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "idem");

    var upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_DELTA)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .setUri("s3://b/p")
            .build();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setUpstream(upstream)
            .setDisplayName("t0")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .setDescription("desc")
            .build();

    var key = IdempotencyKey.newBuilder().setKey("k-123").build();

    var req = CreateTableRequest.newBuilder().setSpec(spec).setIdempotency(key).build();

    var resp1 = table.createTable(req);
    var resp2 = table.createTable(req);

    var idemKey = Keys.idempotencyKey(cat.getResourceId().getAccountId(), "CreateTable", "k-123");
    var idemPtr = ptr.get(idemKey);
    assertTrue(idemPtr.isPresent(), "idempotency pointer missing");

    var t1 = resp1.getTable().getResourceId();
    var t2 = resp2.getTable().getResourceId();
    assertEquals(t1.getId(), t2.getId(), "idempotent create must return the same table id");

    var canonPtrKey = Keys.tablePointerById(cat.getResourceId().getAccountId(), t1.getId());
    var ptr1 = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(blobs.head(ptr1.getBlobUri()).isPresent(), "blob missing");

    var idxByName =
        Keys.tablePointerByName(
            cat.getResourceId().getAccountId(),
            cat.getResourceId().getId(),
            ns.getResourceId().getId(),
            "t0");
    assertTrue(ptr.get(idxByName).isPresent(), "by-name pointer missing");

    assertEquals(resp1.getMeta().getPointerKey(), resp2.getMeta().getPointerKey());
    assertEquals(resp1.getMeta().getPointerVersion(), resp2.getMeta().getPointerVersion());
    assertEquals(resp1.getMeta().getEtag(), resp2.getMeta().getEtag());
  }

  @Test
  void createTableIdempotentMismatch() {
    var catName = "cat_idem_conf_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "idem");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "idem");
    var idem = IdempotencyKey.newBuilder().setKey("k-XYZ").build();

    var upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_DELTA)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .setUri("s3://b/p")
            .build();

    var specA =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setDisplayName("tA")
            .setUpstream(upstream)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .build();

    var specB =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setDisplayName("tB")
            .setUpstream(upstream)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .build();

    var reqA = CreateTableRequest.newBuilder().setSpec(specA).setIdempotency(idem).build();
    var reqB = CreateTableRequest.newBuilder().setSpec(specB).setIdempotency(idem).build();

    table.createTable(reqA);

    var ex = assertThrows(io.grpc.StatusRuntimeException.class, () -> table.createTable(reqB));
    assertEquals(io.grpc.Status.Code.ABORTED, ex.getStatus().getCode());
  }

  @Test
  void createTableIdempotentConcurrent() throws InterruptedException {
    var catName = "cat_idem_cc_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "idem");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db", "sch"), "idem");

    var upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_DELTA)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL)
            .setUri("s3://b/p")
            .build();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(cat.getResourceId())
            .setNamespaceId(ns.getResourceId())
            .setDisplayName("tcc")
            .setUpstream(upstream)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .build();

    var key = IdempotencyKey.newBuilder().setKey("k-CC").build();
    var req = CreateTableRequest.newBuilder().setSpec(spec).setIdempotency(key).build();

    var latch = new CountDownLatch(1);
    var out1 = new AtomicReference<CreateTableResponse>();
    var out2 = new AtomicReference<CreateTableResponse>();
    var err = new AtomicReference<Throwable>();

    Runnable r =
        () -> {
          try {
            latch.await();
            out1.compareAndSet(null, table.createTable(req));
          } catch (Throwable t) {
            err.set(t);
          }
        };
    Runnable s =
        () -> {
          try {
            latch.await();
            out2.compareAndSet(null, table.createTable(req));
          } catch (Throwable t) {
            err.set(t);
          }
        };

    var t1 = new Thread(r);
    var t2 = new Thread(s);
    t1.start();
    t2.start();
    latch.countDown();
    t1.join();
    t2.join();

    assertNull(err.get(), "unexpected error in concurrent writers");

    var a = out1.get();
    var b = out2.get();
    assertNotNull(a);
    assertNotNull(b);
    assertEquals(
        a.getTable().getResourceId().getId(),
        b.getTable().getResourceId().getId(),
        "should be same table id");

    var tid = a.getTable().getResourceId();
    var canonPtrKey = Keys.tablePointerById(cat.getResourceId().getAccountId(), tid.getId());
    var ptr1 = ptr.get(canonPtrKey).orElseThrow();
    assertTrue(blobs.head(ptr1.getBlobUri()).isPresent());
  }
}
