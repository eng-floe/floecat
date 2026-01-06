package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TableRepositoryTest {
  private final Clock clock = Clock.systemUTC();

  private final String account =
      TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();

  private CatalogRepository catalogRepo;
  private NamespaceRepository namespaceRepo;
  private TableRepository tableRepo;
  private SnapshotRepository snapshotRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
    namespaceRepo = new NamespaceRepository(ptr, blobs);
    tableRepo = new TableRepository(ptr, blobs);
    snapshotRepo = new SnapshotRepository(ptr, blobs);
  }

  private ResourceId createTable(String catalogName, String namespaceName, String tableName) {
    ResourceId catalogId;
    if (catalogRepo.getByName(account, catalogName).isPresent()) {
      catalogId = catalogRepo.getByName(account, catalogName).get().getResourceId();
    } else {
      catalogId =
          ResourceId.newBuilder()
              .setAccountId(account)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_CATALOG)
              .build();
      Catalog cat =
          Catalog.newBuilder()
              .setResourceId(catalogId)
              .setDisplayName(catalogName)
              .setDescription("description")
              .build();
      catalogRepo.create(cat);
    }

    ResourceId namespaceId;
    if (namespaceRepo.getByPath(account, catalogId.getId(), List.of(namespaceName)).isPresent()) {
      namespaceId =
          namespaceRepo
              .getByPath(account, catalogId.getId(), List.of(namespaceName))
              .get()
              .getResourceId();
    } else {
      namespaceId =
          ResourceId.newBuilder()
              .setAccountId(account)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_NAMESPACE)
              .build();
      var ns =
          Namespace.newBuilder()
              .setResourceId(namespaceId)
              .setDisplayName(namespaceName)
              .setDescription("description")
              .setCatalogId(catalogId)
              .build();
      namespaceRepo.create(ns);
    }

    return createTable(catalogId, namespaceId, tableName);
  }

  private ResourceId createTable(ResourceId catalogId, ResourceId namespaceId, String tableName) {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setUri("s3://upstream/tables/orders/")
            .build();
    var td =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName(tableName)
            .setDescription("description")
            .setUpstream(upstream)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tableRepo.create(td);

    return tableId;
  }

  @Test
  void tableRepoCreateTable() {
    var tableId = createTable("sales", "us", "line_item");
    var table = tableRepo.getById(tableId);
    assertTrue(table.isPresent());
    var foundTable = table.get();

    var catalogId = foundTable.getCatalogId();
    var namespaceId = foundTable.getNamespaceId();

    var pointerRow =
        ptr.get(
            Keys.tablePointerByName(account, catalogId.getId(), namespaceId.getId(), "line_item"));
    assertTrue(pointerRow.isPresent(), "by-namespace ROW pointer missing");

    var rowsUnderPfx =
        ptr.listPointersByPrefix(
            Keys.tablePointerByNamePrefix(account, catalogId.getId(), namespaceId.getId()),
            Integer.MAX_VALUE,
            null,
            null);
    assertTrue(
        rowsUnderPfx.stream()
            .anyMatch(
                r ->
                    r.getKey()
                        .equals(
                            Keys.tablePointerByName(
                                account, catalogId.getId(), namespaceId.getId(), "line_item"))),
        "prefix scan doesn't see the row key last written");

    String uri = pointerRow.get().getBlobUri();
    assertNotNull(uri);
    assertNotNull(blobs.head(uri).orElse(null), "blob header missing for by-namespace row");
    assertNotNull(blobs.get(uri), "blob bytes missing for by-namespace row");

    var snap =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42)
            .setIngestedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    snapshotRepo.create(snap);

    var fetched = tableRepo.getById(tableId).orElseThrow();
    assertEquals("line_item", fetched.getDisplayName());

    var list =
        tableRepo.list(
            account, catalogId.getId(), namespaceId.getId(), Integer.MAX_VALUE, null, null);
    assertEquals(1, list.size());

    var cur = snapshotRepo.getCurrentSnapshot(tableId).orElseThrow();
    assertEquals(42, cur.getSnapshotId());
  }

  @Test
  void tableRepoListCount() {
    List<ResourceId> tableIds = new ArrayList<ResourceId>();
    for (int i = 0; i < 100000; i++) {
      String paddedIndex = String.format("%10d", i); // Ensures lexicographic ordering on listing
      tableIds.add(createTable("sales", "eu", "line_item_" + paddedIndex));
    }
    var first = tableRepo.getById(tableIds.get(0));
    assertTrue(first.isPresent());
    Table foundFirst = first.get();
    ResourceId catalogId = foundFirst.getCatalogId();
    ResourceId namespaceId = foundFirst.getNamespaceId();

    assertEquals(100000, tableRepo.count(account, catalogId.getId(), namespaceId.getId()));

    int pages = 0;
    String token = "";
    List<Table> listedTables = new ArrayList<Table>();
    do {
      StringBuilder next = new StringBuilder();
      listedTables.addAll(
          tableRepo.list(account, catalogId.getId(), namespaceId.getId(), 10000, token, next));
      pages++;

      token = next.toString();
    } while (!token.isEmpty());

    assertEquals(10, pages);
    assertEquals(100000, listedTables.size());

    for (int i = 0; i < tableIds.size(); i++) {
      assertEquals(tableIds.get(i), listedTables.get(i).getResourceId());
    }
  }

  @Test
  @Timeout(30)
  void tableRepoConcurrentMutations() throws Exception {
    var catId = rid(account, ResourceKind.RK_CATALOG);
    var ns1Id = rid(account, ResourceKind.RK_NAMESPACE);
    var ns2Id = rid(account, ResourceKind.RK_NAMESPACE);
    var tblId = rid(account, ResourceKind.RK_TABLE);

    var cat = Catalog.newBuilder().setResourceId(catId).setDisplayName("sales").build();
    catalogRepo.create(cat);
    namespaceRepo.create(
        Namespace.newBuilder()
            .setResourceId(ns1Id)
            .setDisplayName("ns1")
            .setCatalogId(catId)
            .build());
    namespaceRepo.create(
        Namespace.newBuilder()
            .setResourceId(ns2Id)
            .setDisplayName("ns2")
            .setCatalogId(catId)
            .build());

    var upstream =
        UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).setUri("s3://b/p").build();

    var seed =
        Table.newBuilder()
            .setResourceId(tblId)
            .setDisplayName("seed")
            .setDescription("seed")
            .setUpstream(upstream)
            .setCatalogId(catId)
            .setNamespaceId(ns1Id)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tableRepo.create(seed);

    String canonKey = Keys.tablePointerById(account, tblId.getId());
    long v0 = ptr.get(canonKey).orElseThrow().getVersion();

    int WORKERS = 48;
    int OPS = 200;
    var pool = Executors.newFixedThreadPool(WORKERS);
    var start = new CountDownLatch(1);
    var unexpected = new ConcurrentLinkedQueue<Throwable>();
    var expectedCounts = new ConcurrentHashMap<String, LongAdder>();
    var seedDeleted = new AtomicBoolean(false);

    Runnable worker =
        () -> {
          try {
            start.await();
            var rnd = ThreadLocalRandom.current();
            for (int i = 0; i < OPS; i++) {
              int pick = rnd.nextInt(100);
              try {
                if (pick < 35) {
                  if (seedDeleted.get()) {
                    continue;
                  }

                  String col = "c" + rnd.nextInt(1000);
                  var curMeta = tableRepo.metaFor(tblId);
                  var curOpt = tableRepo.getById(tblId);
                  if (curOpt.isEmpty()) {
                    expectedCounts.computeIfAbsent("NotFound", k -> new LongAdder()).increment();
                    continue;
                  }
                  var cur = curOpt.get();
                  var updated =
                      cur.toBuilder()
                          .setSchemaJson(
                              "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                                  + "{\"name\":\""
                                  + col
                                  + "\",\"type\":\"double\"}]}")
                          .build();
                  boolean ok = tableRepo.update(updated, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else if (pick < 60) {
                  if (seedDeleted.get()) {
                    continue;
                  }

                  var curMeta = tableRepo.metaFor(tblId);
                  var curOpt = tableRepo.getById(tblId);
                  if (curOpt.isEmpty()) {
                    expectedCounts.computeIfAbsent("NotFound", k -> new LongAdder()).increment();
                    continue;
                  }
                  var cur = curOpt.get();
                  String target = "seed_" + rnd.nextInt(5);
                  var renamed = cur.toBuilder().setDisplayName(target).build();
                  boolean ok = tableRepo.update(renamed, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else if (pick < 85) {
                  if (seedDeleted.get()) {
                    continue;
                  }

                  var curMeta = tableRepo.metaFor(tblId);
                  var curOpt = tableRepo.getById(tblId);
                  if (curOpt.isEmpty()) {
                    expectedCounts.computeIfAbsent("NotFound", k -> new LongAdder()).increment();
                    continue;
                  }
                  var cur = curOpt.get();
                  var toNs = rnd.nextBoolean() ? ns1Id : ns2Id;
                  var updated = cur.toBuilder().setNamespaceId(toNs).build();
                  boolean ok = tableRepo.update(updated, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else {
                  if (seedDeleted.compareAndSet(false, true)) {
                    var curMeta = tableRepo.metaFor(tblId);
                    boolean ok =
                        tableRepo.deleteWithPrecondition(tblId, curMeta.getPointerVersion());
                    if (!ok) {
                      seedDeleted.set(false);
                      throw new BaseResourceRepository.PreconditionFailedException(
                          "version mismatch");
                    }
                  }
                }

              } catch (BaseResourceRepository.PreconditionFailedException
                  | BaseResourceRepository.NameConflictException
                  | BaseResourceRepository.NotFoundException
                  | BaseResourceRepository.AbortRetryableException e) {
                expectedCounts
                    .computeIfAbsent(e.getClass().getSimpleName(), k -> new LongAdder())
                    .increment();
              } catch (Throwable t) {
                unexpected.add(t);
              }
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            unexpected.add(ie);
          }
        };

    for (int i = 0; i < WORKERS; i++) pool.submit(worker);
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "workers timed out");

    if (!unexpected.isEmpty()) unexpected.peek().printStackTrace();
    assertTrue(unexpected.isEmpty(), "unexpected exceptions: " + unexpected.size());

    var p = ptr.get(canonKey);
    if (seedDeleted.get()) {
      assertTrue(
          p.isEmpty() || !tableRepo.getById(tblId).isPresent(),
          "deleted table should not be resolvable");
      assertDoesNotThrow(() -> tableRepo.metaForSafe(tblId));
    } else {
      long vN = p.orElseThrow().getVersion();
      assertTrue(vN >= v0, "pointer version should be >= initial");
      var cur = tableRepo.getById(tblId).orElseThrow();
      assertNotNull(cur.getNamespaceId());
      assertNotNull(cur.getCatalogId());
    }
  }

  private static ResourceId rid(String account, ResourceKind kind) {
    return ResourceId.newBuilder()
        .setAccountId(account)
        .setId(UUID.randomUUID().toString())
        .setKind(kind)
        .build();
  }
}
