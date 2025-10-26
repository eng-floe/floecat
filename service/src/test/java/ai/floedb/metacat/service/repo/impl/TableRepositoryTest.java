package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.util.TestSupport;
import ai.floedb.metacat.storage.InMemoryBlobStore;
import ai.floedb.metacat.storage.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TableRepositoryTest {
  private final Clock clock = Clock.systemUTC();

  @Test
  void tableRepoCreateTable() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    final var snapshotRepo = new SnapshotRepository(ptr, blobs);
    final var tableRepo = new TableRepository(ptr, blobs);
    final var repo = new NamespaceRepository(ptr, blobs);
    final var catRepo = new CatalogRepository(ptr, blobs);

    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();
    var catRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();

    Catalog cat = Catalog.newBuilder().setResourceId(catRid).setDisplayName("sales").build();
    catRepo.create(cat);

    var nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var ns =
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName("core")
            .setDescription("Core namespace")
            .setCatalogId(catRid)
            .build();
    repo.create(ns);

    var tableRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    var td =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setDisplayName("orders")
            .setDescription("Orders table")
            .setFormat(TableFormat.TF_ICEBERG)
            .setCatalogId(catRid)
            .setNamespaceId(nsRid)
            .setRootUri("s3://upstream/tables/orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tableRepo.create(td);

    String nsKeyRow = Keys.tablePointerByName(tenant, catRid.getId(), nsRid.getId(), "orders");
    var pointerRow = ptr.get(nsKeyRow);
    assertTrue(pointerRow.isPresent(), "by-namespace ROW pointer missing");

    var nsKeyPfx = Keys.tablePointerByNamePrefix(tenant, catRid.getId(), nsRid.getId());
    var rowsUnderPfx = ptr.listPointersByPrefix(nsKeyPfx, 100, "", new StringBuilder());
    assertTrue(
        rowsUnderPfx.stream().anyMatch(r -> r.key().equals(nsKeyRow)),
        "prefix scan doesn't see the row key you just wrote");

    String uri = pointerRow.get().getBlobUri();
    assertNotNull(uri);
    assertNotNull(blobs.head(uri).orElse(null), "blob header missing for by-namespace row");
    assertNotNull(blobs.get(uri), "blob bytes missing for by-namespace row");

    var snap =
        Snapshot.newBuilder()
            .setTableId(tableRid)
            .setSnapshotId(42)
            .setIngestedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    snapshotRepo.create(snap);

    var fetched = tableRepo.getById(tableRid).orElseThrow();
    assertEquals("orders", fetched.getDisplayName());

    var list = tableRepo.list(tenant, catRid.getId(), nsRid.getId(), 50, "", new StringBuilder());
    assertEquals(1, list.size());

    var cur = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(42, cur.getSnapshotId());
  }

  @Test
  @Timeout(30)
  void tableRepoConcurrentMutations() throws Exception {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var cats = new CatalogRepository(ptr, blobs);
    var nss = new NamespaceRepository(ptr, blobs);
    var tbls = new TableRepository(ptr, blobs);

    String tenant = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT).getId();
    var catId = rid(tenant, ResourceKind.RK_CATALOG);
    var ns1Id = rid(tenant, ResourceKind.RK_NAMESPACE);
    var ns2Id = rid(tenant, ResourceKind.RK_NAMESPACE);
    var tblId = rid(tenant, ResourceKind.RK_TABLE);

    var cat = Catalog.newBuilder().setResourceId(catId).setDisplayName("sales").build();
    cats.create(cat);
    nss.create(
        Namespace.newBuilder()
            .setResourceId(ns1Id)
            .setDisplayName("ns1")
            .setCatalogId(catId)
            .build());
    nss.create(
        Namespace.newBuilder()
            .setResourceId(ns2Id)
            .setDisplayName("ns2")
            .setCatalogId(catId)
            .build());

    var seed =
        Table.newBuilder()
            .setResourceId(tblId)
            .setDisplayName("seed")
            .setDescription("seed")
            .setFormat(TableFormat.TF_DELTA)
            .setCatalogId(catId)
            .setNamespaceId(ns1Id)
            .setRootUri("s3://b/p")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tbls.create(seed);

    String canonKey = Keys.tablePointerById(tenant, tblId.getId());
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
                  if (seedDeleted.get()) continue;
                  String col = "c" + rnd.nextInt(1000);
                  var curMeta = tbls.metaFor(tblId);
                  var cur = tbls.getById(tblId).orElseThrow();
                  var updated =
                      cur.toBuilder()
                          .setSchemaJson(
                              "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                                  + "{\"name\":\""
                                  + col
                                  + "\",\"type\":\"double\"}]}")
                          .build();
                  boolean ok = tbls.update(updated, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else if (pick < 60) {
                  if (seedDeleted.get()) continue;
                  var curMeta = tbls.metaFor(tblId);
                  var cur = tbls.getById(tblId).orElseThrow();
                  String target = "seed_" + rnd.nextInt(5);
                  var renamed = cur.toBuilder().setDisplayName(target).build();
                  boolean ok = tbls.update(renamed, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else if (pick < 85) {
                  if (seedDeleted.get()) continue;
                  var curMeta = tbls.metaFor(tblId);
                  var cur = tbls.getById(tblId).orElseThrow();
                  var toNs = rnd.nextBoolean() ? ns1Id : ns2Id;
                  var updated = cur.toBuilder().setNamespaceId(toNs).build();
                  boolean ok = tbls.update(updated, curMeta.getPointerVersion());
                  if (!ok) {
                    throw new BaseResourceRepository.PreconditionFailedException(
                        "version mismatch");
                  }

                } else {
                  if (seedDeleted.compareAndSet(false, true)) {
                    var curMeta = tbls.metaFor(tblId);
                    boolean ok = tbls.deleteWithPrecondition(tblId, curMeta.getPointerVersion());
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
          p.isEmpty() || !tbls.getById(tblId).isPresent(),
          "deleted table should not be resolvable");
      assertDoesNotThrow(() -> tbls.metaForSafe(tblId));
    } else {
      long vN = p.orElseThrow().getVersion();
      assertTrue(vN >= v0, "pointer version should be >= initial");
      var cur = tbls.getById(tblId).orElseThrow();
      assertNotNull(cur.getNamespaceId());
      assertNotNull(cur.getCatalogId());
    }
  }

  private static ResourceId rid(String tenant, ResourceKind kind) {
    return ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(kind)
        .build();
  }
}
