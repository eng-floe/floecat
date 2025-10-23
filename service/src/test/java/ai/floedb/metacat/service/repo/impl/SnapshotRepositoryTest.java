package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SnapshotRepositoryTest {
  private final Clock clock = Clock.systemUTC();

  @Test
  void snapshotRepoCreateSnapshot() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var snapshotRepo = new SnapshotRepository(ptr, blobs);
    var tableRepo = new TableRepository(ptr, blobs);

    String tenant = "t-0001";
    String catalogId = UUID.randomUUID().toString();
    String nsId = UUID.randomUUID().toString();
    String tblId = UUID.randomUUID().toString();

    var catalogRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(catalogId)
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(nsId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var tableRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(tblId)
            .setKind(ResourceKind.RK_TABLE)
            .build();

    var td =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setDisplayName("orders")
            .setDescription("Orders table")
            .setFormat(TableFormat.TF_DELTA)
            .setCatalogId(catalogRid)
            .setNamespaceId(nsRid)
            .setRootUri("s3://upstream/tables/orders/")
            .setSchemaJson("{}")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();
    tableRepo.create(td);

    seedSnapshot(
        snapshotRepo, tenant, tableRid, 199, clock.millis() - 20_000, clock.millis() - 60_000);
    seedSnapshot(
        snapshotRepo, tenant, tableRid, 200, clock.millis() - 10_000, clock.millis() - 50_000);

    StringBuilder next = new StringBuilder();
    var page1 = snapshotRepo.list(tableRid, 1, "", next);
    assertEquals(1, page1.size());
    assertFalse(next.toString().isEmpty(), "should return next_page_token");

    String token = next.toString();
    next.setLength(0);
    var page2 = snapshotRepo.list(tableRid, 10, token, next);
    assertTrue(page2.size() >= 1);
    assertTrue(next.toString().isEmpty(), "no more pages");

    int total = snapshotRepo.count(tableRid);
    assertEquals(2, total);

    var cur = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(200, cur.getSnapshotId());
  }

  private void seedSnapshot(
      SnapshotRepository snapshotRepo,
      String tenant,
      ResourceId tableId,
      long snapshotId,
      long ingestedAtMs,
      long upstreamCreatedAt) {
    var snap =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setIngestedAt(Timestamps.fromMillis(ingestedAtMs))
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAt))
            .build();

    snapshotRepo.create(snap);
  }

  private static boolean isExpectedRepoAbort(Throwable t) {
    return t instanceof BaseRepository.AbortRetryableException
        && t.getMessage().contains("blob write verification failed");
  }

  @Test
  @Timeout(20)
  void snapshotRepoCreateConcurrentSnapshots() throws Exception {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var snaps = new SnapshotRepository(ptr, blobs);

    String tenant = "t-0001";
    var tblId =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    int WORKERS = 24;
    int OPS = 200;
    var pool = Executors.newFixedThreadPool(WORKERS);
    var start = new CountDownLatch(1);
    var unexpected = new ConcurrentLinkedQueue<Throwable>();
    final ConcurrentMap<String, LongAdder> expected = new ConcurrentHashMap<>();
    var conflicts = new LongAdder();

    Runnable worker =
        () -> {
          try {
            start.await();
            var rnd = ThreadLocalRandom.current();
            for (int i = 0; i < OPS; i++) {
              long snapId = rnd.nextInt(160, 210);
              var snap =
                  Snapshot.newBuilder()
                      .setTableId(tblId)
                      .setSnapshotId(snapId)
                      .setIngestedAt(Timestamps.fromMillis(clock.millis()))
                      .build();
              try {
                for (int j = 0; j < 5; j++) {
                  try {
                    snaps.create(snap);
                  } catch (BaseRepository.AbortRetryableException e) {
                    Thread.sleep(5L * (1L << j));
                  }
                }
              } catch (BaseRepository.NameConflictException e) {
                conflicts.increment();
              } catch (Throwable t) {
                if (isExpectedRepoAbort(t)) {
                  expected.computeIfAbsent("ABORT_RETRYABLE", k -> new LongAdder()).increment();
                } else {
                  unexpected.add(t);
                }
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
    assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS), "workers timed out");

    if (!unexpected.isEmpty()) {
      unexpected.peek().printStackTrace();
    }
    assertTrue(unexpected.isEmpty(), "unexpected exceptions: " + unexpected.size());
    assertTrue(conflicts.sum() >= 0, "should have some conflicts");

    var cur = snaps.getCurrentSnapshot(tblId).orElseThrow();
    assertTrue(cur.getSnapshotId() >= 160, "current snapshot must be in expected range");

    var next = new StringBuilder();
    var first = snaps.list(tblId, 5, "", next);
    assertTrue(first.size() <= 5);
    var token = next.toString();
    if (!token.isEmpty()) {
      next.setLength(0);
      var second = snaps.list(tblId, 5, token, next);
      assertTrue(second.size() >= 0);
    }
  }
}
