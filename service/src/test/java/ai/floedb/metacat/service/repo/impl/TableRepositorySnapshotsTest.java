package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import com.google.protobuf.util.Timestamps;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class TableRepositorySnapshotsTest {

  private final Clock clock = Clock.systemUTC();

  @Test
  void list_count_and_currentSnapshot_fromSeeded() {
    var ptr = new InMemoryPointerStore();
    var blobs= new InMemoryBlobStore();
    var snapshotRepo = new SnapshotRepository(ptr, blobs);
    var tableRepo = new TableRepository(ptr, blobs);

    String tenant = "t-0001";
    String catalogId = UUID.randomUUID().toString();
    String nsId = UUID.randomUUID().toString();
    String tblId = UUID.randomUUID().toString();

    var catalogRid = ResourceId.newBuilder().setTenantId(tenant).setId(catalogId).setKind(ResourceKind.RK_CATALOG).build();
    var nsRid = ResourceId.newBuilder().setTenantId(tenant).setId(nsId).setKind(ResourceKind.RK_NAMESPACE).build();
    var tableRid = ResourceId.newBuilder().setTenantId(tenant).setId(tblId).setKind(ResourceKind.RK_TABLE).build();

    var td = Table.newBuilder()
        .setResourceId(tableRid)
        .setDisplayName("orders")
        .setDescription("Orders table")
        .setCatalogId(catalogRid)
        .setNamespaceId(nsRid)
        .setRootUri("s3://upstream/tables/orders/")
        .setSchemaJson("{}")
        .setCreatedAt(Timestamps.fromMillis(clock.millis()))
        .setCurrentSnapshotId(200)
        .build();
    tableRepo.create(td);

    seedSnapshot(snapshotRepo, tenant, tableRid, 199, clock.millis() - 20_000, clock.millis() - 60_000);
    seedSnapshot(snapshotRepo, tenant, tableRid, 200, clock.millis() - 10_000, clock.millis() - 50_000);

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
    var snap = Snapshot.newBuilder()
      .setTableId(tableId)
      .setSnapshotId(snapshotId)
      .setIngestedAt(Timestamps.fromMillis(ingestedAtMs))
      .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAt))
      .build();
      

    snapshotRepo.create(snap);
  }
}

