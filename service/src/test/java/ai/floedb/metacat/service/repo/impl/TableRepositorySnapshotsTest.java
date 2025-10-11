package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import com.google.protobuf.util.Timestamps;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class TableRepositorySnapshotsTest {

  private final Clock clock = Clock.systemUTC();

  @Test
  void list_count_and_currentSnapshot_fromSeeded() {
    var ptr  = new InMemoryPointerStore();
    var blobs= new InMemoryBlobStore();
    var nameIndexRepo = new NameIndexRepository(ptr, blobs);
    var snapshotRepo = new SnapshotRepository(ptr, blobs);
    var tableRepo = new TableRepository(nameIndexRepo, ptr, blobs);

    String tenant = "t-0001";
    String catalogId = UUID.randomUUID().toString();
    String nsId = UUID.randomUUID().toString();
    String tblId = UUID.randomUUID().toString();

    var catalogRid = ResourceId.newBuilder().setTenantId(tenant).setId(catalogId).setKind(ResourceKind.RK_CATALOG).build();
    var nsRid = ResourceId.newBuilder().setTenantId(tenant).setId(nsId).setKind(ResourceKind.RK_NAMESPACE).build();
    var tableRid = ResourceId.newBuilder().setTenantId(tenant).setId(tblId).setKind(ResourceKind.RK_TABLE).build();

    var td = TableDescriptor.newBuilder()
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
    tableRepo.put(td);

    seedSnapshot(ptr, blobs, tenant, tblId, 199, clock.millis() - 60_000);
    seedSnapshot(ptr, blobs, tenant, tblId, 200, clock.millis());

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

  private static void seedSnapshot(InMemoryPointerStore ptr, InMemoryBlobStore blobs,
                                   String tenant, String tableId, long snapId, long createdMs) {
    String key = "/tenants/" + tenant + "/tables/" + tableId + "/snapshots/" + snapId;
    String uri = "mem://tenants/" + tenant + "/tables/" + tableId + "/snapshots/" + snapId + ".pb";
    var snap = Snapshot.newBuilder().setSnapshotId(snapId).setCreatedAt(Timestamps.fromMillis(createdMs)).build();
    blobs.put(uri, snap.toByteArray(), "application/x-protobuf");

    long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
    boolean ok = ptr.compareAndSet(key, expected,
        Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(expected + 1).build());
    assertTrue(ok, "snapshot CAS should succeed for " + key);
  }
}
