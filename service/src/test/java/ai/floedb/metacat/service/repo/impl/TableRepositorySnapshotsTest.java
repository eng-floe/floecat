package ai.floedb.metacat.service.repo.impl;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class TableRepositorySnapshotsTest {
  @Test
  void list_count_and_currentSnapshot_fromSeeded() {
    var repo = new TableRepository();
    var ptr  = new InMemoryPointerStore();
    var blobs= new InMemoryBlobStore();
    repo.ptr = ptr;
    repo.blobs = blobs;

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
      .setCreatedAtMs(System.currentTimeMillis())
      .setCurrentSnapshotId(200)
      .build();
    repo.put(td);

    seedSnapshot(ptr, blobs, tenant, tblId, 199, System.currentTimeMillis() - 60_000);
    seedSnapshot(ptr, blobs, tenant, tblId, 200, System.currentTimeMillis());

    StringBuilder next = new StringBuilder();
    var page1 = repo.listSnapshots(tableRid, 1, "", next);
    assertEquals(1, page1.size());
    assertFalse(next.toString().isEmpty(), "should return next_page_token");

    String token = next.toString();
    next.setLength(0);
    var page2 = repo.listSnapshots(tableRid, 10, token, next);
    assertTrue(page2.size() >= 1);
    assertTrue(next.toString().isEmpty(), "no more pages");

    int total = repo.countSnapshots(tableRid);
    assertEquals(2, total);

    var cur = repo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(200, cur.getSnapshotId());
  }

  private static void seedSnapshot(InMemoryPointerStore ptr, InMemoryBlobStore blobs,
                                   String tenant, String tableId, long snapId, long createdMs) {
    String key = "/tenants/" + tenant + "/tables/" + tableId + "/snapshots/" + snapId;
    String uri = "mem://tenants/" + tenant + "/tables/" + tableId + "/snapshots/" + snapId + ".pb";
    var snap = Snapshot.newBuilder().setSnapshotId(snapId).setCreatedAtMs(createdMs).build();
    blobs.put(uri, snap.toByteArray(), "application/x-protobuf");

    long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
    boolean ok = ptr.compareAndSet(key, expected,
        Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(expected + 1).build());
    assertTrue(ok, "snapshot CAS should succeed for " + key);
  }
}
