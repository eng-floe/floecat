package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.google.protobuf.util.Timestamps;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class TableRepositoryTest {

  private final Clock clock = Clock.systemUTC();

  @Test
  void putAndGetRoundTrip() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var snapshotRepo = new SnapshotRepository(ptr, blobs);
    var tableRepo = new TableRepository(ptr, blobs);

    var tenant = "t-0001";
    var catalogId = UUID.randomUUID().toString();
    var nsId = UUID.randomUUID().toString();
    var tblId = UUID.randomUUID().toString();

    var tenantRid = ResourceId.newBuilder().setTenantId(tenant).build();
    var catalogRid = ResourceId.newBuilder().setTenantId(tenant).setId(catalogId).setKind(ResourceKind.RK_CATALOG).build();
    var nsRid = ResourceId.newBuilder().setTenantId(tenant).setId(nsId).setKind(ResourceKind.RK_NAMESPACE).build();
    var tableRid = ResourceId.newBuilder().setTenantId(tenant).setId(tblId).setKind(ResourceKind.RK_TABLE).build();

    var td = TableDescriptor.newBuilder()
      .setResourceId(tableRid)
      .setDisplayName("orders")
      .setDescription("Orders table")
      .setCatalogId(catalogRid)
      .setNamespaceId(nsRid)
      .setRootUri("s3://upstream/tables/orders")
      .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
      .setCreatedAtMs(Timestamps.fromMillis(clock.millis()))
      .setCurrentSnapshotId(42)
      .build();
    tableRepo.put(td);

    var snap = Snapshot.newBuilder()
      .setSnapshotId(42)
      .setCreatedAtMs(Timestamps.fromMillis(clock.millis()))
      .build();
    snapshotRepo.put(tableRid, snap);

    var fetched = tableRepo.get(tenantRid, catalogRid, nsRid, tableRid).orElseThrow();
    assertEquals("orders", fetched.getDisplayName());

    var list = tableRepo.list(tenant, catalogId, nsId, 50, "", new StringBuilder());
    assertEquals(1, list.size());

    var cur = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(42, cur.getSnapshotId());
  }
}