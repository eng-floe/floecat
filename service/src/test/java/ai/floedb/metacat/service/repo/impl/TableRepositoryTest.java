package ai.floedb.metacat.service.repo.impl;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.storage.impl.InMemoryBlobStore;
import ai.floedb.metacat.service.storage.impl.InMemoryPointerStore;

class TableRepositoryTest {
  @Test
  void putAndGetRoundTrip() {
    var repo = new TableRepository();
    repo.ptr = new InMemoryPointerStore();
    repo.blobs = new InMemoryBlobStore();

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
      .setCreatedAtMs(System.currentTimeMillis())
      .setCurrentSnapshotId(42)
      .build();

    repo.put(td);

    var fetched = repo.get(tenantRid, catalogRid, nsRid, tableRid).orElseThrow();
    assertEquals("orders", fetched.getDisplayName());

    var list = repo.list(tenant, catalogId, nsId, 50, "", new StringBuilder());
    assertEquals(1, list.size());

    var cur = repo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(42, cur.getSnapshotId());
  }
}