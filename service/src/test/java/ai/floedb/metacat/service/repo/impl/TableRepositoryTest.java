package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import com.google.protobuf.util.Timestamps;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.util.Keys;
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
    var repo = new NamespaceRepository(ptr, blobs);
    var catRepo = new CatalogRepository(ptr, blobs);

    var tenant = "t-0001";
    var catRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_CATALOG)
        .build();

    Catalog cat = Catalog.newBuilder()
        .setResourceId(catRid)
        .setDisplayName("sales")
        .build();
    catRepo.create(cat);

    var nsRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_NAMESPACE)
        .build();

    var ns = Namespace.newBuilder()
        .setResourceId(nsRid)
        .setDisplayName("core")
        .setDescription("Core namespace")
        .build();
    repo.create(ns, catRid);

    var tableRid = ResourceId.newBuilder()
        .setTenantId(tenant)
        .setId(UUID.randomUUID().toString())
        .setKind(ResourceKind.RK_TABLE)
        .build();

    var td = Table.newBuilder()
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

    String nsKeyRow = Keys.tblByNamePtr(tenant, catRid.getId(), nsRid.getId(), "orders");
    var pRow = ptr.get(nsKeyRow);
    assertTrue(pRow.isPresent(), "by-namespace ROW pointer missing");

    String nsKeyPfx = Keys.tblByNamePrefix(tenant, catRid.getId(), nsRid.getId());
    var rowsUnderPfx = ptr.listPointersByPrefix(nsKeyPfx, 100, "", new StringBuilder());
    assertTrue(rowsUnderPfx.stream().anyMatch(r -> r.key().equals(nsKeyRow)),
        "prefix scan doesn't see the row key you just wrote");

    String uri = pRow.get().getBlobUri();
    assertNotNull(uri);
    assertNotNull(blobs.head(uri).orElse(null), "blob header missing for by-namespace row");
    assertNotNull(blobs.get(uri), "blob bytes missing for by-namespace row");

    var snap = Snapshot.newBuilder()
        .setTableId(tableRid)
        .setSnapshotId(42)
        .setIngestedAt(Timestamps.fromMillis(clock.millis()))
        .build();
    snapshotRepo.create(snap);

    var fetched = tableRepo.get(tableRid).orElseThrow();
    assertEquals("orders", fetched.getDisplayName());

    var list = tableRepo.listByNamespace(catRid, nsRid, 50, "", new StringBuilder());
    assertEquals(1, list.size());

    var cur = snapshotRepo.getCurrentSnapshot(tableRid).orElseThrow();
    assertEquals(42, cur.getSnapshotId());
  }
}
