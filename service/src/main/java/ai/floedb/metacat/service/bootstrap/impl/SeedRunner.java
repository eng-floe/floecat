package ai.floedb.metacat.service.bootstrap.impl;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

import com.google.protobuf.util.Timestamps;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.common.rpc.Pointer;

@ApplicationScoped
public class SeedRunner {
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject BlobStore blobs;
  @Inject PointerStore ptr;

  void onStart(@Observes StartupEvent ev) {
    final String tenant = "t-0001";
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var salesId = seedCatalog(tenant, "sales", "Sales catalog", now);
    var financeId = seedCatalog(tenant, "finance", "Finance catalog", now);

    var salesCoreNsId = seedNamespace(tenant, salesId, null, "core", now);
    var salesStg25NsId = seedNamespace(tenant, salesId, List.of("staging"), "2025", now);
    var financeCoreNsId = seedNamespace(tenant, financeId, null, "core", now);

    var ordersId = seedTable(tenant, salesId, salesCoreNsId.getId(), "orders", 0L, now);

    var lineitemId = seedTable(tenant, salesId, salesCoreNsId.getId(), "lineitem", 0L, now);

    seedSnapshot(tenant, ordersId, 101L, now - 60_000L, now - 100_000L);
    seedSnapshot(tenant, ordersId, 102L, now, now - 80_000L);

    var salesStgOrdersId = seedTable(tenant, salesId, salesStg25NsId.getId(), "orders_2025", 0L, now);

    var salesStgEventsId = seedTable(tenant, salesId, salesStg25NsId.getId(), "staging_events", 0L, now);

    var glEntriesId = seedTable(tenant, financeId, financeCoreNsId.getId(), "gl_entries", 0L, now);

    seedSnapshot(tenant, glEntriesId, 201L, now, now - 20_000L);
  }

  private ResourceId seedCatalog(String tenant, String displayName, String description, long now) {
    String id = uuidFor(tenant + "/catalog:" + displayName);
    var rid = ResourceId.newBuilder().setTenantId(tenant).setId(id).setKind(ResourceKind.RK_CATALOG).build();
    var cat = Catalog.newBuilder()
      .setResourceId(rid).setDisplayName(displayName).setDescription(description).setCreatedAt(Timestamps.fromMillis(now)).build();
    catalogs.create(cat);
    return rid;
  }

  private ResourceId seedNamespace(String tenant,
                                   ResourceId catalogId,
                                   List<String> path,
                                   String display,
                                   long now) {
    List<String> clean = (path == null) ? List.of() : path;
    List<String> parents = clean.isEmpty() ? List.of() : clean.subList(0, clean.size() - 1);
    String leaf = clean.isEmpty() ? display : clean.get(clean.size() - 1);

    String nsId = uuidFor(tenant + "/ns:" + displayPathKey(catalogId.getId(), clean));

    ResourceId nsRid = ResourceId.newBuilder()
      .setTenantId(tenant)
      .setId(nsId)
      .setKind(ResourceKind.RK_NAMESPACE)
      .build();

    Namespace ns = Namespace.newBuilder()
      .setResourceId(nsRid)
      .setDisplayName(leaf)
      .addAllParents(parents)
      .setDescription(leaf + " namespace")
      .setCreatedAt(Timestamps.fromMillis(now))
      .build();

    namespaces.create(ns, catalogId);
    return nsRid;
  }

  private ResourceId seedTable(String tenant, ResourceId catalogId, String namespaceId, String name, long snapshotId, long now) {
    String tableId = uuidFor(tenant + "/tbl:" + name);
    var tableRid = ResourceId.newBuilder()
      .setTenantId(tenant).setId(tableId).setKind(ResourceKind.RK_TABLE).build();
    var nsRid = ResourceId.newBuilder()
      .setTenantId(tenant).setId(namespaceId).setKind(ResourceKind.RK_NAMESPACE).build();

    String rootUri = "s3://seed-data/" + tenant + "/" + catalogId + "/" + namespaceId + "/" + name + "/";

    var td = Table.newBuilder()
      .setResourceId(tableRid)
      .setDisplayName(name)
      .setDescription(name + " table")
      .setCatalogId(catalogId)
      .setNamespaceId(nsRid)
      .setRootUri(rootUri)
      .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
      .setCreatedAt(Timestamps.fromMillis(now))
      .setCurrentSnapshotId(snapshotId)
      .build();

    tables.create(td);
    return tableRid;
  }

  private void seedSnapshot(String tenant, ResourceId tableId,
      long snapshotId, long ingestedAtMs, long upstreamCreatedAt) {
    var snap = Snapshot.newBuilder()
      .setTableId(tableId)
      .setSnapshotId(snapshotId)
      .setIngestedAt(Timestamps.fromMillis(ingestedAtMs))
      .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAt))
      .build();

    snapshots.create(snap);
  }

  private static String uuidFor(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }

  private static String displayPathKey(String catalogId, List<String> path) {
    return catalogId + "/" + String.join("/", path);
  }
}