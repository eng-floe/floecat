package ai.floedb.metacat.service.bootstrap.impl;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.tenancy.rpc.Tenant;
import com.google.protobuf.util.Timestamps;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class SeedRunner {
  @Inject TenantRepository tenants;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject ViewRepository views;
  @Inject BlobStore blobs;
  @Inject PointerStore ptr;

  @ConfigProperty(name = "metacat.seed.enabled", defaultValue = "true")
  boolean enabled;

  void onStart(@Observes StartupEvent ev) {
    if (enabled) {
      seedData();
    }
  }

  public void seedData() {
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var tenantId = seedTenant("t-0001", "First tenant", now);

    var salesId = seedCatalog(tenantId.getId(), "sales", "Sales catalog", now);

    var salesCoreNsId = seedNamespace(tenantId.getId(), salesId, null, "core", now);
    var ordersId = seedTable(tenantId.getId(), salesId, salesCoreNsId.getId(), "orders", 0L, now);
    seedTable(tenantId.getId(), salesId, salesCoreNsId.getId(), "lineitem", 0L, now);
    seedSnapshot(tenantId.getId(), ordersId, 101L, now - 60_000L, now - 100_000L);
    seedSnapshot(tenantId.getId(), ordersId, 102L, now, now - 80_000L);
    seedView(
        tenantId.getId(),
        salesId,
        salesCoreNsId.getId(),
        "recent_orders",
        "SELECT o.order_key, o.customer_key, o.order_status FROM sales.core.orders o WHERE"
            + " o.order_date >= date_sub(current_date, interval '30' day)",
        Map.of("seeded", "true"),
        now);

    var salesStg25NsId = seedNamespace(tenantId.getId(), salesId, List.of("staging"), "2025", now);
    seedTable(tenantId.getId(), salesId, salesStg25NsId.getId(), "orders_2025", 0L, now);
    seedTable(tenantId.getId(), salesId, salesStg25NsId.getId(), "staging_events", 0L, now);
    seedView(
        tenantId.getId(),
        salesId,
        salesStg25NsId.getId(),
        "orders_with_events",
        """
        SELECT o.order_id, o.customer_id, e.event_type, e.event_ts
        FROM sales.staging.2025.orders_2025 o
        JOIN sales.staging.2025.staging_events e
          ON o.order_id = e.order_id
        """,
        Map.of("materialized", "false", "seeded", "true"),
        now);

    var financeId = seedCatalog(tenantId.getId(), "finance", "Finance catalog", now);
    var financeCoreNsId = seedNamespace(tenantId.getId(), financeId, null, "core", now);
    var glEntriesId =
        seedTable(tenantId.getId(), financeId, financeCoreNsId.getId(), "gl_entries", 0L, now);
    seedSnapshot(tenantId.getId(), glEntriesId, 201L, now, now - 20_000L);
  }

  private ResourceId seedTenant(String displayName, String description, long now) {
    String id = uuidFor("/tenant:" + displayName);
    var rid =
        ResourceId.newBuilder().setTenantId(id).setId(id).setKind(ResourceKind.RK_TENANT).build();
    var tenant =
        Tenant.newBuilder()
            .setResourceId(rid)
            .setDisplayName(displayName)
            .setDescription(description)
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();
    tenants.create(tenant);
    return rid;
  }

  private ResourceId seedCatalog(String tenant, String displayName, String description, long now) {
    String id = uuidFor(tenant + "/catalog:" + displayName);
    var rid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(id)
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var cat =
        Catalog.newBuilder()
            .setResourceId(rid)
            .setDisplayName(displayName)
            .setDescription(description)
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();
    catalogs.create(cat);
    return rid;
  }

  private ResourceId seedNamespace(
      String tenant, ResourceId catalogId, List<String> path, String display, long now) {
    List<String> clean = (path == null) ? List.of() : path;
    List<String> parents = clean.isEmpty() ? List.of() : clean.subList(0, clean.size() - 1);
    String leaf = clean.isEmpty() ? display : clean.get(clean.size() - 1);

    String nsId = uuidFor(tenant + "/ns:" + displayPathKey(catalogId.getId(), clean));

    ResourceId nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(nsId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    Namespace ns =
        Namespace.newBuilder()
            .setResourceId(nsRid)
            .setDisplayName(leaf)
            .addAllParents(parents)
            .setCatalogId(catalogId)
            .setDescription(leaf + " namespace")
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();

    namespaces.create(ns);
    return nsRid;
  }

  private ResourceId seedTable(
      String tenant,
      ResourceId catalogId,
      String namespaceId,
      String name,
      long snapshotId,
      long now) {
    String tableId = uuidFor(tenant + "/tbl:" + name);
    var tableRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(tableId)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(namespaceId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    String rootUri = "s3://seed-data/";

    var td =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setDisplayName(name)
            .setDescription(name + " table")
            .setFormat(TableFormat.TF_ICEBERG)
            .setCatalogId(catalogId)
            .setNamespaceId(nsRid)
            .setRootUri(rootUri)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();

    tables.create(td);
    return tableRid;
  }

  private void seedSnapshot(
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

    snapshots.create(snap);
  }

  private ResourceId seedView(
      String tenant,
      ResourceId catalogId,
      String namespaceId,
      String name,
      String sql,
      Map<String, String> properties,
      long now) {
    String viewId = uuidFor(tenant + "/view:" + name);
    var viewRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(viewId)
            .setKind(ResourceKind.RK_VIEW)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setTenantId(tenant)
            .setId(namespaceId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var view =
        View.newBuilder()
            .setResourceId(viewRid)
            .setDisplayName(name)
            .setDescription(name + " view")
            .setCatalogId(catalogId)
            .setNamespaceId(nsRid)
            .setSql(sql)
            .setCreatedAt(Timestamps.fromMillis(now))
            .putAllProperties(properties == null ? Map.of() : properties)
            .build();

    views.create(view);
    return viewRid;
  }

  private static String uuidFor(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }

  private static String displayPathKey(String catalogId, List<String> path) {
    return catalogId + "/" + String.join("/", path);
  }
}
