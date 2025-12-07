package ai.floedb.floecat.service.bootstrap.impl;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
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
  @Inject AccountRepository accounts;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject ViewRepository views;
  @Inject BlobStore blobs;
  @Inject PointerStore ptr;

  @ConfigProperty(name = "floecat.seed.enabled", defaultValue = "true")
  boolean enabled;

  void onStart(@Observes StartupEvent ev) {
    if (enabled) {
      seedData();
    }
  }

  public void seedData() {
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var accountId = seedAccount("t-0001", "First account", now);

    var salesId = seedCatalog(accountId.getId(), "sales", "Sales catalog", now);

    var salesCoreNsId = seedNamespace(accountId.getId(), salesId, null, "core", now);
    var ordersId = seedTable(accountId.getId(), salesId, salesCoreNsId.getId(), "orders", 0L, now);
    seedTable(accountId.getId(), salesId, salesCoreNsId.getId(), "lineitem", 0L, now);
    seedSnapshot(accountId.getId(), ordersId, 101L, now - 60_000L, now - 100_000L);
    seedSnapshot(accountId.getId(), ordersId, 102L, now, now - 80_000L);
    seedView(
        accountId.getId(),
        salesId,
        salesCoreNsId.getId(),
        "recent_orders",
        "SELECT o.order_key, o.customer_key, o.order_status FROM sales.core.orders o WHERE"
            + " o.order_date >= date_sub(current_date, interval '30' day)",
        Map.of("seeded", "true"),
        now);

    var salesStg25NsId = seedNamespace(accountId.getId(), salesId, List.of("staging"), "2025", now);
    seedTable(accountId.getId(), salesId, salesStg25NsId.getId(), "orders_2025", 0L, now);
    seedTable(accountId.getId(), salesId, salesStg25NsId.getId(), "staging_events", 0L, now);
    seedView(
        accountId.getId(),
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

    var financeId = seedCatalog(accountId.getId(), "finance", "Finance catalog", now);
    var financeCoreNsId = seedNamespace(accountId.getId(), financeId, null, "core", now);
    var glEntriesId =
        seedTable(accountId.getId(), financeId, financeCoreNsId.getId(), "gl_entries", 0L, now);
    seedSnapshot(accountId.getId(), glEntriesId, 201L, now, now - 20_000L);
  }

  private ResourceId seedAccount(String displayName, String description, long now) {
    String id = uuidFor("/account:" + displayName);
    var rid =
        ResourceId.newBuilder().setAccountId(id).setId(id).setKind(ResourceKind.RK_ACCOUNT).build();
    var account =
        ai.floedb.floecat.account.rpc.Account.newBuilder()
            .setResourceId(rid)
            .setDisplayName(displayName)
            .setDescription(description)
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();
    accounts.create(account);
    return rid;
  }

  private ResourceId seedCatalog(String account, String displayName, String description, long now) {
    String id = uuidFor(account + "/catalog:" + displayName);
    var rid =
        ResourceId.newBuilder()
            .setAccountId(account)
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
      String account, ResourceId catalogId, List<String> path, String display, long now) {
    List<String> clean = (path == null) ? List.of() : path;
    List<String> parents = clean.isEmpty() ? List.of() : clean.subList(0, clean.size() - 1);
    String leaf = clean.isEmpty() ? display : clean.get(clean.size() - 1);

    String nsId = uuidFor(account + "/ns:" + displayPathKey(catalogId.getId(), clean));

    ResourceId nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
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
      String account,
      ResourceId catalogId,
      String namespaceId,
      String name,
      long snapshotId,
      long now) {
    String tableId = uuidFor(account + "/tbl:" + name);
    var tableRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(tableId)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(namespaceId)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    String rootUri = "s3://seed-data/";

    var upstream =
        UpstreamRef.newBuilder()
            .setUri(rootUri)
            .addAllNamespacePath(List.of(("a.b.c").split("\\.")))
            .setTableDisplayName("upstream_table")
            .setFormat(TableFormat.TF_ICEBERG)
            .build();

    var td =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setDisplayName(name)
            .setDescription(name + " table")
            .setCatalogId(catalogId)
            .setNamespaceId(nsRid)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setUpstream(upstream)
            .setCreatedAt(Timestamps.fromMillis(now))
            .build();

    tables.create(td);
    return tableRid;
  }

  private void seedSnapshot(
      String account,
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
      String account,
      ResourceId catalogId,
      String namespaceId,
      String name,
      String sql,
      Map<String, String> properties,
      long now) {
    String viewId = uuidFor(account + "/view:" + name);
    var viewRid =
        ResourceId.newBuilder()
            .setAccountId(account)
            .setId(viewId)
            .setKind(ResourceKind.RK_VIEW)
            .build();
    var nsRid =
        ResourceId.newBuilder()
            .setAccountId(account)
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
    return catalogId + "." + String.join(".", path);
  }
}
