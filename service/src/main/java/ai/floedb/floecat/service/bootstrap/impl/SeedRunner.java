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
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import com.google.protobuf.util.Timestamps;
import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SeedRunner {
  private static final Logger LOG = Logger.getLogger(SeedRunner.class);
  private static final List<String> CORE_NAMESPACE = List.of("core");
  private static final AtomicBoolean SEEDED = new AtomicBoolean(false);

  @Inject AccountRepository accounts;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject ViewRepository views;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject ConnectorRepository connectorRepo;
  @Inject ReconcilerService reconciler;
  @Inject Vertx vertx;

  @ConfigProperty(name = "floecat.seed.enabled", defaultValue = "true")
  boolean enabled;

  @ConfigProperty(name = "floecat.seed.mode", defaultValue = "iceberg")
  String seedMode;

  void onStart(@Observes StartupEvent ev) {
    if (!enabled) {
      LOG.info("Seeding disabled (floecat.seed.enabled=false)");
      return;
    }

    if (!SEEDED.compareAndSet(false, true)) {
      LOG.info("Seeding already completed; skipping");
      return;
    }

    LOG.infof("Starting seedData() mode=%s", seedMode);
    var future =
        vertx.<Void>executeBlocking(
            promise -> {
              try {
                seedData();
                promise.complete(null);
              } catch (Throwable t) {
                promise.fail(t);
              }
            },
            true);
    if (LaunchMode.current() == LaunchMode.NORMAL) {
      waitForSeeding(future);
    } else {
      LOG.infof("Seeding running asynchronously for launch mode %s", LaunchMode.current());
      future.onComplete(
          ar -> {
            if (ar.succeeded()) {
              LOG.info("Startup seeding completed successfully (async)");
            } else {
              LOG.error("Startup seeding failed (async)", ar.cause());
            }
          });
    }
  }

  private void waitForSeeding(io.vertx.core.Future<Void> future) {
    try {
      future.toCompletionStage().toCompletableFuture().join();
      LOG.info("Startup seeding completed successfully");
    } catch (CompletionException e) {
      LOG.error("Startup seeding failed", e.getCause());
      throw new RuntimeException("Startup seeding failed", e.getCause());
    }
  }

  public void seedData() {
    if (isFakeMode()) {
      seedFakeData();
    } else {
      seedRealData();
    }
  }

  private void seedRealData() {
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var accountId = seedAccount("t-0001", "First account", now);

    var salesId = seedCatalog(accountId.getId(), "sales", "Sales catalog", now);

    var salesCoreNsId = seedNamespace(accountId.getId(), salesId, null, "core", now);
    seedFixtureTables(accountId, salesId, now);

    seedView(
        accountId.getId(),
        salesId,
        salesCoreNsId.getId(),
        "fixture_preview",
        "SELECT i FROM sales.core.trino_test WHERE i IS NOT NULL",
        Map.of("seeded", "true", "source", "iceberg-fixtures"),
        now);
  }

  private void seedFakeData() {
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var accountId = seedAccount("t-0001", "First account", now);

    var salesId = seedCatalog(accountId.getId(), "sales", "Sales catalog", now);
    var salesCoreNsId = seedNamespace(accountId.getId(), salesId, null, "core", now);
    var ordersId = seedFakeTable(accountId.getId(), salesId, salesCoreNsId.getId(), "orders", now);
    seedFakeTable(accountId.getId(), salesId, salesCoreNsId.getId(), "lineitem", now);
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
    seedFakeTable(accountId.getId(), salesId, salesStg25NsId.getId(), "orders_2025", now);
    seedFakeTable(accountId.getId(), salesId, salesStg25NsId.getId(), "staging_events", now);
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
        seedFakeTable(accountId.getId(), financeId, financeCoreNsId.getId(), "gl_entries", now);
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

  private ResourceId seedFakeTable(
      String account, ResourceId catalogId, String namespaceId, String name, long now) {
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

    var table =
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

    tables.create(table);
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

  private void seedFixtureTables(ResourceId accountId, ResourceId catalogId, long now) {
    TestS3Fixtures.seedFixturesOnce();
    String fixtureRoot = TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString();

    List<FixtureConfig> fixtures =
        List.of(
            new FixtureConfig(
                "fixture-simple",
                "Simple Iceberg fixture table",
                TestS3Fixtures.bucketUri(
                    "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json"),
                TestS3Fixtures.bucketUri(null),
                "fixtures.simple",
                "trino_test",
                CORE_NAMESPACE),
            new FixtureConfig(
                "fixture-types",
                "Trino types Iceberg fixture table",
                "s3://floecat/sales/us/trino_types/metadata/00001-d751d7ce-209e-443e-9937-c25e2a08fc29.metadata.json",
                "s3://floecat/sales/us/trino_types",
                "sales.us",
                "trino_types",
                CORE_NAMESPACE));

    for (FixtureConfig fixture : fixtures) {
      ResourceId connectorId =
          seedIcebergConnector(accountId, catalogId, fixture, fixtureRoot, now);
      syncConnector(connectorId, fixture);
    }
  }

  private ResourceId seedIcebergConnector(
      ResourceId accountId,
      ResourceId catalogId,
      FixtureConfig fixture,
      String fixtureRoot,
      long now) {

    String connectorUuid = uuidFor(accountId.getId() + "/connector:" + fixture.connectorName());
    var connectorRid =
        ResourceId.newBuilder()
            .setAccountId(accountId.getId())
            .setId(connectorUuid)
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();

    List<String> sourceSegments = namespaceSegments(fixture.sourceNamespace());
    var sourceNs = NamespacePath.newBuilder().addAllSegments(sourceSegments).build();
    var destNs = NamespacePath.newBuilder().addAllSegments(fixture.destinationNamespace()).build();

    var connector =
        Connector.newBuilder()
            .setResourceId(connectorRid)
            .setDisplayName(fixture.connectorName())
            .setDescription(fixture.description())
            .setKind(ConnectorKind.CK_ICEBERG)
            .setState(ConnectorState.CS_ACTIVE)
            .setUri(fixture.upstreamUri())
            .setSource(
                SourceSelector.newBuilder().setNamespace(sourceNs).setTable(fixture.tableName()))
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(catalogId)
                    .setNamespace(destNs)
                    .setTableDisplayName(fixture.tableName()))
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .setCreatedAt(Timestamps.fromMillis(now))
            .setUpdatedAt(Timestamps.fromMillis(now))
            .putAllProperties(
                Map.of(
                    "external.metadata-location", fixture.metadataLocation(),
                    "external.namespace", fixture.sourceNamespace(),
                    "external.table-name", fixture.tableName(),
                    "io-impl", InMemoryS3FileIO.class.getName(),
                    "fs.floecat.test-root", fixtureRoot,
                    "stats.ndv.enabled", "false"));

    connectorRepo.create(connector.build());
    LOG.infov(
        "Seeded connector {0} for fixture table {1}", fixture.connectorName(), fixture.tableName());
    return connectorRid;
  }

  private void syncConnector(ResourceId connectorId, FixtureConfig fixture) {
    var scope =
        ReconcileScope.of(List.of(fixture.destinationNamespace()), fixture.tableName(), List.of());
    var result =
        reconciler.reconcile(
            connectorId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);
    if (result.ok()) {
      LOG.infov(
          "Populated fixture table {0} (scanned={1}, changed={2})",
          fixture.tableName(), result.scanned, result.changed);
    } else {
      LOG.warnf(
          result.error,
          "Failed to populate fixture table {0}: {1}",
          fixture.tableName(),
          result.message());
    }
  }

  private static List<String> namespaceSegments(String namespaceFq) {
    if (namespaceFq == null || namespaceFq.isBlank()) {
      return List.of();
    }
    return List.of(namespaceFq.split("\\."));
  }

  private record FixtureConfig(
      String connectorName,
      String description,
      String metadataLocation,
      String upstreamUri,
      String sourceNamespace,
      String tableName,
      List<String> destinationNamespace) {}

  private static String uuidFor(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }

  private static String displayPathKey(String catalogId, List<String> path) {
    return catalogId + "." + String.join(".", path);
  }

  private boolean isFakeMode() {
    return seedMode != null && seedMode.equalsIgnoreCase("fake");
  }
}
