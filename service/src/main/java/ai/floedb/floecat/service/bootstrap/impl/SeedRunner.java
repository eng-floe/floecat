/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.bootstrap.impl;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.PrincipalContext;
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
import ai.floedb.floecat.gateway.iceberg.rest.common.TestDeltaFixtures;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SeedRunner {
  private static final Logger LOG = Logger.getLogger(SeedRunner.class);
  private static final String EXAMPLES_CATALOG = "examples";
  private static final List<String> ICEBERG_NAMESPACE = List.of("iceberg");
  private static final List<String> DELTA_NAMESPACE = List.of("delta");
  private static final AtomicBoolean SEEDED = new AtomicBoolean(false);
  private static final int SEED_SYNC_MAX_ATTEMPTS = 6;
  private static final long SEED_SYNC_INITIAL_BACKOFF_MS = 250L;
  private static final long SEED_SYNC_MAX_BACKOFF_MS = 5_000L;
  private static final int SEED_TOKEN_MAX_ATTEMPTS = 6;
  private static final long SEED_TOKEN_INITIAL_BACKOFF_MS = 250L;
  private static final long SEED_TOKEN_MAX_BACKOFF_MS = 5_000L;
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

  @ConfigProperty(name = "floecat.seed.oidc.token")
  java.util.Optional<String> seedOidcToken;

  @ConfigProperty(name = "floecat.seed.oidc.issuer")
  java.util.Optional<String> seedOidcIssuer;

  @ConfigProperty(name = "floecat.seed.oidc.client-id")
  java.util.Optional<String> seedOidcClientId;

  @ConfigProperty(name = "floecat.seed.oidc.client-secret")
  java.util.Optional<String> seedOidcClientSecret;

  @ConfigProperty(name = "floecat.seed.oidc.timeout", defaultValue = "10s")
  Duration seedOidcTimeout;

  private volatile java.util.Optional<String> seedAuthorizationHeader;

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
    future
        .onSuccess(v -> LOG.info("Startup seeding completed successfully"))
        .onFailure(t -> LOG.error("Startup seeding failed", t));
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

    var examplesId = seedCatalog(accountId.getId(), EXAMPLES_CATALOG, "Examples catalog", now);

    var icebergNsId =
        seedNamespace(
            accountId.getId(),
            examplesId,
            List.of(ICEBERG_NAMESPACE.get(0)),
            ICEBERG_NAMESPACE.get(0),
            now);
    seedNamespace(
        accountId.getId(),
        examplesId,
        List.of(DELTA_NAMESPACE.get(0)),
        DELTA_NAMESPACE.get(0),
        now);
    seedFixtureTables(accountId, examplesId, now);
    seedDeltaFixtureTables(accountId, examplesId, now);

    seedView(
        accountId.getId(),
        examplesId,
        icebergNsId.getId(),
        "fixture_preview",
        "SELECT i FROM examples.iceberg.trino_test WHERE i IS NOT NULL",
        Map.of("seeded", "true", "source", "iceberg-fixtures"),
        now);
  }

  private void seedFakeData() {
    final Clock clock = Clock.systemUTC();
    final long now = clock.millis();

    var accountId = seedAccount("t-0001", "First account", now);

    var examplesId = seedCatalog(accountId.getId(), EXAMPLES_CATALOG, "Examples catalog", now);
    var icebergNsId =
        seedNamespace(
            accountId.getId(),
            examplesId,
            List.of(ICEBERG_NAMESPACE.get(0)),
            ICEBERG_NAMESPACE.get(0),
            now);
    seedNamespace(
        accountId.getId(),
        examplesId,
        List.of(DELTA_NAMESPACE.get(0)),
        DELTA_NAMESPACE.get(0),
        now);
    var ordersId = seedFakeTable(accountId.getId(), examplesId, icebergNsId.getId(), "orders", now);
    seedFakeTable(accountId.getId(), examplesId, icebergNsId.getId(), "lineitem", now);
    seedSnapshot(accountId.getId(), ordersId, 101L, now - 60_000L, now - 100_000L);
    seedSnapshot(accountId.getId(), ordersId, 102L, now, now - 80_000L);
    seedView(
        accountId.getId(),
        examplesId,
        icebergNsId.getId(),
        "recent_orders",
        "SELECT o.order_key, o.customer_key, o.order_status FROM examples.iceberg.orders o WHERE"
            + " o.order_date >= date_sub(current_date, interval '30' day)",
        Map.of("seeded", "true"),
        now);

    var salesStg25NsId =
        seedNamespace(
            accountId.getId(), examplesId, List.of("iceberg", "staging", "2025"), "2025", now);
    seedFakeTable(accountId.getId(), examplesId, salesStg25NsId.getId(), "orders_2025", now);
    seedFakeTable(accountId.getId(), examplesId, salesStg25NsId.getId(), "staging_events", now);
    seedView(
        accountId.getId(),
        examplesId,
        salesStg25NsId.getId(),
        "orders_with_events",
        """
        SELECT o.order_id, o.customer_id, e.event_type, e.event_ts
        FROM examples.iceberg.staging.2025.orders_2025 o
        JOIN examples.iceberg.staging.2025.staging_events e
          ON o.order_id = e.order_id
        """,
        Map.of("materialized", "false", "seeded", "true"),
        now);
  }

  private ResourceId seedAccount(String displayName, String description, long now) {
    var existing = accounts.getByName(displayName).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
    String id = "5eaa9cd5-7d08-3750-9457-cfe800b0b9d2";
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
    var existing = catalogs.getByName(account, displayName).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
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
    var existing = namespaces.getByPath(account, catalogId.getId(), clean).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
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
    var existing = views.getByName(account, catalogId.getId(), namespaceId, name).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
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
    var existing = tables.getByName(account, catalogId.getId(), namespaceId, name).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
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
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
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
    if (snapshots.getById(tableId, snapshotId).isPresent()) {
      return;
    }
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
                ICEBERG_NAMESPACE),
            new FixtureConfig(
                "fixture-types",
                "Trino types Iceberg fixture table",
                "s3://floecat/sales/us/trino_types/metadata/00001-d751d7ce-209e-443e-9937-c25e2a08fc29.metadata.json",
                "s3://floecat/sales/us/trino_types",
                "sales.us",
                "trino_types",
                ICEBERG_NAMESPACE));

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
    var existing = connectorRepo.getByName(accountId.getId(), fixture.connectorName()).orElse(null);
    if (existing != null) {
      ensureIcebergFixtureProperties(existing, now);
      return existing.getResourceId();
    }

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
            .putAllProperties(connectorProperties(fixture, fixtureRoot));

    connectorRepo.create(connector.build());
    LOG.infov(
        "Seeded connector {0} for fixture table {1}", fixture.connectorName(), fixture.tableName());
    return connectorRid;
  }

  private void ensureIcebergFixtureProperties(Connector existing, long now) {
    var props = new LinkedHashMap<>(existing.getPropertiesMap());
    boolean hasExternal =
        props.containsKey("external.metadata-location")
            && !props.get("external.metadata-location").isBlank();
    boolean hasSource =
        props.containsKey("iceberg.source") && !props.get("iceberg.source").isBlank();
    if (!hasExternal || hasSource) {
      return;
    }
    props.put("iceberg.source", "filesystem");

    var updated =
        existing.toBuilder()
            .clearProperties()
            .putAllProperties(props)
            .setUpdatedAt(Timestamps.fromMillis(now))
            .build();
    var meta = connectorRepo.metaFor(existing.getResourceId());
    if (!connectorRepo.update(updated, meta.getPointerVersion())) {
      LOG.warnf(
          "Failed to update fixture connector properties for %s", existing.getResourceId().getId());
    }
  }

  private Map<String, String> connectorProperties(FixtureConfig fixture, String fixtureRoot) {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("iceberg.source", "filesystem");
    props.put("external.metadata-location", fixture.metadataLocation());
    props.put("external.namespace", fixture.sourceNamespace());
    props.put("external.table-name", fixture.tableName());
    props.put("stats.ndv.enabled", "false");
    if (TestS3Fixtures.useAwsFixtures()) {
      props.putAll(TestS3Fixtures.awsFileIoProperties());
    } else {
      props.put("io-impl", InMemoryS3FileIO.class.getName());
      props.put("fs.floecat.test-root", fixtureRoot);
    }
    return props;
  }

  private void seedDeltaFixtureTables(ResourceId accountId, ResourceId catalogId, long now) {
    TestDeltaFixtures.seedFixturesOnce();

    List<DeltaFixtureConfig> fixtures =
        List.of(
            new DeltaFixtureConfig(
                "fixture-delta-call-center",
                "Delta call_center fixture table",
                TestDeltaFixtures.tableUri("call_center"),
                "examples.delta",
                "call_center",
                DELTA_NAMESPACE),
            new DeltaFixtureConfig(
                "fixture-delta-my-local-delta-table",
                "Delta my_local_delta_table fixture table",
                TestDeltaFixtures.tableUri("my_local_delta_table"),
                "examples.delta",
                "my_local_delta_table",
                DELTA_NAMESPACE),
            new DeltaFixtureConfig(
                "fixture-delta-dv-demo-delta",
                "Delta dv_demo_delta fixture table",
                TestDeltaFixtures.tableUri("dv_demo_delta"),
                "examples.delta",
                "dv_demo_delta",
                DELTA_NAMESPACE));

    for (DeltaFixtureConfig fixture : fixtures) {
      ResourceId connectorId = seedDeltaConnector(accountId, catalogId, fixture, now);
      syncConnector(connectorId, fixture.tableName(), fixture.destinationNamespace());
    }
  }

  private ResourceId seedDeltaConnector(
      ResourceId accountId, ResourceId catalogId, DeltaFixtureConfig fixture, long now) {
    var existing = connectorRepo.getByName(accountId.getId(), fixture.connectorName()).orElse(null);
    if (existing != null) {
      return existing.getResourceId();
    }
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
            .setKind(ConnectorKind.CK_DELTA)
            .setState(ConnectorState.CS_ACTIVE)
            .setUri("http://localhost")
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
            .putAllProperties(deltaConnectorProperties(fixture));

    connectorRepo.create(connector.build());
    LOG.infov(
        "Seeded connector {0} for delta fixture table {1}",
        fixture.connectorName(), fixture.tableName());
    return connectorRid;
  }

  private Map<String, String> deltaConnectorProperties(DeltaFixtureConfig fixture) {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("delta.source", "filesystem");
    props.put("delta.table-root", fixture.tableRoot());
    props.put("external.namespace", fixture.sourceNamespace());
    props.put("external.table-name", fixture.tableName());
    props.put("stats.ndv.enabled", "false");
    if (TestDeltaFixtures.useAwsFixtures()) {
      props.putAll(TestDeltaFixtures.s3Options());
    } else {
      String fixtureRoot = System.getProperty("fs.floecat.test-root", "");
      if (!fixtureRoot.isBlank()) {
        props.put("fs.floecat.test-root", fixtureRoot);
      }
    }
    return props;
  }

  private void syncConnector(ResourceId connectorId, FixtureConfig fixture) {
    syncConnector(connectorId, fixture.tableName(), fixture.destinationNamespace());
  }

  private void syncConnector(
      ResourceId connectorId, String tableName, List<String> destinationNamespace) {
    var scope = ReconcileScope.of(List.of(destinationNamespace), tableName, List.of());
    long backoffMs = SEED_SYNC_INITIAL_BACKOFF_MS;
    for (int attempt = 1; attempt <= SEED_SYNC_MAX_ATTEMPTS; attempt++) {
      try {
        var result = reconcileWithSeedAuth(connectorId, scope);
        if (result.ok()) {
          LOG.infov(
              "Populated fixture table {0} (scanned={1}, changed={2})",
              tableName, result.scanned, result.changed);
          return;
        }
        if (result.error != null && isRetryableSeedSyncError(result.error)) {
          if (attempt == SEED_SYNC_MAX_ATTEMPTS) {
            LOG.warnf(
                result.error,
                "Failed to populate fixture table {0}: {1}",
                tableName,
                result.message());
            return;
          }
          if (Thread.currentThread().isInterrupted()) {
            LOG.warnf(
                "Seed sync thread interrupted for %s; clearing interrupt and retrying", tableName);
            Thread.interrupted();
          }
          LOG.warnf(
              "Retrying fixture sync for %s (attempt %d/%d) after %dms due to: %s",
              tableName, attempt, SEED_SYNC_MAX_ATTEMPTS, backoffMs, result.message());
          LockSupport.parkNanos(backoffMs * 1_000_000L);
          backoffMs = Math.min(backoffMs * 2, SEED_SYNC_MAX_BACKOFF_MS);
          continue;
        }
        LOG.warnf(
            result.error, "Failed to populate fixture table {0}: {1}", tableName, result.message());
        return;
      } catch (RuntimeException e) {
        // gRPC blocking calls may set the interrupt flag (e.g., CANCELLED: Thread interrupted).
        // During startup seeding we treat this as transient; clear the flag so retries can proceed.
        if (Thread.currentThread().isInterrupted()) {
          LOG.warnf(
              "Seed sync thread interrupted for %s; clearing interrupt and retrying", tableName);
          Thread.interrupted(); // clears interrupt status
        }
        if (!isRetryableSeedSyncError(e) || attempt == SEED_SYNC_MAX_ATTEMPTS) {
          throw e;
        }
        LOG.warnf(
            "Retrying fixture sync for %s (attempt %d/%d) after %dms due to: %s",
            tableName, attempt, SEED_SYNC_MAX_ATTEMPTS, backoffMs, e.getMessage());
        LockSupport.parkNanos(backoffMs * 1_000_000L);
        backoffMs = Math.min(backoffMs * 2, SEED_SYNC_MAX_BACKOFF_MS);
      }
    }
  }

  private static boolean isRetryableSeedSyncError(Throwable error) {
    Throwable current = error;
    while (current != null) {
      // Retry transient gRPC startup conditions.
      if (current instanceof StatusRuntimeException statusEx) {
        var code = statusEx.getStatus().getCode();
        if (code == Status.Code.UNAVAILABLE || code == Status.Code.NOT_FOUND) {
          return true;
        }
        // If the call was cancelled but the thread wasn't interrupted, treat it as transient.
        if (code == Status.Code.CANCELLED && !Thread.currentThread().isInterrupted()) {
          return true;
        }
      }

      // Reconciler wraps getConnector failures as IllegalArgumentException; treat that as
      // retryable.
      if (current instanceof IllegalArgumentException iae) {
        String msg = iae.getMessage();
        if (msg != null
            && (msg.startsWith("Connector not found:") || msg.startsWith("getConnector failed"))) {
          return true;
        }
      }

      current = current.getCause();
    }
    return false;
  }

  private ReconcilerService.Result reconcileWithSeedAuth(
      ResourceId connectorId, ReconcileScope scope) {
    var header = seedAuthorizationHeader();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setSubject("seed-runner")
            .setCorrelationId("seed-sync-" + connectorId.getId())
            .build();
    return reconciler.reconcile(
        principal,
        connectorId,
        true,
        scope,
        ReconcilerService.CaptureMode.METADATA_AND_STATS,
        header.orElse(null));
  }

  private java.util.Optional<String> seedAuthorizationHeader() {
    if (seedAuthorizationHeader != null) {
      return seedAuthorizationHeader;
    }
    synchronized (this) {
      if (seedAuthorizationHeader != null) {
        return seedAuthorizationHeader;
      }
      var built = buildSeedAuthorizationHeader();
      if (built.isPresent()) {
        seedAuthorizationHeader = built;
        return seedAuthorizationHeader;
      }
      return built;
    }
  }

  private java.util.Optional<String> buildSeedAuthorizationHeader() {
    var explicit = seedOidcToken.map(String::trim).filter(v -> !v.isBlank());
    if (explicit.isPresent()) {
      LOG.info("Using configured seed OIDC token for fixture sync.");
      return java.util.Optional.of(withBearerPrefix(explicit.get()));
    }

    var issuer = seedOidcIssuer.map(String::trim).filter(v -> !v.isBlank());
    var clientId = seedOidcClientId.map(String::trim).filter(v -> !v.isBlank());
    var clientSecret = seedOidcClientSecret.map(String::trim).filter(v -> !v.isBlank());

    if (issuer.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
      LOG.info(
          "Seed OIDC token not configured; fixture sync may fail in OIDC mode. "
              + "Set floecat.seed.oidc.token or floecat.seed.oidc.issuer/client-id/client-secret.");
      return java.util.Optional.empty();
    }

    String tokenEndpoint =
        issuer.get().endsWith("/")
            ? issuer.get() + "protocol/openid-connect/token"
            : issuer.get() + "/protocol/openid-connect/token";

    String body =
        "client_id="
            + urlEncode(clientId.get())
            + "&client_secret="
            + urlEncode(clientSecret.get())
            + "&grant_type=client_credentials";

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(tokenEndpoint))
            .timeout(seedOidcTimeout)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    return fetchSeedTokenWithRetry(request);
  }

  private java.util.Optional<String> fetchSeedTokenWithRetry(HttpRequest request) {
    long backoffMs = SEED_TOKEN_INITIAL_BACKOFF_MS;
    for (int attempt = 1; attempt <= SEED_TOKEN_MAX_ATTEMPTS; attempt++) {
      var result = fetchSeedTokenOnce(request);
      if (result.isPresent()) {
        return result;
      }
      if (attempt < SEED_TOKEN_MAX_ATTEMPTS) {
        LOG.infof(
            "Retrying seed OIDC token request (attempt %d/%d) after %dms",
            attempt, SEED_TOKEN_MAX_ATTEMPTS, backoffMs);
        LockSupport.parkNanos(backoffMs * 1_000_000L);
        backoffMs = Math.min(backoffMs * 2, SEED_TOKEN_MAX_BACKOFF_MS);
      }
    }
    return java.util.Optional.empty();
  }

  private java.util.Optional<String> fetchSeedTokenOnce(HttpRequest request) {
    try {
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2) {
        LOG.warnf("Seed OIDC token request failed: %d %s", response.statusCode(), response.body());
        return java.util.Optional.empty();
      }
      String token = extractJsonValue(response.body(), "access_token");
      LOG.info("Seed OIDC token acquired for fixture sync.");
      return java.util.Optional.of(withBearerPrefix(token));
    } catch (Exception e) {
      LOG.warn("Seed OIDC token request failed", e);
      return java.util.Optional.empty();
    }
  }

  private static String extractJsonValue(String json, String key) {
    String needle = "\"" + key + "\":\"";
    int start = json.indexOf(needle);
    if (start < 0) {
      throw new IllegalStateException("Missing key " + key + " in response: " + json);
    }
    start += needle.length();
    int end = json.indexOf('"', start);
    if (end < 0) {
      throw new IllegalStateException("Malformed JSON response: " + json);
    }
    return json.substring(start, end);
  }

  private static String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
  }

  private static String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to URL-encode value", e);
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

  private record DeltaFixtureConfig(
      String connectorName,
      String description,
      String tableRoot,
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
