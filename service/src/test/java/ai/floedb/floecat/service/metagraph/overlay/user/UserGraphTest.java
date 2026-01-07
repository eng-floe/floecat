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

package ai.floedb.floecat.service.metagraph.overlay.user;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.testsupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.FakeViewRepository;
import ai.floedb.floecat.service.testsupport.SecurityTestSupport.FakePrincipalProvider;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserGraphTest {

  FakeCatalogRepository catalogRepository;
  FakeNamespaceRepository namespaceRepository;
  SnapshotTestSupport.FakeSnapshotRepository snapshotRepository;
  FakeTableRepository tableRepository;
  FakeViewRepository viewRepository;
  SnapshotTestSupport.FakeSnapshotClient snapshotClient;
  FakePrincipalProvider principalProvider;
  UserGraph graph;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    snapshotRepository = new SnapshotTestSupport.FakeSnapshotRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();

    // Use the lightweight test-only constructor; it wires up a minimal graph
    graph =
        new UserGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository);

    // Configure fakes that tests rely on
    snapshotClient = new SnapshotTestSupport.FakeSnapshotClient();
    SnapshotHelper helper = new SnapshotHelper(snapshotRepository, null);
    helper.setSnapshotClient(snapshotClient);
    graph.setSnapshotHelper(helper);

    principalProvider = new FakePrincipalProvider("account");
    graph.setPrincipalProvider(principalProvider);
  }

  @Test
  void schemaJsonHelperFallsBackToTable() {
    var ids = seedTable("base-schema", "{}");
    UserTableNode node = graph.table(ids.tableId()).orElseThrow();

    String schema = graph.schemaJsonFor("corr", node, null);

    assertThat(schema).isEqualTo("{}");
  }

  @Test
  void schemaJsonHelperUsesSnapshotOverrides() {
    var ids = seedTable("snap-schema", "{\"fields\":[{\"name\":\"id\"}]}");
    UserTableNode node = graph.table(ids.tableId()).orElseThrow();

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(10L)
            .setSchemaJson("{\"fields\":[{\"name\":\"snap\"}]}")
            .setUpstreamCreatedAt(ts(Instant.parse("2024-01-01T00:00:00Z")))
            .build();
    snapshotRepository.put(snapshot);

    String schema =
        graph.schemaJsonFor("corr", node, SnapshotRef.newBuilder().setSnapshotId(10L).build());

    assertThat(schema).contains("snap");
  }

  @Test
  void schemaJsonHelperSupportsAsOf() {
    var ids = seedTable("asof", "{\"type\":\"struct\"}");
    UserTableNode node = graph.table(ids.tableId()).orElseThrow();

    Snapshot oldSnapshot =
        Snapshot.newBuilder()
            .setSnapshotId(11L)
            .setSchemaJson("{\"old\":true}")
            .setUpstreamCreatedAt(ts(Instant.parse("2024-02-01T00:00:00Z")))
            .build();
    Snapshot newSnapshot =
        Snapshot.newBuilder()
            .setSnapshotId(12L)
            .setSchemaJson("{\"new\":true}")
            .setUpstreamCreatedAt(ts(Instant.parse("2024-03-01T00:00:00Z")))
            .build();
    snapshotRepository.put(oldSnapshot);
    snapshotRepository.put(newSnapshot);

    SnapshotRef ref =
        SnapshotRef.newBuilder().setAsOf(ts(Instant.parse("2024-02-15T00:00:00Z"))).build();

    String schema = graph.schemaJsonFor("corr", node, ref);

    assertThat(schema).contains("old").doesNotContain("new");
  }

  @Test
  void schemaJsonHelperThrowsWhenSnapshotMissing() {
    var ids = seedTable("missing", "{}");
    UserTableNode node = graph.table(ids.tableId()).orElseThrow();
    SnapshotRef ref = SnapshotRef.newBuilder().setSnapshotId(404L).build();

    assertThatThrownBy(() -> graph.schemaJsonFor("cid", node, ref))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void tableResolveCachesNodes() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("account", "tbl", ResourceKind.RK_TABLE);

    MutationMeta meta = mutationMeta(7L, Instant.parse("2024-01-01T00:00:00Z"));
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("sales")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setUri("s3://bucket/sales")
                    .build())
            .build();

    tableRepository.put(table, meta);

    Optional<UserTableNode> first = graph.table(tableId);
    Optional<UserTableNode> second = graph.table(tableId);

    assertThat(first).isPresent();
    assertThat(second).containsSame(first.get());
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(1);

    graph.invalidate(tableId);
    Optional<UserTableNode> third = graph.table(tableId);
    assertThat(third).isPresent();
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(2);
  }

  @Test
  void accountCachesAreIsolated() {
    var accountOne = seedTable("cached-one", "{}");
    var accountTwo = seedTableForAccount("account-b", "catB", "nsB", "cached-two", "{}");

    graph.table(accountOne.tableId());
    graph.table(accountTwo.tableId());

    graph.table(accountOne.tableId());
    graph.table(accountTwo.tableId());

    assertThat(tableRepository.getByIdCount(accountOne.tableId())).isEqualTo(1);
    assertThat(tableRepository.getByIdCount(accountTwo.tableId())).isEqualTo(1);
  }

  @Test
  void invalidateRemovesAllCachedPointerVersions() {
    var ids = seedTable("multi-version", "{}");
    ResourceId tableId = ids.tableId();

    graph.table(tableId);
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(1);

    tableRepository.putMeta(tableId, mutationMeta(2L, Instant.parse("2024-06-01T00:00:00Z")));
    graph.table(tableId);
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(2);

    graph.invalidate(tableId);

    tableRepository.putMeta(tableId, mutationMeta(1L, Instant.parse("2024-06-02T00:00:00Z")));
    graph.table(tableId);
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(3);

    tableRepository.putMeta(tableId, mutationMeta(2L, Instant.parse("2024-06-03T00:00:00Z")));
    graph.table(tableId);
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(4);
  }

  @Test
  void registersCacheGaugesWhenEnabled() {
    var registry = new SimpleMeterRegistry();
    UserGraph instrumentedGraph =
        new UserGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository,
            null, // snapshotStub
            registry,
            principalProvider,
            42L, // cache size
            null // engineHintManager
            );
    SnapshotHelper helperEnabled = new SnapshotHelper(snapshotRepository, null);
    helperEnabled.setSnapshotClient(snapshotClient);
    instrumentedGraph.setSnapshotHelper(helperEnabled);

    assertThat(registry.get("floecat.metadata.graph.cache.enabled").gauge().value()).isEqualTo(1.0);
    assertThat(registry.get("floecat.metadata.graph.cache.max_size").gauge().value())
        .isEqualTo(42.0);
    registry.close();
  }

  @Test
  void registersCacheGaugesWhenDisabled() {
    var registry = new SimpleMeterRegistry();
    UserGraph instrumentedGraph =
        new UserGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository,
            null, // snapshotStub
            registry,
            principalProvider,
            0L, // cache size (disabled)
            null // engineHintManager
            );
    SnapshotHelper helperDisabled = new SnapshotHelper(snapshotRepository, null);
    helperDisabled.setSnapshotClient(snapshotClient);
    instrumentedGraph.setSnapshotHelper(helperDisabled);

    assertThat(registry.get("floecat.metadata.graph.cache.enabled").gauge().value()).isZero();
    assertThat(registry.get("floecat.metadata.graph.cache.max_size").gauge().value()).isZero();
    registry.close();
  }

  @Test
  void resolveCatalogByNameUsesGraph() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("sales").build(),
        mutationMeta(1L, Instant.now()));

    ResourceId resolved = graph.resolveCatalog("corr", "sales");

    assertThat(resolved).isEqualTo(catalogId);
  }

  @Test
  void resolveNamespaceReturnsId() {
    var nsId =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns", ResourceKind.RK_NAMESPACE))
            .setCatalogId(rid("account", "cat", ResourceKind.RK_CATALOG))
            .setDisplayName("ns")
            .build();
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(rid("account", "cat", ResourceKind.RK_CATALOG))
            .setDisplayName("cat")
            .build();
    catalogRepository.put(catalog, mutationMeta(1L, Instant.now()));
    namespaceRepository.put(nsId, mutationMeta(2L, Instant.now()));

    NameRef ref = NameRef.newBuilder().setCatalog("cat").setName("ns").build();

    ResourceId resolved = graph.resolveNamespace("corr", ref);

    assertThat(resolved).isEqualTo(nsId.getResourceId());
  }

  @Test
  void resolveTableUsesGraph() {
    var ids = seedTable("orders", "{\"fields\":[]}");
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders").build();

    ResourceId resolved = graph.resolveTable("corr", ref);

    assertThat(resolved).isEqualTo(ids.tableId());
  }

  @Test
  void resolveViewUsesGraph() {
    var ids = seedView("reports");
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("reports").build();

    ResourceId resolved = graph.resolveView("corr", ref);

    assertThat(resolved).isEqualTo(ids);
  }

  @Test
  void resolveNameReturnsTableWhenOnlyTableExists() {
    var ids = seedTable("only_table", "{}");
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("only_table").build();

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved).isEqualTo(ids.tableId());
  }

  @Test
  void resolveNameReturnsViewWhenOnlyViewExists() {
    ResourceId viewId = seedView("only_view");
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("only_view").build();

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved).isEqualTo(viewId);
  }

  @Test
  void resolveNameThrowsWhenAmbiguous() {
    seedTable("ambiguous", "{}");
    seedView("ambiguous");
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("ambiguous").build();

    assertThatThrownBy(() -> graph.resolveName("corr", ref))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void lookupNameRefsFromIds() {
    var ids = seedTable("acct", "{\"fields\":[]}");

    Optional<NameRef> tableName = graph.tableName(ids.tableId());
    Optional<NameRef> namespaceName = graph.namespaceName(ids.namespaceId());

    assertThat(tableName).isPresent();
    assertThat(tableName.get().getName()).isEqualTo("acct");

    assertThat(namespaceName).isPresent();
    assertThat(namespaceName.get().getName()).isEqualTo("ns");
  }

  @Test
  void catalogWrapperReturnsNode() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    MutationMeta meta = mutationMeta(3L, Instant.parse("2023-06-01T00:00:00Z"));
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(catalogId)
            .setDisplayName("analytics")
            .putProperties("env", "dev")
            .build();

    catalogRepository.put(catalog, meta);

    Optional<CatalogNode> node = graph.catalog(catalogId);
    assertThat(node).isPresent();
    assertThat(node.get().displayName()).isEqualTo("analytics");
  }

  @Test
  void namespaceWrapperReturnsNode() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    MutationMeta meta = mutationMeta(11L, Instant.parse("2024-03-10T10:15:30Z"));
    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("finance")
            .addParents("root")
            .putProperties("region", "us-west-2")
            .build();

    namespaceRepository.put(namespace, meta);

    Optional<NamespaceNode> node = graph.namespace(namespaceId);
    assertThat(node).isPresent();
    assertThat(node.get().catalogId()).isEqualTo(catalogId);
    assertThat(node.get().pathSegments()).containsExactly("root");
  }

  @Test
  void viewWrapperReturnsNode() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId viewId = rid("account", "view", ResourceKind.RK_VIEW);
    MutationMeta meta = mutationMeta(15L, Instant.parse("2024-05-01T05:06:07Z"));
    View view =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("sales_view")
            .setSql("select * from sales")
            .build();

    viewRepository.put(view, meta);

    Optional<ViewNode> node = graph.view(viewId);
    assertThat(node).isPresent();
    assertThat(node.get().sql()).contains("sales");
  }

  @Test
  void resolveReturnsEmptyWhenMetadataMissing() {
    ResourceId missingId = rid("account", "missing", ResourceKind.RK_CATALOG);

    Optional<CatalogNode> node = graph.catalog(missingId);
    assertThat(node).isEmpty();
  }

  @Test
  void resolveReturnsEmptyWhenRepoHasNoResource() {
    ResourceId viewId = rid("account", "view", ResourceKind.RK_VIEW);
    MutationMeta meta = mutationMeta(1L, Instant.parse("2024-02-01T00:00:00Z"));
    viewRepository.putMeta(viewId, meta);

    Optional<ViewNode> node = graph.view(viewId);
    assertThat(node).isEmpty();
  }

  @Test
  void snapshotPinUsesOverrides() {
    ResourceId tableId = rid("account", "tbl-override", ResourceKind.RK_TABLE);
    SnapshotRef override = SnapshotRef.newBuilder().setSnapshotId(42).build();

    SnapshotPin pin = graph.snapshotPinFor("corr", tableId, override, Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(42);

    Timestamp ts = Timestamp.newBuilder().setSeconds(123).build();
    SnapshotRef asOf = SnapshotRef.newBuilder().setAsOf(ts).build();
    SnapshotPin pinTs = graph.snapshotPinFor("corr", tableId, asOf, Optional.empty());
    assertThat(pinTs.hasAsOf()).isTrue();
    assertThat(pinTs.getAsOf()).isEqualTo(ts);
  }

  @Test
  void snapshotPinUsesDefaultAndCurrent() {
    ResourceId tableId = rid("account", "tbl-default", ResourceKind.RK_TABLE);
    Timestamp defaultTs = Timestamp.newBuilder().setSeconds(456).build();

    SnapshotPin defaultPin = graph.snapshotPinFor("corr", tableId, null, Optional.of(defaultTs));
    assertThat(defaultPin.hasAsOf()).isTrue();
    assertThat(defaultPin.getAsOf()).isEqualTo(defaultTs);

    snapshotClient.nextResponse =
        GetSnapshotResponse.newBuilder()
            .setSnapshot(Snapshot.newBuilder().setSnapshotId(9999))
            .build();

    SnapshotPin current = graph.snapshotPinFor("corr", tableId, null, Optional.empty());
    assertThat(current.getSnapshotId()).isEqualTo(9999);
    assertThat(snapshotClient.lastRequest.getSnapshot().getSpecial())
        .isEqualTo(SpecialSnapshot.SS_CURRENT);
  }

  @Test
  void resolveNamePrefersTablesWhenOnlyTableExists() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("tbl").build();
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(rid("account", "cat-id", ResourceKind.RK_CATALOG))
            .setDisplayName("cat")
            .build();
    catalogRepository.put(catalog, mutationMeta(1L, Instant.now()));
    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns-id", ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalog.getResourceId())
            .addAllParents(List.of())
            .setDisplayName("ns")
            .build();
    namespaceRepository.put(namespace, mutationMeta(1L, Instant.now()));
    Table table =
        Table.newBuilder()
            .setResourceId(rid("account", "TBL", ResourceKind.RK_TABLE))
            .setCatalogId(catalog.getResourceId())
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("tbl")
            .setSchemaJson("{}")
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
            .build();
    tableRepository.put(table, mutationMeta(1L, Instant.now()));

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved).isEqualTo(table.getResourceId());
  }

  @Test
  void resolveNameReturnsViewsWhenOnlyViewExists() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("view").build();
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(rid("account", "cat-id", ResourceKind.RK_CATALOG))
            .setDisplayName("cat")
            .build();
    catalogRepository.put(catalog, mutationMeta(1L, Instant.now()));
    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns-id", ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalog.getResourceId())
            .addAllParents(List.of())
            .setDisplayName("ns")
            .build();
    namespaceRepository.put(namespace, mutationMeta(1L, Instant.now()));
    View view =
        View.newBuilder()
            .setResourceId(rid("account", "VIEW", ResourceKind.RK_VIEW))
            .setCatalogId(catalog.getResourceId())
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("view")
            .setSql("select 1")
            .build();
    viewRepository.put(view, mutationMeta(1L, Instant.now()));

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved).isEqualTo(view.getResourceId());
  }

  @Test
  void resolveNameFailsWhenAmbiguous() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("obj").build();
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(rid("account", "cat-id", ResourceKind.RK_CATALOG))
            .setDisplayName("cat")
            .build();
    catalogRepository.put(catalog, mutationMeta(1L, Instant.now()));
    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns-id", ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalog.getResourceId())
            .addAllParents(List.of())
            .setDisplayName("ns")
            .build();
    namespaceRepository.put(namespace, mutationMeta(1L, Instant.now()));
    Table table =
        Table.newBuilder()
            .setResourceId(rid("account", "tbl-id", ResourceKind.RK_TABLE))
            .setCatalogId(catalog.getResourceId())
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("obj")
            .setSchemaJson("{}")
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
            .build();
    tableRepository.put(table, mutationMeta(1L, Instant.now()));
    View view =
        View.newBuilder()
            .setResourceId(rid("account", "view-id", ResourceKind.RK_VIEW))
            .setCatalogId(catalog.getResourceId())
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("obj")
            .setSql("select 1")
            .build();
    viewRepository.put(view, mutationMeta(1L, Instant.now()));

    assertThatThrownBy(() -> graph.resolveName("corr", ref))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void tryResolveTable_returnsTableIdWhenExists() {
    var ids = seedTable("try_resolve_table", "{}");
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("try_resolve_table").build();

    Optional<ResourceId> result = graph.tryResolveTable("corr", ref);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(ids.tableId());
  }

  @Test
  void tryResolveTable_returnsEmptyWhenNotExists() {
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("nonexistent_table").build();

    Optional<ResourceId> result = graph.tryResolveTable("corr", ref);

    assertThat(result).isEmpty();
  }

  @Test
  void tryResolveView_returnsViewIdWhenExists() {
    ResourceId viewId = seedView("try_resolve_view");
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("try_resolve_view").build();

    Optional<ResourceId> result = graph.tryResolveView("corr", ref);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(viewId);
  }

  @Test
  void tryResolveView_returnsEmptyWhenNotExists() {
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("nonexistent_view").build();

    Optional<ResourceId> result = graph.tryResolveView("corr", ref);

    assertThat(result).isEmpty();
  }

  @Test
  void tryResolveNamespace_returnsNamespaceIdWhenExists() {
    ResourceId namespaceId = seedNamespace("try_resolve_ns");
    NameRef ref = NameRef.newBuilder().setCatalog("cat").setName("try_resolve_ns").build();

    Optional<ResourceId> result = graph.tryResolveNamespace("corr", ref);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(namespaceId);
  }

  @Test
  void tryResolveNamespace_returnsEmptyWhenNotExists() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").setName("nonexistent_ns").build();

    Optional<ResourceId> result = graph.tryResolveNamespace("corr", ref);

    assertThat(result).isEmpty();
  }

  @Test
  void tryResolveName_returnsTableIdWhenOnlyTableExists() {
    var ids = seedTable("try_resolve_name_table", "{}");
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog("cat")
            .addPath("ns")
            .setName("try_resolve_name_table")
            .build();

    Optional<ResourceId> result = graph.tryResolveName("corr", ref);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(ids.tableId());
  }

  @Test
  void tryResolveName_returnsViewIdWhenOnlyViewExists() {
    ResourceId viewId = seedView("try_resolve_name_view");
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog("cat")
            .addPath("ns")
            .setName("try_resolve_name_view")
            .build();

    Optional<ResourceId> result = graph.tryResolveName("corr", ref);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(viewId);
  }

  @Test
  void tryResolveName_returnsEmptyWhenAmbiguous() {
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("ambiguous_obj").build();
    // Create both table and view with same name
    seedTable("ambiguous_obj", "{}");
    seedView("ambiguous_obj");

    Optional<ResourceId> result = graph.tryResolveName("corr", ref);

    assertThat(result).isEmpty(); // Should return empty for ambiguous names
  }

  @Test
  void tryResolveName_returnsEmptyWhenNotExists() {
    NameRef ref =
        NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("nonexistent_obj").build();

    Optional<ResourceId> result = graph.tryResolveName("corr", ref);

    assertThat(result).isEmpty();
  }

  @Test
  void resolveTablesListReturnsCanonicalNames() {
    var ids = seedTable("orders_list", "{\"fields\":[]}");
    List<NameRef> inputs =
        List.of(
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_list").build(),
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("missing").build());

    UserGraph.ResolveResult result = graph.resolveTables("corr", inputs, 10, "");

    assertThat(result.totalSize()).isEqualTo(inputs.size());
    assertThat(result.nextToken()).isEmpty();
    assertThat(result.relations()).hasSize(2);
    UserGraph.QualifiedRelation first = result.relations().get(0);
    assertThat(first.resourceId()).isEqualTo(ids.tableId());
    assertThat(first.name().getResourceId()).isEqualTo(ids.tableId());
    UserGraph.QualifiedRelation second = result.relations().get(1);
    assertThat(second.resourceId()).isEqualTo(ResourceId.getDefaultInstance());
  }

  @Test
  void resolveTablesPrefixSupportsPaging() {
    seedTable("orders_a", "{}");
    seedTable("orders_b", "{}");
    seedTable("orders_c", "{}");

    NameRef prefix = NameRef.newBuilder().setCatalog("cat").addPath("ns").build();

    UserGraph.ResolveResult first = graph.resolveTables("corr", prefix, 2, "");
    assertThat(first.relations()).hasSize(2);
    assertThat(first.totalSize()).isEqualTo(3);
    assertThat(first.nextToken()).isNotBlank();

    UserGraph.ResolveResult second = graph.resolveTables("corr", prefix, 2, first.nextToken());
    assertThat(second.relations()).hasSize(1);
    assertThat(second.totalSize()).isEqualTo(3);
    assertThat(second.nextToken()).isBlank();

    assertThatThrownBy(() -> graph.resolveTables("corr", prefix, 2, "bad-token"))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void resolveTablesPrefixAcceptsNamespaceNameInRef() {
    ResourceId catalogId = rid("account", "cat-nested", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat_nested").build(),
        mutationMeta(1L, Instant.now()));

    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns-nested", ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalogId)
            .addAllParents(List.of("finance"))
            .setDisplayName("sales")
            .build();
    namespaceRepository.put(namespace, mutationMeta(1L, Instant.now()));

    ResourceId tableId = rid("account", "tbl-nested", ResourceKind.RK_TABLE);
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("orders_nested")
            .setSchemaJson("{}")
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
            .build(),
        mutationMeta(1L, Instant.now()));

    NameRef prefix =
        NameRef.newBuilder().setCatalog("cat_nested").addPath("finance").setName("sales").build();

    UserGraph.ResolveResult result = graph.resolveTables("corr", prefix, 5, "");

    assertThat(result.totalSize()).isEqualTo(1);
    assertThat(result.relations()).hasSize(1);
    UserGraph.QualifiedRelation relation = result.relations().get(0);
    assertThat(relation.resourceId()).isEqualTo(tableId);
    assertThat(relation.name().getPathList()).containsExactly("finance", "sales");
    assertThat(relation.name().getName()).isEqualTo("orders_nested");
  }

  @Test
  void resolveViewsListHandlesMissingEntries() {
    ResourceId viewId = seedView("reports_list");
    List<NameRef> inputs =
        List.of(
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("reports_list").build(),
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("missing_view").build());

    UserGraph.ResolveResult result = graph.resolveViews("corr", inputs, 50, "");

    assertThat(result.totalSize()).isEqualTo(inputs.size());
    assertThat(result.relations()).hasSize(2);
    assertThat(result.relations().get(0).resourceId()).isEqualTo(viewId);
    assertThat(result.relations().get(1).resourceId()).isEqualTo(ResourceId.getDefaultInstance());
  }

  @Test
  void resolveViewsPrefixRejectsBadToken() {
    seedView("reports_prefix");
    NameRef prefix = NameRef.newBuilder().setCatalog("cat").addPath("ns").build();

    UserGraph.ResolveResult page = graph.resolveViews("corr", prefix, 1, "");
    assertThat(page.relations()).hasSize(1);

    assertThatThrownBy(() -> graph.resolveViews("corr", prefix, 1, "unknown-token"))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void resolveViewsPrefixAcceptsNamespaceNameInRef() {
    ResourceId catalogId = rid("account", "cat-view-nested", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat_view_nested").build(),
        mutationMeta(1L, Instant.now()));

    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(rid("account", "ns-view-nested", ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalogId)
            .addAllParents(List.of("finance"))
            .setDisplayName("sales")
            .build();
    namespaceRepository.put(namespace, mutationMeta(1L, Instant.now()));

    ResourceId viewId = rid("account", "view-nested", ResourceKind.RK_VIEW);
    viewRepository.put(
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName("reports_nested")
            .setSql("select 1")
            .build(),
        mutationMeta(1L, Instant.now()));

    NameRef prefix =
        NameRef.newBuilder()
            .setCatalog("cat_view_nested")
            .addPath("finance")
            .setName("sales")
            .build();

    UserGraph.ResolveResult result = graph.resolveViews("corr", prefix, 5, "");

    assertThat(result.totalSize()).isEqualTo(1);
    assertThat(result.relations()).hasSize(1);
    UserGraph.QualifiedRelation relation = result.relations().get(0);
    assertThat(relation.resourceId()).isEqualTo(viewId);
    assertThat(relation.name().getPathList()).containsExactly("finance", "sales");
    assertThat(relation.name().getName()).isEqualTo("reports_nested");
  }

  // ----------------------------------------------------------------------
  // Test Helpers
  // ----------------------------------------------------------------------

  private static MutationMeta mutationMeta(long version, Instant updatedAt) {
    Timestamp ts =
        Timestamp.newBuilder()
            .setSeconds(updatedAt.getEpochSecond())
            .setNanos(updatedAt.getNano())
            .build();
    return MutationMeta.newBuilder().setPointerVersion(version).setUpdatedAt(ts).build();
  }

  private static ResourceId rid(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }

  private static Timestamp ts(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private TableIds seedTable(String name, String schemaJson) {
    return seedTableForAccount("account", "cat", "ns", name, schemaJson);
  }

  private TableIds seedTableForAccount(
      String accountId,
      String catalogName,
      String namespaceName,
      String tableName,
      String schemaJson) {
    ResourceId catalogId = rid(accountId, catalogName, ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid(accountId, namespaceName, ResourceKind.RK_NAMESPACE);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("table-" + tableName)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName(catalogName).build(),
        mutationMeta(1L, Instant.now()));
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName(namespaceName)
            .build(),
        mutationMeta(1L, Instant.now()));
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(tableName)
            .setSchemaJson(schemaJson)
            .setUpstream(
                UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).setUri("s3://x").build())
            .build(),
        mutationMeta(1L, Instant.now()));
    return new TableIds(catalogId, namespaceId, tableId);
  }

  private ResourceId seedView(String name) {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId viewId = rid("account", "view-" + name, ResourceKind.RK_VIEW);

    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat").build(),
        mutationMeta(1L, Instant.now()));
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns")
            .build(),
        mutationMeta(1L, Instant.now()));
    viewRepository.put(
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(name)
            .setSql("select 1")
            .build(),
        mutationMeta(1L, Instant.now()));

    return viewId;
  }

  private ResourceId seedNamespace(String name) {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "ns-" + name, ResourceKind.RK_NAMESPACE);

    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat").build(),
        mutationMeta(1L, Instant.now()));
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName(name)
            .build(),
        mutationMeta(1L, Instant.now()));

    return namespaceId;
  }

  record TableIds(ResourceId catalogId, ResourceId namespaceId, ResourceId tableId) {}
}
