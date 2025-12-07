package ai.floedb.floecat.service.query.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.query.graph.model.CatalogNode;
import ai.floedb.floecat.service.query.graph.model.NamespaceNode;
import ai.floedb.floecat.service.query.graph.model.TableNode;
import ai.floedb.floecat.service.query.graph.model.ViewNode;
import ai.floedb.floecat.service.query.graph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataGraphTest {

  FakeCatalogRepository catalogRepository;
  FakeNamespaceRepository namespaceRepository;
  FakeSnapshotRepository snapshotRepository;
  FakeTableRepository tableRepository;
  FakeViewRepository viewRepository;
  FakeSnapshotClient snapshotClient;
  FakePrincipalProvider principalProvider;
  MetadataGraph graph;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    snapshotRepository = new FakeSnapshotRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();
    graph =
        new MetadataGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository);
    snapshotClient = new FakeSnapshotClient();
    graph.setSnapshotClient(snapshotClient);
    principalProvider = new FakePrincipalProvider("account");
    graph.setPrincipalProvider(principalProvider);
  }

  @Test
  void schemaJsonHelperFallsBackToTable() {
    var ids = seedTable("base-schema", "{}");
    TableNode node = graph.table(ids.tableId()).orElseThrow();

    String schema = graph.schemaJsonFor("corr", node, null);

    assertThat(schema).isEqualTo("{}");
  }

  @Test
  void schemaJsonHelperUsesSnapshotOverrides() {
    var ids = seedTable("snap-schema", "{\"fields\":[{\"name\":\"id\"}]}");
    TableNode node = graph.table(ids.tableId()).orElseThrow();

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
    TableNode node = graph.table(ids.tableId()).orElseThrow();

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
    TableNode node = graph.table(ids.tableId()).orElseThrow();
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

    Optional<TableNode> first = graph.table(tableId);
    Optional<TableNode> second = graph.table(tableId);

    assertThat(first).isPresent();
    assertThat(second).containsSame(first.get());
    assertThat(tableRepository.getByIdCount(tableId)).isEqualTo(1);

    graph.invalidate(tableId);
    Optional<TableNode> third = graph.table(tableId);
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
    MetadataGraph instrumentedGraph =
        new MetadataGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository,
            null,
            registry,
            principalProvider,
            42L,
            null,
            null);
    instrumentedGraph.setSnapshotClient(snapshotClient);

    assertThat(registry.get("floecat.metadata.graph.cache.enabled").gauge().value()).isEqualTo(1.0);
    assertThat(registry.get("floecat.metadata.graph.cache.max_size").gauge().value())
        .isEqualTo(42.0);
    registry.close();
  }

  @Test
  void registersCacheGaugesWhenDisabled() {
    var registry = new SimpleMeterRegistry();
    MetadataGraph instrumentedGraph =
        new MetadataGraph(
            catalogRepository,
            namespaceRepository,
            snapshotRepository,
            tableRepository,
            viewRepository,
            null,
            registry,
            principalProvider,
            0L,
            null,
            null);
    instrumentedGraph.setSnapshotClient(snapshotClient);

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
    assertThat(tableName.get().getCatalog()).isEqualTo("cat");
    assertThat(tableName.get().getPathList()).containsExactly("ns");

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
  void resolveTablesListReturnsCanonicalNames() {
    var ids = seedTable("orders_list", "{\"fields\":[]}");
    List<NameRef> inputs =
        List.of(
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_list").build(),
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("missing").build());

    MetadataGraph.ResolveResult result = graph.resolveTables("corr", inputs, 10, "");

    assertThat(result.totalSize()).isEqualTo(inputs.size());
    assertThat(result.nextPageToken()).isEmpty();
    assertThat(result.relations()).hasSize(2);
    MetadataGraph.QualifiedRelation first = result.relations().get(0);
    assertThat(first.resourceId()).isEqualTo(ids.tableId());
    assertThat(first.name().getResourceId()).isEqualTo(ids.tableId());
    MetadataGraph.QualifiedRelation second = result.relations().get(1);
    assertThat(second.resourceId()).isEqualTo(ResourceId.getDefaultInstance());
  }

  @Test
  void resolveTablesPrefixSupportsPaging() {
    seedTable("orders_a", "{}");
    seedTable("orders_b", "{}");
    seedTable("orders_c", "{}");

    NameRef prefix = NameRef.newBuilder().setCatalog("cat").addPath("ns").build();

    MetadataGraph.ResolveResult first = graph.resolveTables("corr", prefix, 2, "");
    assertThat(first.relations()).hasSize(2);
    assertThat(first.totalSize()).isEqualTo(3);
    assertThat(first.nextPageToken()).isNotBlank();

    MetadataGraph.ResolveResult second =
        graph.resolveTables("corr", prefix, 2, first.nextPageToken());
    assertThat(second.relations()).hasSize(1);
    assertThat(second.totalSize()).isEqualTo(3);
    assertThat(second.nextPageToken()).isBlank();

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

    MetadataGraph.ResolveResult result = graph.resolveTables("corr", prefix, 5, "");

    assertThat(result.totalSize()).isEqualTo(1);
    assertThat(result.relations()).hasSize(1);
    MetadataGraph.QualifiedRelation relation = result.relations().get(0);
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

    MetadataGraph.ResolveResult result = graph.resolveViews("corr", inputs, 50, "");

    assertThat(result.totalSize()).isEqualTo(inputs.size());
    assertThat(result.relations()).hasSize(2);
    assertThat(result.relations().get(0).resourceId()).isEqualTo(viewId);
    assertThat(result.relations().get(1).resourceId()).isEqualTo(ResourceId.getDefaultInstance());
  }

  @Test
  void resolveViewsPrefixRejectsBadToken() {
    seedView("reports_prefix");
    NameRef prefix = NameRef.newBuilder().setCatalog("cat").addPath("ns").build();

    MetadataGraph.ResolveResult page = graph.resolveViews("corr", prefix, 1, "");
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

    MetadataGraph.ResolveResult result = graph.resolveViews("corr", prefix, 5, "");

    assertThat(result.totalSize()).isEqualTo(1);
    assertThat(result.relations()).hasSize(1);
    MetadataGraph.QualifiedRelation relation = result.relations().get(0);
    assertThat(relation.resourceId()).isEqualTo(viewId);
    assertThat(relation.name().getPathList()).containsExactly("finance", "sales");
    assertThat(relation.name().getName()).isEqualTo("reports_nested");
  }

  // ----------------------------------------------------------------------
  // Test doubles
  // ----------------------------------------------------------------------

  static final class FakeCatalogRepository extends CatalogRepository {
    private final Map<ResourceId, Catalog> entries = new HashMap<>();
    private final Map<ResourceId, MutationMeta> metas = new HashMap<>();
    private final Map<ResourceId, Integer> gets = new HashMap<>();

    FakeCatalogRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Catalog catalog, MutationMeta meta) {
      entries.put(catalog.getResourceId(), catalog);
      metas.put(catalog.getResourceId(), meta);
    }

    void putMeta(ResourceId id, MutationMeta meta) {
      metas.put(id, meta);
    }

    @Override
    public Optional<Catalog> getById(ResourceId id) {
      gets.merge(id, 1, Integer::sum);
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Catalog> getByName(String accountId, String displayName) {
      return entries.values().stream()
          .filter(
              c ->
                  accountId.equals(c.getResourceId().getAccountId())
                      && displayName.equals(c.getDisplayName()))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing catalog meta");
      }
      return meta;
    }

    int getByIdCount(ResourceId id) {
      return gets.getOrDefault(id, 0);
    }
  }

  static final class FakeNamespaceRepository extends NamespaceRepository {
    private final Map<ResourceId, Namespace> entries = new HashMap<>();
    private final Map<ResourceId, MutationMeta> metas = new HashMap<>();

    FakeNamespaceRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Namespace namespace, MutationMeta meta) {
      entries.put(namespace.getResourceId(), namespace);
      metas.put(namespace.getResourceId(), meta);
    }

    @Override
    public Optional<Namespace> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Namespace> getByPath(String accountId, String catalogId, List<String> path) {
      return entries.values().stream()
          .filter(
              ns ->
                  accountId.equals(ns.getResourceId().getAccountId())
                      && catalogId.equals(ns.getCatalogId().getId())
                      && matchesPath(ns, path))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing namespace meta");
      }
      return meta;
    }

    private boolean matchesPath(Namespace namespace, List<String> path) {
      if (path.isEmpty()) {
        return namespace.getDisplayName().isBlank() && namespace.getParentsCount() == 0;
      }
      List<String> parents = path.subList(0, path.size() - 1);
      String name = path.get(path.size() - 1);
      return parents.equals(namespace.getParentsList()) && name.equals(namespace.getDisplayName());
    }
  }

  static final class FakeSnapshotRepository extends SnapshotRepository {
    private final Map<Long, Snapshot> byId = new HashMap<>();

    FakeSnapshotRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Snapshot snapshot) {
      byId.put(snapshot.getSnapshotId(), snapshot);
    }

    @Override
    public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
      return Optional.ofNullable(byId.get(snapshotId));
    }

    @Override
    public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
      return byId.values().stream().max(Comparator.comparingLong(this::createdMillis));
    }

    @Override
    public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
      long target = asOf.getSeconds() * 1000L + asOf.getNanos() / 1_000_000L;
      return byId.values().stream()
          .filter(s -> createdMillis(s) <= target)
          .max(Comparator.comparingLong(this::createdMillis));
    }

    private long createdMillis(Snapshot snapshot) {
      Timestamp ts = snapshot.getUpstreamCreatedAt();
      return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    }
  }

  record TableIds(ResourceId catalogId, ResourceId namespaceId, ResourceId tableId) {}

  static final class FakeTableRepository extends TableRepository {
    private final Map<ResourceId, Table> entries = new HashMap<>();
    private final Map<ResourceId, MutationMeta> metas = new HashMap<>();
    private final Map<ResourceId, Integer> gets = new HashMap<>();

    FakeTableRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Table table, MutationMeta meta) {
      entries.put(table.getResourceId(), table);
      metas.put(table.getResourceId(), meta);
    }

    void putMeta(ResourceId id, MutationMeta meta) {
      metas.put(id, meta);
    }

    @Override
    public Optional<Table> getById(ResourceId id) {
      gets.merge(id, 1, Integer::sum);
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Table> getByName(
        String accountId, String catalogId, String namespaceId, String displayName) {
      return entries.values().stream()
          .filter(
              t ->
                  accountId.equals(t.getResourceId().getAccountId())
                      && catalogId.equals(t.getCatalogId().getId())
                      && namespaceId.equals(t.getNamespaceId().getId())
                      && displayName.equals(t.getDisplayName()))
          .findFirst();
    }

    @Override
    public List<Table> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String pageToken,
        StringBuilder nextOut) {
      List<Table> sorted = matchingTables(accountId, catalogId, namespaceId);
      int start = startIndexForToken(sorted, pageToken);
      int want = Math.max(1, limit);
      int end = Math.min(sorted.size(), start + want);
      List<Table> slice = new ArrayList<>(sorted.subList(start, end));
      nextOut.setLength(0);
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return slice;
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return matchingTables(accountId, catalogId, namespaceId).size();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing table meta");
      }
      return meta;
    }

    int getByIdCount(ResourceId id) {
      return gets.getOrDefault(id, 0);
    }

    private List<Table> matchingTables(String accountId, String catalogId, String namespaceId) {
      return entries.values().stream()
          .filter(
              t ->
                  accountId.equals(t.getResourceId().getAccountId())
                      && catalogId.equals(t.getCatalogId().getId())
                      && namespaceId.equals(t.getNamespaceId().getId()))
          .sorted(Comparator.comparing(Table::getDisplayName))
          .toList();
    }

    private int startIndexForToken(List<Table> sorted, String token) {
      if (token == null || token.isBlank()) {
        return 0;
      }
      for (int i = 0; i < sorted.size(); i++) {
        if (sorted.get(i).getDisplayName().equals(token)) {
          return i + 1;
        }
      }
      throw new IllegalArgumentException("bad token");
    }
  }

  static final class FakeViewRepository extends ViewRepository {
    private final Map<ResourceId, View> entries = new HashMap<>();
    private final Map<ResourceId, MutationMeta> metas = new HashMap<>();

    FakeViewRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(View view, MutationMeta meta) {
      entries.put(view.getResourceId(), view);
      metas.put(view.getResourceId(), meta);
    }

    void putMeta(ResourceId id, MutationMeta meta) {
      metas.put(id, meta);
    }

    @Override
    public Optional<View> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<View> getByName(
        String accountId, String catalogId, String namespaceId, String displayName) {
      return entries.values().stream()
          .filter(
              v ->
                  accountId.equals(v.getResourceId().getAccountId())
                      && catalogId.equals(v.getCatalogId().getId())
                      && namespaceId.equals(v.getNamespaceId().getId())
                      && displayName.equals(v.getDisplayName()))
          .findFirst();
    }

    @Override
    public List<View> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String pageToken,
        StringBuilder nextOut) {
      List<View> sorted = matchingViews(accountId, catalogId, namespaceId);
      int start = startIndexForToken(sorted, pageToken);
      int want = Math.max(1, limit);
      int end = Math.min(sorted.size(), start + want);
      List<View> slice = new ArrayList<>(sorted.subList(start, end));
      nextOut.setLength(0);
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return slice;
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return matchingViews(accountId, catalogId, namespaceId).size();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing view meta");
      }
      return meta;
    }

    private List<View> matchingViews(String accountId, String catalogId, String namespaceId) {
      return entries.values().stream()
          .filter(
              v ->
                  accountId.equals(v.getResourceId().getAccountId())
                      && catalogId.equals(v.getCatalogId().getId())
                      && namespaceId.equals(v.getNamespaceId().getId()))
          .sorted(Comparator.comparing(View::getDisplayName))
          .toList();
    }

    private int startIndexForToken(List<View> sorted, String token) {
      if (token == null || token.isBlank()) {
        return 0;
      }
      for (int i = 0; i < sorted.size(); i++) {
        if (sorted.get(i).getDisplayName().equals(token)) {
          return i + 1;
        }
      }
      throw new IllegalArgumentException("bad token");
    }
  }

  static final class FakeSnapshotClient implements SnapshotHelper.SnapshotClient {
    GetSnapshotResponse nextResponse;
    GetSnapshotRequest lastRequest;

    @Override
    public GetSnapshotResponse getSnapshot(GetSnapshotRequest request) {
      lastRequest = request;
      if (nextResponse == null) {
        throw new IllegalStateException("no snapshot response configured");
      }
      return nextResponse;
    }
  }

  static final class FakePrincipalProvider extends PrincipalProvider {
    private PrincipalContext ctx;

    FakePrincipalProvider(String accountId) {
      ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    void setAccount(String accountId) {
      ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    @Override
    public PrincipalContext get() {
      return ctx;
    }
  }
}
