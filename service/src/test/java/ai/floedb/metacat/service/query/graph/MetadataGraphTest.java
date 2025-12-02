package ai.floedb.metacat.service.query.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.ResolveViewRequest;
import ai.floedb.metacat.catalog.rpc.ResolveViewResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.storage.InMemoryBlobStore;
import ai.floedb.metacat.storage.InMemoryPointerStore;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataGraphTest {

  FakeCatalogRepository catalogRepository;
  FakeNamespaceRepository namespaceRepository;
  FakeTableRepository tableRepository;
  FakeViewRepository viewRepository;
  FakeSnapshotClient snapshotClient;
  FakeDirectoryClient directoryClient;
  MetadataGraph graph;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();
    graph =
        new MetadataGraph(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    snapshotClient = new FakeSnapshotClient();
    graph.setSnapshotClient(snapshotClient);
    directoryClient = new FakeDirectoryClient();
    graph.setDirectoryClient(directoryClient);
  }

  @Test
  void tableResolveCachesNodes() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("tenant", "tbl", ResourceKind.RK_TABLE);

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
  void catalogWrapperReturnsNode() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
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
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
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
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId viewId = rid("tenant", "view", ResourceKind.RK_VIEW);
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
    ResourceId missingId = rid("tenant", "missing", ResourceKind.RK_CATALOG);

    Optional<CatalogNode> node = graph.catalog(missingId);
    assertThat(node).isEmpty();
  }

  @Test
  void resolveReturnsEmptyWhenRepoHasNoResource() {
    ResourceId viewId = rid("tenant", "view", ResourceKind.RK_VIEW);
    MutationMeta meta = mutationMeta(1L, Instant.parse("2024-02-01T00:00:00Z"));
    viewRepository.putMeta(viewId, meta);

    Optional<ViewNode> node = graph.view(viewId);
    assertThat(node).isEmpty();
  }

  @Test
  void snapshotPinUsesOverrides() {
    ResourceId tableId = rid("tenant", "tbl-override", ResourceKind.RK_TABLE);
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
    ResourceId tableId = rid("tenant", "tbl-default", ResourceKind.RK_TABLE);
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

  private static ResourceId rid(String tenant, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setTenantId(tenant).setId(id).setKind(kind).build();
  }

  @Test
  void resolveNamePrefersTablesWhenOnlyTableExists() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("tbl").build();
    directoryClient.tables.put(ref, rid("tenant", "tbl-id", ResourceKind.RK_TABLE));

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved.getId()).isEqualTo("tbl-id");
  }

  @Test
  void resolveNameReturnsViewsWhenOnlyViewExists() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("view").build();
    directoryClient.views.put(ref, rid("tenant", "view-id", ResourceKind.RK_VIEW));

    ResourceId resolved = graph.resolveName("corr", ref);

    assertThat(resolved.getId()).isEqualTo("view-id");
  }

  @Test
  void resolveNameFailsWhenAmbiguous() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("obj").build();
    directoryClient.tables.put(ref, rid("tenant", "tbl-id", ResourceKind.RK_TABLE));
    directoryClient.views.put(ref, rid("tenant", "view-id", ResourceKind.RK_VIEW));

    assertThatThrownBy(() -> graph.resolveName("corr", ref))
        .isInstanceOf(StatusRuntimeException.class);
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
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing namespace meta");
      }
      return meta;
    }
  }

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
    public MutationMeta metaForSafe(ResourceId id) {
      MutationMeta meta = metas.get(id);
      if (meta == null) {
        throw new StorageNotFoundException("missing view meta");
      }
      return meta;
    }
  }

  static final class FakeSnapshotClient implements MetadataGraph.SnapshotClient {
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

  static final class FakeDirectoryClient implements MetadataGraph.DirectoryClient {
    final Map<NameRef, ResourceId> tables = new HashMap<>();
    final Map<NameRef, ResourceId> views = new HashMap<>();

    @Override
    public ResolveTableResponse resolveTable(ResolveTableRequest request) {
      ResourceId rid = tables.get(request.getRef());
      if (rid == null) {
        throw new RuntimeException("table not found");
      }
      return ResolveTableResponse.newBuilder().setResourceId(rid).build();
    }

    @Override
    public ResolveViewResponse resolveView(ResolveViewRequest request) {
      ResourceId rid = views.get(request.getRef());
      if (rid == null) {
        throw new RuntimeException("view not found");
      }
      return ResolveViewResponse.newBuilder().setResourceId(rid).build();
    }
  }
}
