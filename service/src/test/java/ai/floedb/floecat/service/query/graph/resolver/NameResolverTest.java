package ai.floedb.floecat.service.query.graph.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NameResolverTest {

  private FakeCatalogRepository catalogRepository;
  private FakeNamespaceRepository namespaceRepository;
  private FakeTableRepository tableRepository;
  private FakeViewRepository viewRepository;
  private NameResolver resolver;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();
    resolver =
        new NameResolver(catalogRepository, namespaceRepository, tableRepository, viewRepository);

    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat").build());

    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns")
            .build());

    ResourceId tableId = rid("account", "tbl", ResourceKind.RK_TABLE);
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .setSchemaJson("{}")
            .build());

    ResourceId viewId = rid("account", "view", ResourceKind.RK_VIEW);
    viewRepository.put(
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_v")
            .setSql("select 1")
            .build());
  }

  @Test
  void resolveCatalogIdReturnsResource() {
    ResourceId resolved = resolver.resolveCatalogId("corr", "account", "cat");
    assertThat(resolved.getId()).isEqualTo("cat");
  }

  @Test
  void resolveNamespaceIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").setName("ns").build();
    ResourceId resolved = resolver.resolveNamespaceId("corr", "account", ref);
    assertThat(resolved.getId()).isEqualTo("ns");
  }

  @Test
  void resolveTableIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders").build();
    ResourceId resolved = resolver.resolveTableId("corr", "account", ref);
    assertThat(resolved.getId()).isEqualTo("tbl");
  }

  @Test
  void resolveViewIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_v").build();
    ResourceId resolved = resolver.resolveViewId("corr", "account", ref);
    assertThat(resolved.getId()).isEqualTo("view");
  }

  private static ResourceId rid(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }

  static final class FakeCatalogRepository extends CatalogRepository {
    private final Map<ResourceId, Catalog> entries = new HashMap<>();

    FakeCatalogRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Catalog catalog) {
      entries.put(catalog.getResourceId(), catalog);
    }

    @Override
    public Optional<Catalog> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Catalog> getByName(String accountId, String displayName) {
      return entries.values().stream()
          .filter(
              cat ->
                  accountId.equals(cat.getResourceId().getAccountId())
                      && displayName.equals(cat.getDisplayName()))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }

  static final class FakeNamespaceRepository extends NamespaceRepository {
    private final Map<ResourceId, Namespace> entries = new HashMap<>();

    FakeNamespaceRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Namespace namespace) {
      entries.put(namespace.getResourceId(), namespace);
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
                      && matches(ns, path))
          .findFirst();
    }

    private boolean matches(Namespace namespace, List<String> path) {
      if (path.isEmpty()) {
        return namespace.getDisplayName().isBlank() && namespace.getParentsList().isEmpty();
      }
      List<String> parents = path.subList(0, path.size() - 1);
      String name = path.get(path.size() - 1);
      return parents.equals(namespace.getParentsList()) && name.equals(namespace.getDisplayName());
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }

  static final class FakeTableRepository extends TableRepository {
    private final Map<ResourceId, Table> entries = new HashMap<>();

    FakeTableRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(Table table) {
      entries.put(table.getResourceId(), table);
    }

    @Override
    public Optional<Table> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Table> getByName(
        String accountId, String catalogId, String namespaceId, String displayName) {
      return entries.values().stream()
          .filter(
              tbl ->
                  accountId.equals(tbl.getResourceId().getAccountId())
                      && catalogId.equals(tbl.getCatalogId().getId())
                      && namespaceId.equals(tbl.getNamespaceId().getId())
                      && displayName.equals(tbl.getDisplayName()))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }

  static final class FakeViewRepository extends ViewRepository {
    private final Map<ResourceId, View> entries = new HashMap<>();

    FakeViewRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(View view) {
      entries.put(view.getResourceId(), view);
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
              vw ->
                  accountId.equals(vw.getResourceId().getAccountId())
                      && catalogId.equals(vw.getCatalogId().getId())
                      && namespaceId.equals(vw.getNamespaceId().getId())
                      && displayName.equals(vw.getDisplayName()))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }
}
