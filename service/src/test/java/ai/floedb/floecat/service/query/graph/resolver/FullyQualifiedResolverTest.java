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

class FullyQualifiedResolverTest {

  private FakeCatalogRepository catalogRepository;
  private FakeNamespaceRepository namespaceRepository;
  private FakeTableRepository tableRepository;
  private FakeViewRepository viewRepository;
  private FullyQualifiedResolver resolver;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();
    resolver =
        new FullyQualifiedResolver(
            catalogRepository, namespaceRepository, tableRepository, viewRepository);

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

    tableRepository.put(
        Table.newBuilder()
            .setResourceId(rid("account", "tbl1", ResourceKind.RK_TABLE))
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_a")
            .setSchemaJson("{}")
            .build());
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(rid("account", "tbl2", ResourceKind.RK_TABLE))
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_b")
            .setSchemaJson("{}")
            .build());

    viewRepository.put(
        View.newBuilder()
            .setResourceId(rid("account", "view1", ResourceKind.RK_VIEW))
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .setSql("select 1")
            .build());
  }

  @Test
  void resolveTableListCanonicalizesNames() {
    List<NameRef> refs =
        List.of(
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_a").build(),
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("missing").build());

    var result = resolver.resolveTableList("corr", "account", refs, 10, "");

    assertThat(result.totalSize()).isEqualTo(2);
    assertThat(result.relations().get(0).name().getResourceId().getId()).isEqualTo("tbl1");
    assertThat(result.relations().get(1).resourceId()).isEqualTo(ResourceId.getDefaultInstance());
  }

  @Test
  void resolveTablesByPrefixPaginates() {
    NameRef prefix = NameRef.newBuilder().setCatalog("cat").addPath("ns").build();

    var page1 = resolver.resolveTablesByPrefix("corr", "account", prefix, 1, "");
    var page2 = resolver.resolveTablesByPrefix("corr", "account", prefix, 1, page1.nextPageToken());

    assertThat(page1.relations()).hasSize(1);
    assertThat(page2.relations()).hasSize(1);
    assertThat(page1.relations().get(0).name().getName()).isEqualTo("orders_a");
    assertThat(page2.relations().get(0).name().getName()).isEqualTo("orders_b");
  }

  @Test
  void resolveViewListReturnsCanonicalNames() {
    List<NameRef> refs =
        List.of(
            NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_view").build());

    var result = resolver.resolveViewList("corr", "account", refs, 10, "");

    assertThat(result.totalSize()).isEqualTo(1);
    assertThat(result.relations().get(0).name().getResourceId().getId()).isEqualTo("view1");
  }

  private static ResourceId rid(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }

  // Reuse in-memory repos identical to those in NameResolverTest
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
    public List<Table> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String pageToken,
        StringBuilder nextOut) {
      List<Table> sorted =
          entries.values().stream()
              .filter(
                  tbl ->
                      accountId.equals(tbl.getResourceId().getAccountId())
                          && catalogId.equals(tbl.getCatalogId().getId())
                          && namespaceId.equals(tbl.getNamespaceId().getId()))
              .sorted(java.util.Comparator.comparing(Table::getDisplayName))
              .toList();
      int start = 0;
      if (pageToken != null && !pageToken.isBlank()) {
        for (int i = 0; i < sorted.size(); i++) {
          if (sorted.get(i).getDisplayName().equals(pageToken)) {
            start = i + 1;
            break;
          }
        }
        if (start == 0) {
          throw new IllegalArgumentException("bad token");
        }
      }
      int end = Math.min(sorted.size(), start + Math.max(1, limit));
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return sorted.subList(start, end);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return (int)
          entries.values().stream()
              .filter(
                  tbl ->
                      accountId.equals(tbl.getResourceId().getAccountId())
                          && catalogId.equals(tbl.getCatalogId().getId())
                          && namespaceId.equals(tbl.getNamespaceId().getId()))
              .count();
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
    public List<View> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String pageToken,
        StringBuilder nextOut) {
      List<View> sorted =
          entries.values().stream()
              .filter(
                  vw ->
                      accountId.equals(vw.getResourceId().getAccountId())
                          && catalogId.equals(vw.getCatalogId().getId())
                          && namespaceId.equals(vw.getNamespaceId().getId()))
              .sorted(java.util.Comparator.comparing(View::getDisplayName))
              .toList();
      int start = 0;
      if (pageToken != null && !pageToken.isBlank()) {
        for (int i = 0; i < sorted.size(); i++) {
          if (sorted.get(i).getDisplayName().equals(pageToken)) {
            start = i + 1;
            break;
          }
        }
        if (start == 0) {
          throw new IllegalArgumentException("bad token");
        }
      }
      int end = Math.min(sorted.size(), start + Math.max(1, limit));
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return sorted.subList(start, end);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return (int)
          entries.values().stream()
              .filter(
                  vw ->
                      accountId.equals(vw.getResourceId().getAccountId())
                          && catalogId.equals(vw.getCatalogId().getId())
                          && namespaceId.equals(vw.getNamespaceId().getId()))
              .count();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }
}
