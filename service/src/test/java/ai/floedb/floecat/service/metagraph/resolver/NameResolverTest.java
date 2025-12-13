package ai.floedb.floecat.service.metagraph.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeViewRepository;
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
}
