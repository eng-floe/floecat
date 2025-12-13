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
import java.util.List;
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
    var page2 = resolver.resolveTablesByPrefix("corr", "account", prefix, 1, page1.nextToken());

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
}
