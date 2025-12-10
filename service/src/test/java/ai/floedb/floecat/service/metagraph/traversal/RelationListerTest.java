package ai.floedb.floecat.service.metagraph.traversal;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectResolver;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.service.metagraph.loader.NodeLoader;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RelationListerTest {

  private static final String ACCOUNT_ID = "account";

  private PointerStore pointerStore;
  private BlobStore blobStore;
  private CatalogRepository catalogRepository;
  private NamespaceRepository namespaceRepository;
  private TableRepository tableRepository;
  private ViewRepository viewRepository;
  private NameResolver names;
  private NodeLoader nodes;
  private RelationLister lister;

  @BeforeEach
  void setUp() {
    pointerStore = new InMemoryPointerStore();
    blobStore = new InMemoryBlobStore();
    catalogRepository = new CatalogRepository(pointerStore, blobStore);
    namespaceRepository = new NamespaceRepository(pointerStore, blobStore);
    tableRepository = new TableRepository(pointerStore, blobStore);
    viewRepository = new ViewRepository(pointerStore, blobStore);
    names =
        new NameResolver(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    nodes = new NodeLoader(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    lister = new RelationLister(names, nodes, new SystemObjectResolver());
  }

  @Test
  void listRelationsIncludesNestedNamespaceContent() {
    ResourceId catalogId = createCatalog("floe-catalog");
    ResourceId parentNamespaceId = createNamespace(catalogId, "parent", "staging", List.of());
    ResourceId nestedNamespaceId = createNamespace(catalogId, "nested", "2025", List.of("staging"));
    createTable(catalogId, nestedNamespaceId, "orders_2025");
    createView(catalogId, nestedNamespaceId, "yearly_report");

    List<GraphNode> nodes = lister.listRelations(catalogId, "floe", "1");

    assertThat(nodes)
        .filteredOn(node -> node instanceof TableNode)
        .map(TableNode.class::cast)
        .anyMatch(table -> table.namespaceId().equals(nestedNamespaceId));
    assertThat(nodes)
        .filteredOn(node -> node instanceof NamespaceNode)
        .map(NamespaceNode.class::cast)
        .anyMatch(namespace -> namespace.displayName().equals("2025"));
    assertThat(nodes)
        .filteredOn(node -> node instanceof NamespaceNode)
        .map(NamespaceNode.class::cast)
        .anyMatch(namespace -> namespace.catalogId().equals(catalogId));
  }

  @Test
  void listRelationsInNamespaceRestrictsToThatNamespace() {
    ResourceId catalogId = createCatalog("floe-catalog");
    ResourceId parentNamespaceId = createNamespace(catalogId, "parent", "staging", List.of());
    ResourceId nestedNamespaceId = createNamespace(catalogId, "nested", "2025", List.of("staging"));
    createTable(catalogId, parentNamespaceId, "legacy_orders");
    ResourceId nestedTableId = createTable(catalogId, nestedNamespaceId, "orders_2025");
    ResourceId nestedViewId = createView(catalogId, nestedNamespaceId, "yearly_report");

    List<GraphNode> nodes = lister.listRelationsInNamespace(catalogId, nestedNamespaceId);

    assertThat(nodes).hasSize(2);
    List<TableNode> tables =
        nodes.stream()
            .filter(node -> node instanceof TableNode)
            .map(TableNode.class::cast)
            .toList();
    List<ViewNode> views =
        nodes.stream().filter(node -> node instanceof ViewNode).map(ViewNode.class::cast).toList();

    assertThat(tables).hasSize(1);
    assertThat(tables).allMatch(table -> table.namespaceId().equals(nestedNamespaceId));
    assertThat(tables).anyMatch(table -> table.id().equals(nestedTableId));
    assertThat(views).hasSize(1);
    assertThat(views).allMatch(view -> view.namespaceId().equals(nestedNamespaceId));
    assertThat(views).anyMatch(view -> view.id().equals(nestedViewId));
  }

  private ResourceId createCatalog(String name) {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setId(name)
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    catalogRepository.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName(name).build());
    return catalogId;
  }

  private ResourceId createNamespace(
      ResourceId catalogId, String id, String displayName, List<String> parents) {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setId(id)
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    namespaceRepository.create(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .addAllParents(parents)
            .setDisplayName(displayName)
            .build());
    return namespaceId;
  }

  private ResourceId createTable(ResourceId catalogId, ResourceId namespaceId, String name) {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setId("table-" + name)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(name)
            .setSchemaJson("{\"fields\":[]}")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setUri("s3://floecat/" + name)
                    .build())
            .build();
    tableRepository.create(table);
    return tableId;
  }

  private ResourceId createView(ResourceId catalogId, ResourceId namespaceId, String name) {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setId("view-" + name)
            .setKind(ResourceKind.RK_VIEW)
            .build();
    View view =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(name)
            .setSql("select 1")
            .build();
    viewRepository.create(view);
    return viewId;
  }
}
