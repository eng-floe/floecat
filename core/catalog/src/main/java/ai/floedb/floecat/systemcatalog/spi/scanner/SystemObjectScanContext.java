package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Optional;

/**
 * Immutable context during a system object scan.
 *
 * <p>This provides view/relation/namespace resolution through a minimal graph view abstraction. It
 * is safe, cache-aware, and keeps core decoupled from the full MetadataGraph implementation.
 */
public record SystemObjectScanContext(CatalogOverlay graph, NameRef name, ResourceId catalogId) {

  public GraphNode resolve(ResourceId id) {
    return graph.resolve(id).orElseThrow();
  }

  public Optional<GraphNode> tryResolve(ResourceId id) {
    return graph.resolve(id);
  }

  /** Tables + views */
  public List<GraphNode> listRelations(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(catalogId, namespaceId);
  }

  /** Tables only */
  public List<TableNode> listTables(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(catalogId, namespaceId).stream()
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .toList();
  }

  /** Views only */
  public List<ViewNode> listViews(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(catalogId, namespaceId).stream()
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .toList();
  }

  public List<NamespaceNode> listNamespaces() {
    return graph.listNamespaces(catalogId);
  }

  public List<FunctionNode> listFunctions(ResourceId namespaceId) {
    return graph.listFunctions(catalogId, namespaceId);
  }

  public List<TypeNode> listTypes() {
    return graph.listTypes(catalogId);
  }
}
