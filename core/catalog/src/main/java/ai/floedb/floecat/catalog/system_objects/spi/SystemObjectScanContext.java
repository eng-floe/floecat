package ai.floedb.floecat.catalog.system_objects.spi;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectGraphView;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable context during a system object scan.
 *
 * <p>This provides view/table/namespace resolution through a minimal graph view abstraction. It is
 * safe, cache-aware, and keeps core decoupled from the full MetadataGraph implementation.
 */
public record SystemObjectScanContext(
    SystemObjectGraphView graph,
    NameRef name,
    String engineKind,
    String engineVersion,
    ResourceId catalogId,
    ResourceId namespaceId) {

  public GraphNode resolve(ResourceId id) {
    return graph.resolve(id).orElseThrow();
  }

  public Optional<GraphNode> tryResolve(ResourceId id) {
    return graph.resolve(id);
  }

  public List<TableNode> listTables(ResourceId namespace) {
    return graph.listTables(namespace);
  }

  public Optional<String> schemaJson(ResourceId tableId) {
    return graph.tableSchemaJson(tableId);
  }

  public Map<String, String> columnTypes(ResourceId tableId) {
    return graph.tableColumnTypes(tableId);
  }

  public List<NamespaceNode> listNamespaces() {
    return graph.listNamespaces(catalogId);
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalog) {
    return graph.listNamespaces(catalog);
  }

  public List<ViewNode> listViews(ResourceId namespace) {
    return graph.listViews(namespace);
  }

  public List<TableNode> listTables() {
    return graph.listTables(namespaceId);
  }
}
