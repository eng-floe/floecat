package ai.floedb.floecat.catalog.system_objects.registry;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal read-only view of the metadata graph exposed to system object scanners.
 *
 * <p>This keeps the core catalog module decoupled from the full MetadataGraph implementation which
 * lives in the service layer.
 */
public interface SystemObjectGraphView {

  // ---- Resolution ----
  Optional<GraphNode> resolve(ResourceId id);

  Optional<TableNode> tryTable(ResourceId id);

  Optional<ViewNode> tryView(ResourceId id);

  Optional<NamespaceNode> tryNamespace(ResourceId id);

  // ---- Catalog enumeration ----
  List<ResourceId> listCatalogs();

  // ---- Namespace enumeration ----
  List<NamespaceNode> listNamespaces(ResourceId catalogId);

  // ---- Table / View enumeration ----
  List<TableNode> listTables(ResourceId namespaceId);

  List<ViewNode> listViews(ResourceId namespaceId);

  // ---- Schema access ----
  Optional<String> tableSchemaJson(ResourceId tableId);

  default Map<String, String> tableColumnTypes(ResourceId tableId) {
    return Map.of();
  }
}
