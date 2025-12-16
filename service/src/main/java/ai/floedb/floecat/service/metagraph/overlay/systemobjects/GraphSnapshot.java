package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable, engine/version-scoped snapshot of all builtin system objects.
 *
 * <p>A {@code GraphSnapshot} represents the complete synthetic system catalog (namespaces, tables,
 * functions, and other graph nodes) for a single engine kind and engine version.
 *
 * <p>Snapshots are constructed once by {@link SystemGraph} and then treated as read-only data
 * structures. They are safe to share across threads and across scan contexts.
 *
 * <p>Key properties:
 *
 * <ul>
 *   <li>Fully pre-filtered by engine kind + version
 *   <li>No runtime filtering or mutation
 *   <li>Indexed for fast namespace and id-based lookup
 *   <li>Indexed for fast name-based lookup
 * </ul>
 */
public final class GraphSnapshot {

  // All system namespaces visible in this snapshot (e.g. pg_catalog, information_schema)
  private final List<NamespaceNode> namespaces;
  // System tables/view grouped by owning namespace ResourceId
  private final Map<ResourceId, List<TableNode>> tablesByNamespace;
  private final Map<ResourceId, List<ViewNode>> viewsByNamespace;
  // System functions grouped by owning namespace ResourceId
  private final Map<ResourceId, List<FunctionNode>> functionsByNamespace;
  // Fast lookup map for resolving any system node by its ResourceId
  private final Map<ResourceId, GraphNode> nodesById;
  // Fast lookup maps for resolving by canonical name
  private final Map<String, ResourceId> tableNames;
  private final Map<String, ResourceId> viewNames;
  private final Map<String, ResourceId> namespaceNames;

  public GraphSnapshot(
      List<NamespaceNode> namespaces,
      Map<ResourceId, List<TableNode>> tablesByNamespace,
      Map<ResourceId, List<ViewNode>> viewsByNamespace,
      Map<ResourceId, List<FunctionNode>> functionsByNamespace,
      Map<ResourceId, GraphNode> nodesById,
      Map<String, ResourceId> tableNames,
      Map<String, ResourceId> viewNames,
      Map<String, ResourceId> namespaceNames) {

    this.namespaces = namespaces;
    this.tablesByNamespace = tablesByNamespace;
    this.viewsByNamespace = viewsByNamespace;
    this.functionsByNamespace = functionsByNamespace;
    this.nodesById = nodesById;
    this.tableNames = tableNames;
    this.viewNames = viewNames;
    this.namespaceNames = namespaceNames;
  }

  /** Returns all system namespaces in this snapshot. */
  public List<NamespaceNode> namespaces() {
    return namespaces;
  }

  /** Returns all system types in this snapshot. */
  public List<TypeNode> types() {
    return nodesById.values().stream()
        .filter(TypeNode.class::isInstance)
        .map(TypeNode.class::cast)
        .toList();
  }

  /**
   * Returns all system tables that belong to the given namespace.
   *
   * @param namespaceId namespace ResourceId
   */
  public List<TableNode> tablesInNamespace(ResourceId namespaceId) {
    return tablesByNamespace.getOrDefault(namespaceId, List.of());
  }

  /**
   * Returns all system views that belong to the given namespace.
   *
   * @param namespaceId namespace ResourceId
   */
  public List<ViewNode> viewsInNamespace(ResourceId namespaceId) {
    return viewsByNamespace.getOrDefault(namespaceId, List.of());
  }

  /**
   * Returns all system functions that belong to the given namespace.
   *
   * @param namespaceId namespace ResourceId
   */
  public List<FunctionNode> functionsInNamespace(ResourceId namespaceId) {
    return functionsByNamespace.getOrDefault(namespaceId, List.of());
  }

  /**
   * Resolves any system graph node by its ResourceId.
   *
   * @param id ResourceId of the node
   */
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.ofNullable(nodesById.get(id));
  }

  /**
   * Returns an empty snapshot used as a safe fallback when no system catalog exists for a given
   * engine/version.
   */
  public static GraphSnapshot empty() {
    return new GraphSnapshot(
        List.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
  }

  /** Returns the table name to ResourceId map. */
  public Map<String, ResourceId> tableNames() {
    return tableNames;
  }

  /** Returns the view name to ResourceId map. */
  public Map<String, ResourceId> viewNames() {
    return viewNames;
  }

  /** Returns the namespace name to ResourceId map. */
  public Map<String, ResourceId> namespaceNames() {
    return namespaceNames;
  }
}
