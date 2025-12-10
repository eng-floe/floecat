package ai.floedb.floecat.service.metagraph;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectGraphView;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.service.query.resolver.LogicalSchemaMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * System-object safe read-only view backed by MetadataGraph.
 *
 * <p>This is the only bridge between system object scanners (core/catalog) and the full metadata
 * graph (service layer).
 *
 * <p>It exposes: - resolution (table/view/namespace/catalog) - enumeration (catalogs → namespaces →
 * tables/views) - schema access (tableSchemaJson)
 *
 * <p>Everything is cached through MetadataGraph.
 */
public final class MetadataGraphView implements SystemObjectGraphView {

  private final MetadataGraph graph;
  private final LogicalSchemaMapper schemaMapper;

  public MetadataGraphView(MetadataGraph graph, LogicalSchemaMapper schemaMapper) {
    this.graph = graph;
    this.schemaMapper = schemaMapper;
  }

  // ------------------------------------------------------------
  // Resolution
  // ------------------------------------------------------------

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return graph.resolve(id);
  }

  @Override
  public Optional<TableNode> tryTable(ResourceId id) {
    return graph.table(id);
  }

  @Override
  public Optional<ViewNode> tryView(ResourceId id) {
    return graph.view(id);
  }

  @Override
  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return graph.namespace(id);
  }

  // ------------------------------------------------------------
  // Catalog enumeration
  // ------------------------------------------------------------

  @Override
  public List<ResourceId> listCatalogs() {
    var accountId = graph.currentAccountId(); // from PrincipalProvider
    return graph.listAllCatalogIds(accountId);
  }

  // ------------------------------------------------------------
  // Namespace enumeration
  // ------------------------------------------------------------

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return graph.listNamespaces(catalogId).stream()
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .toList();
  }

  // ------------------------------------------------------------
  // Table enumeration
  // ------------------------------------------------------------

  @Override
  public List<TableNode> listTables(ResourceId id) {
    if (isCatalog(id)) {
      return graph.listTablesInCatalog(id).stream()
          .filter(TableNode.class::isInstance)
          .map(TableNode.class::cast)
          .toList();
    } else if (isNamespace(id)) {
      return graph.listTablesInNamespace(id).stream()
          .filter(TableNode.class::isInstance)
          .map(TableNode.class::cast)
          .toList();
    } else {
      return List.of();
    }
  }

  @Override
  public List<ViewNode> listViews(ResourceId id) {
    if (isCatalog(id)) {
      return graph.listViewsInCatalog(id).stream()
          .filter(ViewNode.class::isInstance)
          .map(ViewNode.class::cast)
          .toList();
    } else if (isNamespace(id)) {
      return graph.listViewsInNamespace(id).stream()
          .filter(ViewNode.class::isInstance)
          .map(ViewNode.class::cast)
          .toList();
    } else {
      return List.of();
    }
  }

  // ------------------------------------------------------------
  // Schema access
  // ------------------------------------------------------------

  @Override
  public Optional<String> tableSchemaJson(ResourceId tableId) {
    return graph.table(tableId).map(TableNode::schemaJson);
  }

  @Override
  public Map<String, String> tableColumnTypes(ResourceId tableId) {
    return graph
        .table(tableId)
        .map(schemaMapper::map)
        .map(SchemaDescriptor::getColumnsList)
        .map(this::toTypeMap)
        .orElse(Map.of());
  }

  private Map<String, String> toTypeMap(List<SchemaColumn> columns) {
    Map<String, String> types = new LinkedHashMap<>(columns.size());
    for (SchemaColumn column : columns) {
      String path = column.getPhysicalPath();
      if (path == null || path.isBlank()) {
        path = column.getName();
      }
      types.put(path, column.getLogicalType());
    }
    return types;
  }

  private boolean isCatalog(ResourceId id) {
    return id.getKind() == ResourceKind.RK_CATALOG;
  }

  private boolean isNamespace(ResourceId id) {
    return id.getKind() == ResourceKind.RK_NAMESPACE;
  }
}
