package ai.floedb.floecat.service.metagraph.overlay;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.query.resolver.LogicalSchemaMapper;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public final class MetaGraph implements CatalogOverlay {

  private static final String SYSTEM_ACCOUNT = "_system";

  private final UserGraph metadataGraph;
  private final LogicalSchemaMapper schemaMapper;
  private final SystemGraph systemGraph;

  @Inject
  public MetaGraph(
      UserGraph metadataGraph, LogicalSchemaMapper schemaMapper, SystemGraph systemGraph) {
    this.metadataGraph = metadataGraph;
    this.schemaMapper = schemaMapper;
    this.systemGraph = systemGraph;
  }

  @Override
  public Optional<GraphNode> resolve(ResourceId id, String engineKind, String engineVersion) {
    Optional<GraphNode> systemNode = systemGraph.resolve(id, engineKind, engineVersion);
    if (systemNode.isPresent()) {
      return systemNode;
    }
    return metadataGraph.resolve(id);
  }

  @Override
  public List<GraphNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {
    if (isSystemAccount(catalogId)) {
      return systemGraph.listRelations(catalogId, engineKind, engineVersion);
    }
    return metadataGraph.listRelations(catalogId);
  }

  @Override
  public List<GraphNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion) {
    return metadataGraph.listRelationsInNamespace(catalogId, namespaceId);
  }

  @Override
  public List<NamespaceNode> listNamespaces(
      ResourceId catalogId, String engineKind, String engineVersion) {
    if (isSystemAccount(catalogId)) {
      return systemGraph.listNamespaces(catalogId, engineKind, engineVersion);
    }
    return metadataGraph.listNamespaces(catalogId);
  }

  @Override
  public ResourceId resolveCatalog(String correlationId, String name) {
    return metadataGraph.resolveCatalog(correlationId, name);
  }

  @Override
  public ResourceId resolveNamespace(String correlationId, NameRef ref) {
    return metadataGraph.resolveNamespace(correlationId, ref);
  }

  @Override
  public ResourceId resolveTable(String correlationId, NameRef ref) {
    return metadataGraph.resolveTable(correlationId, ref);
  }

  @Override
  public ResourceId resolveView(String correlationId, NameRef ref) {
    return metadataGraph.resolveView(correlationId, ref);
  }

  @Override
  public ResourceId resolveName(String correlationId, NameRef ref) {
    return metadataGraph.resolveName(correlationId, ref);
  }

  @Override
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    return metadataGraph.snapshotPinFor(correlationId, tableId, override, asOfDefault);
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    return toResolveResult(metadataGraph.resolveTables(correlationId, items, limit, token));
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, NameRef prefix, int limit, String token) {
    return toResolveResult(metadataGraph.resolveTables(correlationId, prefix, limit, token));
  }

  @Override
  public ResolveResult resolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    return toResolveResult(metadataGraph.resolveViews(correlationId, items, limit, token));
  }

  @Override
  public ResolveResult resolveViews(String correlationId, NameRef prefix, int limit, String token) {
    return toResolveResult(metadataGraph.resolveViews(correlationId, prefix, limit, token));
  }

  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    return metadataGraph.namespaceName(id);
  }

  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    return metadataGraph.tableName(id);
  }

  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    return metadataGraph.viewName(id);
  }

  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    return metadataGraph.catalog(id);
  }

  @Override
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    UserGraph.SchemaResolution delegate = metadataGraph.schemaFor(correlationId, tableId, snapshot);
    if (delegate == null) {
      return null;
    }
    return new SchemaResolution(delegate.table(), delegate.schemaJson());
  }

  private static boolean isSystemAccount(ResourceId id) {
    return id != null && SYSTEM_ACCOUNT.equals(id.getAccountId());
  }

  // SystemObjectGraphView delegate helpers

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return metadataGraph.resolve(id);
  }

  @Override
  public Optional<TableNode> tryTable(ResourceId id) {
    return metadataGraph.table(id);
  }

  @Override
  public Optional<ViewNode> tryView(ResourceId id) {
    return metadataGraph.view(id);
  }

  @Override
  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return metadataGraph.namespace(id);
  }

  @Override
  public List<ResourceId> listCatalogs() {
    return metadataGraph.listAllCatalogIds(metadataGraph.currentAccountId());
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return metadataGraph.listNamespaces(catalogId);
  }

  @Override
  public List<TableNode> listTables(ResourceId id) {
    if (isCatalog(id)) {
      return metadataGraph.listTablesInCatalog(id);
    } else if (isNamespace(id)) {
      return metadataGraph.listTablesInNamespace(id);
    }
    return List.of();
  }

  @Override
  public List<ViewNode> listViews(ResourceId id) {
    if (isCatalog(id)) {
      return metadataGraph.listViewsInCatalog(id);
    } else if (isNamespace(id)) {
      return metadataGraph.listViewsInNamespace(id);
    }
    return List.of();
  }

  @Override
  public Optional<String> tableSchemaJson(ResourceId tableId) {
    return metadataGraph.table(tableId).map(TableNode::schemaJson);
  }

  @Override
  public Map<String, String> tableColumnTypes(ResourceId tableId) {
    return metadataGraph
        .table(tableId)
        .map(schemaMapper::map)
        .map(SchemaDescriptor::getColumnsList)
        .map(this::toTypeMap)
        .orElse(Map.of());
  }

  private ResolveResult toResolveResult(UserGraph.ResolveResult delegate) {
    List<CatalogOverlay.QualifiedRelation> relations =
        delegate.relations().stream()
            .map(rel -> new CatalogOverlay.QualifiedRelation(rel.name(), rel.resourceId()))
            .toList();
    return new ResolveResult(relations, delegate.totalSize(), delegate.nextToken());
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
    return id != null && id.getKind() == ResourceKind.RK_CATALOG;
  }

  private boolean isNamespace(ResourceId id) {
    return id != null && id.getKind() == ResourceKind.RK_NAMESPACE;
  }
}
