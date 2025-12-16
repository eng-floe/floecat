package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared stub implementation of {@link CatalogOverlay} for unit tests. */
public class TestCatalogOverlay implements CatalogOverlay {

  private final Map<ResourceId, GraphNode> nodes = new HashMap<>();
  private final Map<ResourceId, List<GraphNode>> relationsByNamespace = new HashMap<>();
  private final Map<ResourceId, List<FunctionNode>> functionsByNamespace = new HashMap<>();
  private final Map<ResourceId, Map<String, String>> columnTypes = new HashMap<>();

  // ------------------------
  // Test helpers
  // ------------------------

  public TestCatalogOverlay addNode(GraphNode node) {
    nodes.put(node.id(), node);
    return this;
  }

  public TestCatalogOverlay addRelation(ResourceId namespaceId, GraphNode node) {
    relationsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(node);
    addNode(node);
    return this;
  }

  public TestCatalogOverlay addFunction(ResourceId namespaceId, FunctionNode fn) {
    functionsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(fn);
    addNode(fn);
    return this;
  }

  public TestCatalogOverlay setColumnTypes(ResourceId tableId, Map<String, String> types) {
    columnTypes.put(tableId, types);
    return this;
  }

  // ------------------------
  // CatalogOverlay impl
  // ------------------------

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.ofNullable(nodes.get(id));
  }

  @Override
  public List<GraphNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    return relationsByNamespace.getOrDefault(namespaceId, List.of());
  }

  @Override
  public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
    return functionsByNamespace.getOrDefault(namespaceId, List.of());
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return nodes.values().stream()
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .toList();
  }

  @Override
  public Map<String, String> tableColumnTypes(ResourceId tableId) {
    return columnTypes.getOrDefault(tableId, Map.of());
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Not used in this test");
  }

  @Override
  public List<GraphNode> listRelations(ResourceId catalogId) {
    throw unsupported();
  }

  @Override
  public ResourceId resolveCatalog(String correlationId, String name) {
    throw unsupported();
  }

  @Override
  public ResourceId resolveNamespace(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public ResourceId resolveTable(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public ResourceId resolveView(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public ResourceId resolveName(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    throw unsupported();
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    throw unsupported();
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, NameRef prefix, int limit, String token) {
    throw unsupported();
  }

  @Override
  public ResolveResult resolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    throw unsupported();
  }

  @Override
  public ResolveResult resolveViews(String correlationId, NameRef prefix, int limit, String token) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    throw unsupported();
  }

  @Override
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    throw unsupported();
  }
}
