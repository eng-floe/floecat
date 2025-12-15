package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.metagraph.overlay.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared stub implementation of {@link CatalogOverlay} for unit tests. */
public abstract class TestCatalogOverlay implements CatalogOverlay {

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Not used in this test");
  }

  @Override
  public Optional<GraphNode> resolve(ResourceId id, String engineKind, String engineVersion) {
    throw unsupported();
  }

  @Override
  public List<GraphNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {
    throw unsupported();
  }

  @Override
  public List<NamespaceNode> listNamespaces(
      ResourceId catalogId, String engineKind, String engineVersion) {
    throw unsupported();
  }

  @Override
  public List<GraphNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion) {
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

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<TableNode> tryTable(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<ViewNode> tryView(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    throw unsupported();
  }

  @Override
  public List<ResourceId> listCatalogs() {
    throw unsupported();
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    throw unsupported();
  }

  @Override
  public List<TableNode> listTables(ResourceId namespaceId) {
    throw unsupported();
  }

  @Override
  public List<ViewNode> listViews(ResourceId namespaceId) {
    throw unsupported();
  }

  @Override
  public Optional<String> tableSchemaJson(ResourceId tableId) {
    throw unsupported();
  }

  @Override
  public Map<String, String> tableColumnTypes(ResourceId tableId) {
    throw unsupported();
  }
}
