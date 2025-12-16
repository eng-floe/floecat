package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Shared minimal implementation of CatalogOverlay for tests. Tests may subclass or override
 * individual methods.
 */
public class TestCatalogOverlay implements CatalogOverlay {

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public List<GraphNode> listRelations(ResourceId catalogId) {
    return List.of();
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return List.of();
  }

  @Override
  public List<GraphNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    return List.of();
  }

  @Override
  public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
    return List.of();
  }

  @Override
  public ResourceId resolveCatalog(String correlationId, String name) {
    return ResourceId.getDefaultInstance();
  }

  @Override
  public ResourceId resolveNamespace(String correlationId, NameRef ref) {
    return ResourceId.getDefaultInstance();
  }

  @Override
  public ResourceId resolveTable(String correlationId, NameRef ref) {
    return ResourceId.getDefaultInstance();
  }

  @Override
  public ResourceId resolveView(String correlationId, NameRef ref) {
    return ResourceId.getDefaultInstance();
  }

  @Override
  public ResourceId resolveName(String correlationId, NameRef ref) {
    return ResourceId.getDefaultInstance();
  }

  @Override
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    return SnapshotPin.getDefaultInstance();
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    return new ResolveResult(List.of(), 0, "");
  }

  @Override
  public ResolveResult resolveTables(
      String correlationId, NameRef prefix, int limit, String token) {
    return new ResolveResult(List.of(), 0, "");
  }

  @Override
  public ResolveResult resolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    return new ResolveResult(List.of(), 0, "");
  }

  @Override
  public ResolveResult resolveViews(String correlationId, NameRef prefix, int limit, String token) {
    return new ResolveResult(List.of(), 0, "");
  }

  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    return null;
  }

  @Override
  public Map<String, String> tableColumnTypes(ResourceId tableId) {
    return Map.of();
  }
}
