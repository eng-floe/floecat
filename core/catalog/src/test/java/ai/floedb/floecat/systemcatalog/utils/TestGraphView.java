package ai.floedb.floecat.systemcatalog.utils;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectGraphView;
import java.util.List;
import java.util.Optional;

/**
 * Shared minimal implementation of SystemObjectGraphView for tests. Tests may subclass or override
 * individual methods.
 */
public class TestGraphView implements SystemObjectGraphView {

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<TableNode> tryTable(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<ViewNode> tryView(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return Optional.empty();
  }

  @Override
  public List<ResourceId> listCatalogs() {
    return List.of();
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return List.of();
  }

  @Override
  public List<TableNode> listTables(ResourceId namespaceId) {
    return List.of();
  }

  @Override
  public List<ViewNode> listViews(ResourceId namespaceId) {
    return List.of();
  }

  @Override
  public Optional<String> tableSchemaJson(ResourceId tableId) {
    return Optional.empty();
  }
}
