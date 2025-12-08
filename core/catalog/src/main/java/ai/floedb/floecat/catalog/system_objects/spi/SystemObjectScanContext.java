package ai.floedb.floecat.catalog.system_objects.spi;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectGraphView;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Optional;

/**
 * Immutable context during a system object scan.
 *
 * <p>This provides view/table/namespace resolution through a minimal graph view abstraction. It is
 * safe, cache-aware, and keeps core decoupled from the full MetadataGraph implementation.
 */
public record SystemObjectScanContext(
    SystemObjectGraphView graphView,
    NameRef name,
    String engineKind,
    String engineVersion,
    ResourceId catalogId,
    ResourceId namespaceId) {

  /** Strong resolution, throws if not present. */
  public RelationNode resolve(ResourceId id) {
    return graphView.resolve(id).orElseThrow();
  }

  /** Weak resolution, does not throw. */
  public Optional<RelationNode> tryResolve(ResourceId id) {
    return graphView.resolve(id);
  }

  public List<TableNode> listTables(ResourceId catalog) {
    return graphView.listTables(catalog);
  }

  public List<ViewNode> listViews(ResourceId catalog) {
    return graphView.listViews(catalog);
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalog) {
    return graphView.listNamespaces(catalog);
  }
}
