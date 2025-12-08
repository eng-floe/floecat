package ai.floedb.floecat.catalog.system_objects.registry;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import java.util.List;
import java.util.Optional;

/**
 * Minimal read-only view of the metadata graph exposed to system object scanners.
 *
 * <p>This keeps the core catalog module decoupled from the full MetadataGraph implementation which
 * lives in the service layer.
 */
public interface SystemObjectGraphView {

  Optional<RelationNode> resolve(ResourceId id);

  List<TableNode> listTables(ResourceId catalogId);

  List<ViewNode> listViews(ResourceId catalogId);

  List<NamespaceNode> listNamespaces(ResourceId catalogId);
}
