package ai.floedb.floecat.systemcatalog.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Virtual system object node (information_schema, floecat.*, engine plugin system tables).
 *
 * <p>Unlike tables/views, system objects are not stored in the pointer/blob repositories. They are
 * instead sourced from the SystemObjectRegistry and lifted into the metagraph so resolution is
 * unified and downstream planners do not need a separate codepath.
 *
 * <p>Nodes are immutable and behave exactly like any GraphNode: - stable ResourceId - version = 0
 * (static definition) - metadataUpdatedAt = service startup instant
 *
 * <p>Row generation logic is NOT stored inside the node. It is looked up in SystemObjectRegistry
 * using the node's scannerId.
 */
public record SystemTableNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    ResourceId namespaceId,
    List<SchemaColumn> columns,
    String scannerId,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public SystemTableNode {
    columns = List.copyOf(columns);
    displayName = displayName == null ? "" : displayName;
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.TABLE;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
