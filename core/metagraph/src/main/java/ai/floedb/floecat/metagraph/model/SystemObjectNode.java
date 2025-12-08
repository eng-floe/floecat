package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.Map;

/**
 * Virtual system object node (information_schema, floecat.*, engine plugin system tables).
 *
 * <p>Unlike tables/views, system objects are not stored in the pointer/blob repositories. They are
 * instead sourced from the SystemObjectRegistry and lifted into the metagraph so resolution is
 * unified and downstream planners do not need a separate codepath.
 *
 * <p>Nodes are immutable and behave exactly like any RelationNode: - stable ResourceId - version =
 * 0 (static definition) - metadataUpdatedAt = service startup instant
 *
 * <p>Row generation logic is NOT stored inside the node. It is looked up in SystemObjectRegistry
 * using the node's scannerId.
 */
public record SystemObjectNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    SchemaColumn[] columns,
    String scannerId,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public SystemObjectNode {
    columns = columns == null ? new SchemaColumn[0] : columns.clone();
    displayName = displayName == null ? "" : displayName;
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.SYSTEM_OBJECT;
  }
}
