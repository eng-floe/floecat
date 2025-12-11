package ai.floedb.floecat.catalog.builtin.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin SQL type. */
public record BuiltinTypeNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    String category,
    boolean array,
    ResourceId elementType,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public BuiltinTypeNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.BUILTIN_TYPE;
  }
}
