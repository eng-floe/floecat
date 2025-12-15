package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin cast rule. */
public record CastNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    ResourceId sourceType,
    ResourceId targetType,
    String method,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public CastNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.CAST;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
