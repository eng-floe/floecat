package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin collation. */
public record CollationNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    String locale,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public CollationNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.COLLATION;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
