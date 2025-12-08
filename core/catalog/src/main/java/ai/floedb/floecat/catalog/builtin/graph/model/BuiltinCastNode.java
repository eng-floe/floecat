package ai.floedb.floecat.catalog.builtin.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin cast rule. */
public record BuiltinCastNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    ResourceId sourceType,
    ResourceId targetType,
    String method,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinCastNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_CAST;
  }
}
