package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin cast rule. */
public record BuiltinCastNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String sourceType,
    String targetType,
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
