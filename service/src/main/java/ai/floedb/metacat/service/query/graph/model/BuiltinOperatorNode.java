package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin operator definition. */
public record BuiltinOperatorNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String name,
    String leftType,
    String rightType,
    String functionName,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinOperatorNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_OPERATOR;
  }
}
