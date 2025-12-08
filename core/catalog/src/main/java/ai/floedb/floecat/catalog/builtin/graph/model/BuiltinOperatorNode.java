package ai.floedb.floecat.catalog.builtin.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin operator definition. */
public record BuiltinOperatorNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    ResourceId leftType,
    ResourceId rightType,
    ResourceId returnType,
    boolean commutative,
    boolean associative,
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
