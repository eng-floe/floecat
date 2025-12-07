package ai.floedb.floecat.service.query.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin function definition. */
public record BuiltinFunctionNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    List<ResourceId> argumentTypes,
    ResourceId returnType,
    boolean aggregate,
    boolean window,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinFunctionNode {
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_FUNCTION;
  }
}
