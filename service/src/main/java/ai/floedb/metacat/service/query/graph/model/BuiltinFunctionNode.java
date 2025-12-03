package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin function definition. */
public record BuiltinFunctionNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String name,
    List<String> argumentTypes,
    String returnType,
    boolean aggregate,
    boolean window,
    boolean strict,
    boolean immutable,
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
