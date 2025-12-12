package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin function definition. */
public record FunctionNode(
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
    implements GraphNode {

  public FunctionNode {
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.FUNCTION;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
