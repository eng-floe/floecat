package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin aggregate definition. */
public record AggregateNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    List<ResourceId> argumentTypes,
    ResourceId stateType,
    ResourceId returnType,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public AggregateNode {
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.AGGREGATE;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
