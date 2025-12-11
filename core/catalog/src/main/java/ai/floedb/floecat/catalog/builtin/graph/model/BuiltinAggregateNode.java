package ai.floedb.floecat.catalog.builtin.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin aggregate definition. */
public record BuiltinAggregateNode(
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

  public BuiltinAggregateNode {
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.BUILTIN_AGGREGATE;
  }
}
