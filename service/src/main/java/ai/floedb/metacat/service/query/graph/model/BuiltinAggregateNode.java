package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Relation node describing a builtin aggregate definition. */
public record BuiltinAggregateNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String name,
    List<String> argumentTypes,
    String stateType,
    String returnType,
    String stateFunction,
    String finalFunction,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinAggregateNode {
    argumentTypes = List.copyOf(argumentTypes == null ? List.of() : argumentTypes);
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_AGGREGATE;
  }
}
