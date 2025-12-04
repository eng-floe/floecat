package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin SQL type. */
public record BuiltinTypeNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String name,
    String category,
    boolean array,
    String elementType,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinTypeNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_TYPE;
  }
}
