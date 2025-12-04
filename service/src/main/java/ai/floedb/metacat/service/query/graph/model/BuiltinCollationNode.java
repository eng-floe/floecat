package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;

/** Relation node describing a builtin collation. */
public record BuiltinCollationNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    String locale,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public BuiltinCollationNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.BUILTIN_COLLATION;
  }
}
