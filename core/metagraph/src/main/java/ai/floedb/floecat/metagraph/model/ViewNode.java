package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable view node encapsulating SQL definition and dependency references.
 *
 * <p>The node stores base relation IDs only; traversal APIs resolve them to aligned {@link
 * RelationNode}s so callers always see a consistent tree.
 */
public record ViewNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    String sql,
    String dialect,
    List<SchemaColumn> outputColumns,
    List<ResourceId> baseRelations,
    List<String> creationSearchPath,
    Map<String, String> properties,
    Optional<String> owner,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public ViewNode {
    outputColumns = List.copyOf(outputColumns);
    baseRelations = List.copyOf(baseRelations);
    creationSearchPath = List.copyOf(creationSearchPath);
    properties = Map.copyOf(properties);
    owner = owner == null ? Optional.empty() : owner;
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.VIEW;
  }
}
