package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable namespace node tracking hierarchy and optional children.
 *
 * <p>Child relations are only populated for listing RPCs; query-resolution paths can leave the
 * field empty to avoid unnecessary cache churn.
 */
public record NamespaceNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    List<String> pathSegments,
    String displayName,
    Map<String, String> properties,
    Optional<List<ResourceId>> relationIds,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public NamespaceNode {
    pathSegments = List.copyOf(pathSegments);
    properties = Map.copyOf(properties);
    relationIds =
        relationIds == null ? Optional.empty() : relationIds.map(list -> List.copyOf(list));
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.NAMESPACE;
  }
}
