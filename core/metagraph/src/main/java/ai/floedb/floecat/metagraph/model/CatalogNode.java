package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable view of a catalog resource.
 *
 * <p>Catalog nodes primarily act as anchors for namespace/table traversal, so the model keeps their
 * metadata intentionally small.
 */
public record CatalogNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String displayName,
    Map<String, String> properties,
    Optional<String> connectorId,
    Optional<String> policyRef,
    Optional<List<ResourceId>> namespaceIds,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public CatalogNode {
    properties = Map.copyOf(properties);
    connectorId = connectorId == null ? Optional.empty() : connectorId;
    policyRef = policyRef == null ? Optional.empty() : policyRef;
    namespaceIds =
        namespaceIds == null ? Optional.empty() : namespaceIds.map(list -> List.copyOf(list));
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.CATALOG;
  }
}
