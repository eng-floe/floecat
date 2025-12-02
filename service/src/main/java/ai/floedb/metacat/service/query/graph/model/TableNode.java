package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable table node capturing logical schema and snapshot metadata.
 *
 * <p>The node purposefully limits heavyweight fields (statistics, resolved snapshot sets) to
 * optional references so the cache can keep large catalogs hot without unnecessary churn.
 */
public record TableNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    TableFormat format,
    String schemaJson,
    Map<String, String> properties,
    List<String> partitionKeys,
    Map<String, Integer> fieldIdByPath,
    Optional<SnapshotRef> currentSnapshot,
    Optional<SnapshotRef> previousSnapshot,
    Optional<ResolvedSnapshotInfo> resolvedSnapshots,
    Optional<TableStatsSummary> statsSummary,
    List<ResourceId> dependentViews,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public TableNode {
    properties = Map.copyOf(properties);
    partitionKeys = List.copyOf(partitionKeys);
    fieldIdByPath = Map.copyOf(fieldIdByPath);
    currentSnapshot = currentSnapshot == null ? Optional.empty() : currentSnapshot;
    previousSnapshot = previousSnapshot == null ? Optional.empty() : previousSnapshot;
    resolvedSnapshots = resolvedSnapshots == null ? Optional.empty() : resolvedSnapshots;
    statsSummary = statsSummary == null ? Optional.empty() : statsSummary;
    dependentViews = List.copyOf(dependentViews);
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.TABLE;
  }
}
