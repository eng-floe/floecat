package ai.floedb.floecat.service.query.graph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Metadata node describing a system relation (information_schema, floecat.* or engine-provided
 * views).
 */
public record SystemViewNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    SystemViewBackendKind backendKind,
    List<SchemaColumn> columns,
    String generatorId,
    Map<String, String> properties,
    Map<EngineKey, EngineHint> engineHints)
    implements RelationNode {

  public SystemViewNode {
    columns = List.copyOf(columns);
    properties = Map.copyOf(properties);
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public RelationNodeKind kind() {
    return RelationNodeKind.SYSTEM_VIEW;
  }
}
