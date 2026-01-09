package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Mutable holder describing a relation during bundle decoration. */
public final class RelationDecoration {

  private final RelationInfo.Builder builder;
  private final ResourceId relationId;
  private final GraphNode node;
  private final List<SchemaColumn> schema;
  private final List<SchemaColumn> emitted;
  private final MetadataResolutionContext resolutionContext;
  private final Map<String, Object> columns = new HashMap<>();

  public RelationDecoration(
      RelationInfo.Builder builder,
      ResourceId relationId,
      GraphNode node,
      List<SchemaColumn> schema,
      List<SchemaColumn> emitted,
      MetadataResolutionContext resolutionContext) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.relationId = Objects.requireNonNull(relationId, "relationId");
    this.node = node;
    this.schema = List.copyOf(schema);
    this.emitted = List.copyOf(emitted);
    this.resolutionContext = Objects.requireNonNull(resolutionContext, "resolutionContext");
  }

  public RelationInfo.Builder builder() {
    return builder;
  }

  public ResourceId relationId() {
    return relationId;
  }

  public GraphNode node() {
    return node;
  }

  public List<SchemaColumn> schema() {
    return schema;
  }

  public List<SchemaColumn> emitted() {
    return emitted;
  }

  public MetadataResolutionContext resolutionContext() {
    return resolutionContext;
  }

  @SuppressWarnings("unchecked")
  public <T> T attribute(String key) {
    return (T) columns.get(key);
  }

  public void attribute(String key, Object value) {
    if (value == null) {
      columns.remove(key);
    } else {
      columns.put(key, value);
    }
  }
}
