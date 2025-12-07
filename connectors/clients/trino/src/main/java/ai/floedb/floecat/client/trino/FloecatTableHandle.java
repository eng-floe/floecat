package ai.floedb.floecat.client.trino;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class FloecatTableHandle implements ConnectorTableHandle {

  private final SchemaTableName schemaTableName;
  private final String tableId;
  private final String tableAccountId;
  private final String tableKind;
  private final String tableUri;
  private final String schemaJson;
  private final String partitionSpecJson;
  private final String format;
  private final String catalogHandleId;
  private final TupleDomain<IcebergColumnHandle> enforcedConstraint;
  private final Set<String> projectedColumns;
  private final Long snapshotId;
  private final Long asOfEpochMillis;

  @JsonCreator
  public FloecatTableHandle(
      @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
      @JsonProperty("tableId") String tableId,
      @JsonProperty("tableAccountId") String tableAccountId,
      @JsonProperty("tableKind") String tableKind,
      @JsonProperty("tableUri") String tableUri,
      @JsonProperty("schemaJson") String schemaJson,
      @JsonProperty("partitionSpecJson") String partitionSpecJson,
      @JsonProperty("format") String format,
      @JsonProperty("catalogHandleId") String catalogHandleId,
      @JsonProperty("enforcedConstraint") TupleDomain<IcebergColumnHandle> enforcedConstraint,
      @JsonProperty("projectedColumns") Set<String> projectedColumns,
      @JsonProperty("snapshotId") Long snapshotId,
      @JsonProperty("asOfEpochMillis") Long asOfEpochMillis) {
    this.schemaTableName = schemaTableName;
    this.tableId = tableId;
    this.tableAccountId = tableAccountId;
    this.tableKind = tableKind;
    this.tableUri = tableUri;
    this.schemaJson = schemaJson;
    this.partitionSpecJson = partitionSpecJson;
    this.format = format;
    this.catalogHandleId = catalogHandleId;
    this.enforcedConstraint = enforcedConstraint == null ? TupleDomain.all() : enforcedConstraint;
    this.projectedColumns = projectedColumns == null ? Set.of() : Set.copyOf(projectedColumns);
    this.snapshotId = snapshotId;
    this.asOfEpochMillis = asOfEpochMillis;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  public ResourceId getTableResourceId() {
    if (tableAccountId == null || tableAccountId.isBlank()) {
      throw new IllegalStateException("Missing account id on table handle for table " + tableId);
    }
    ResourceKind kind =
        (tableKind == null || tableKind.isBlank())
            ? ResourceKind.RK_TABLE
            : ResourceKind.valueOf(tableKind);
    return ResourceId.newBuilder().setId(tableId).setAccountId(tableAccountId).setKind(kind).build();
  }

  @JsonProperty
  public String getTableId() {
    return tableId;
  }

  @JsonProperty
  public String getTableAccountId() {
    return tableAccountId;
  }

  @JsonProperty
  public String getTableKind() {
    return tableKind;
  }

  @JsonProperty("tableUri")
  public String getUri() {
    return tableUri;
  }

  @JsonProperty
  public String getSchemaJson() {
    return schemaJson;
  }

  @JsonProperty
  public String getPartitionSpecJson() {
    return partitionSpecJson;
  }

  @JsonProperty
  public String getFormat() {
    return format;
  }

  @JsonProperty("catalogHandleId")
  public String getCatalogHandleId() {
    return catalogHandleId;
  }

  @JsonProperty("enforcedConstraint")
  public TupleDomain<IcebergColumnHandle> getEnforcedConstraint() {
    return enforcedConstraint;
  }

  @JsonProperty("projectedColumns")
  public Set<String> getProjectedColumns() {
    return projectedColumns;
  }

  @JsonProperty("snapshotId")
  public Long getSnapshotId() {
    return snapshotId;
  }

  @JsonProperty("asOfEpochMillis")
  public Long getAsOfEpochMillis() {
    return asOfEpochMillis;
  }

  public IcebergTableHandle toIcebergTableHandle(Map<String, String> storageProperties) {
    return new IcebergTableHandle(
        CatalogHandle.fromId(catalogHandleId),
        schemaTableName.getSchemaName(),
        schemaTableName.getTableName(),
        TableType.DATA,
        Optional.empty(),
        schemaJson,
        Optional.ofNullable(partitionSpecJson),
        2,
        enforcedConstraint,
        TupleDomain.all(),
        OptionalLong.empty(),
        Set.of(),
        Optional.empty(),
        tableUri,
        storageProperties,
        Optional.empty(),
        false,
        Optional.empty(),
        Set.of(),
        Optional.empty());
  }
}
