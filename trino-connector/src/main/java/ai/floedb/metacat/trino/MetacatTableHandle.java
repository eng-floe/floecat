package ai.floedb.metacat.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

/** Connector table handle carrying the Metacat table id and Iceberg metadata JSON. */
public class MetacatTableHandle implements ConnectorTableHandle {

  private final SchemaTableName schemaTableName;
  private final ai.floedb.metacat.common.rpc.ResourceId tableId;
  private final String tableUri;
  private final String schemaJson;
  private final String partitionSpecJson;
  private final String format;
  private final CatalogHandle catalogHandle;

  @JsonCreator
  public MetacatTableHandle(
      @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
      @JsonProperty("tableId") ai.floedb.metacat.common.rpc.ResourceId tableId,
      @JsonProperty("tableUri") String tableUri,
      @JsonProperty("schemaJson") String schemaJson,
      @JsonProperty("partitionSpecJson") String partitionSpecJson,
      @JsonProperty("format") String format,
      @JsonProperty("catalogHandle") CatalogHandle catalogHandle) {
    this.schemaTableName = schemaTableName;
    this.tableId = tableId;
    this.tableUri = tableUri;
    this.schemaJson = schemaJson;
    this.partitionSpecJson = partitionSpecJson;
    this.format = format;
    this.catalogHandle = catalogHandle;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @JsonProperty
  public ai.floedb.metacat.common.rpc.ResourceId getTableId() {
    return tableId;
  }

  @JsonProperty
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

  public IcebergTableHandle toIcebergTableHandle(Map<String, String> storageProperties) {
    return new IcebergTableHandle(
        catalogHandle,
        schemaTableName.getSchemaName(),
        schemaTableName.getTableName(),
        TableType.DATA,
        Optional.empty(),
        schemaJson,
        Optional.ofNullable(partitionSpecJson),
        2,
        TupleDomain.all(),
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
