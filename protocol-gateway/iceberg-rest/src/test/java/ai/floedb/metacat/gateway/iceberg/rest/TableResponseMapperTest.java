package ai.floedb.metacat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergStatisticsFile;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableResponseMapperTest {

  private static final String EMPTY_SCHEMA_JSON = "{\"type\":\"struct\",\"fields\":[]}";

  @Test
  void loadResultFallsBackWhenSchemaFieldsMissing() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setSchemaJson("{\"type\":\"struct\"}")
            .putProperties("location", "s3://bucket/orders")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/00000.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertNotNull(result.metadata());
    List<Map<String, Object>> schemas = result.metadata().schemas();
    assertFalse(schemas.isEmpty(), "Expected at least one schema");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?> list && !list.isEmpty(), "Expected placeholder fields");
  }

  @Test
  void createRequestWithoutFieldsUsesPlaceholderSchema() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .build();
    TableRequests.Create request =
        new TableRequests.Create(
            "orders", EMPTY_SCHEMA_JSON, null, null, Map.of(), null, null, null);

    LoadTableResultDto loadResult =
        TableResponseMapper.toLoadResultFromCreate("orders", table, request, Map.of(), List.of());

    List<Map<String, Object>> schemas = loadResult.metadata().schemas();
    assertFalse(schemas.isEmpty(), "schema list should not be empty");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?> list && !list.isEmpty(), "Expected placeholder fields");
  }

  @Test
  void statisticsBlobMetadataDefaultsToArray() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/00000.metadata.json")
            .addStatistics(
                IcebergStatisticsFile.newBuilder()
                    .setSnapshotId(5)
                    .setStatisticsPath("s3://stats")
                    .setFileSizeInBytes(128)
                    .build())
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    List<Map<String, Object>> statistics = result.metadata().statistics();
    assertFalse(statistics.isEmpty());
    Object blobMetadata = statistics.get(0).get("blob-metadata");
    assertTrue(blobMetadata instanceof List<?>);
  }
}
