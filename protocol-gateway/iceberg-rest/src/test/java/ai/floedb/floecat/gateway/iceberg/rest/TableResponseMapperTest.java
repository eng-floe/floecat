package ai.floedb.floecat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
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
    assertEquals("s3://bucket/orders/metadata", result.config().get("write.metadata.path"));
    assertEquals(
        "s3://bucket/orders/metadata", result.metadata().properties().get("write.metadata.path"));
    List<Map<String, Object>> schemas = result.metadata().schemas();
    assertFalse(schemas.isEmpty(), "Expected at least one schema");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?> list && !list.isEmpty(), "Expected placeholder fields");
  }

  @Test
  void writeMetadataPathStripsMirrorPrefix() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties(
                "metadata-location",
                "s3://bucket/.floecat-metadata/orders/metadata/00000.metadata.json")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation(
                "s3://bucket/.floecat-metadata/orders/metadata/00000.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata", result.config().get("write.metadata.path"));
    assertEquals(
        "s3://bucket/orders/metadata",
        result.metadata().properties().get("write.metadata.path"));
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

    assertEquals("s3://bucket/orders/metadata", loadResult.config().get("write.metadata.path"));
    assertEquals(
        "s3://bucket/orders/metadata",
        loadResult.metadata().properties().get("write.metadata.path"));
    List<Map<String, Object>> schemas = loadResult.metadata().schemas();
    assertFalse(schemas.isEmpty(), "schema list should not be empty");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?> list && !list.isEmpty(), "Expected placeholder fields");
  }

  @Test
  void metadataLocationPrefersIcebergMetadataWhenPropertyIsPointer() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .putProperties("metadata-location", "s3://bucket/orders/metadata.json")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/00001-abc.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata", result.config().get("write.metadata.path"));
    assertEquals(
        "s3://bucket/orders/metadata", result.metadata().properties().get("write.metadata.path"));
  }

  @Test
  void metadataLocationUsesIcebergMetadataWhenTablePropertyDiffers() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .putProperties(
                "metadata-location", "s3://bucket/orders/metadata/00002-new.metadata.json")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/00001-old.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata/00001-old.metadata.json", result.metadataLocation());
    assertEquals("s3://bucket/orders/metadata", result.config().get("write.metadata.path"));
  }

  @Test
  void metadataLocationFallsBackToLatestMetadataLogEntry() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .putProperties("metadata-location", "s3://bucket/orders/metadata/00000-old.metadata.json")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .addMetadataLog(
                IcebergMetadataLogEntry.newBuilder()
                    .setFile("s3://bucket/orders/metadata/00000-old.metadata.json")
                    .build())
            .addMetadataLog(
                IcebergMetadataLogEntry.newBuilder()
                    .setFile("s3://bucket/orders/metadata/00001-new.metadata.json")
                    .build())
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata/00001-new.metadata.json", result.metadataLocation());
    assertEquals(
        "s3://bucket/orders/metadata", result.metadata().properties().get("write.metadata.path"));
  }

  @Test
  void metadataLocationPrefersMetadataLogOverStaleMetadataLocationField() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/00000-old.metadata.json")
            .addMetadataLog(
                IcebergMetadataLogEntry.newBuilder()
                    .setFile("s3://bucket/orders/metadata/00000-old.metadata.json")
                    .build())
            .addMetadataLog(
                IcebergMetadataLogEntry.newBuilder()
                    .setFile("s3://bucket/orders/metadata/00001-new.metadata.json")
                    .build())
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void metadataJsonPointerStillProducesMetadataDirectory() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties("location", "s3://bucket/orders")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://bucket/orders/metadata/metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals("s3://bucket/orders/metadata", result.config().get("write.metadata.path"));
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
