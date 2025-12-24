package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableResponseMapperTest {

  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TableMetadataView FIXTURE_METADATA_VIEW =
      TableMetadataBuilder.fromCatalog(
          FIXTURE.table().getDisplayName(),
          FIXTURE.table(),
          new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
          FIXTURE.metadata(),
          FIXTURE.snapshots());

  @Test
  void loadResultUsesMetadataSchema() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .build();
    IcebergMetadata metadata = FIXTURE.metadata();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertNotNull(result.metadata());
    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata", result.config().get("write.metadata.path"));
    assertFalse(
        result.metadata().properties().containsKey("write.metadata.path"),
        "Original metadata should remain unchanged");
    List<Map<String, Object>> schemas = result.metadata().schemas();
    assertFalse(schemas.isEmpty(), "Expected at least one schema");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?>, "Expected fixture fields");
    List<?> list = (List<?>) fields;
    assertFalse(list.isEmpty(), "Expected fixture fields");
    @SuppressWarnings("unchecked")
    Map<String, Object> field = (Map<String, Object>) list.get(0);
    assertEquals("i", field.get("name"));
  }

  @Test
  void writeMetadataPathStripsMirrorPrefix() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties(
                "metadata-location",
                "s3://yb-iceberg-tpcds/.floecat-metadata/trino_test/metadata/00000-abc.metadata.json")
            .build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation(
                "s3://yb-iceberg-tpcds/.floecat-metadata/trino_test/metadata/00000-abc.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata", result.config().get("write.metadata.path"));
    assertFalse(
        result.metadata().properties().containsKey("write.metadata.path"),
        "Original metadata should remain unchanged");
  }

  @Test
  void createRequestUsesSchemaJson() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .build();
    String formatVersion = FIXTURE.table().getPropertiesMap().get("format-version");
    String location = FIXTURE.table().getPropertiesMap().get("location");
    if (formatVersion == null || formatVersion.isBlank()) {
      throw new IllegalStateException("fixture format-version is required");
    }
    if (location == null || location.isBlank()) {
      throw new IllegalStateException("fixture location is required");
    }
    Map<String, String> props =
        Map.of(
            "metadata-location",
            FIXTURE.metadataLocation(),
            "format-version",
            formatVersion,
            "last-updated-ms",
            Long.toString(FIXTURE.metadata().getLastUpdatedMs()));
    TableRequests.Create request =
        new TableRequests.Create(
            "orders",
            fixtureSchemaJson(),
            null,
            location,
            props,
            fixturePartitionSpec(),
            fixtureWriteOrder(),
            null);

    LoadTableResultDto loadResult =
        TableResponseMapper.toLoadResultFromCreate("orders", table, request, Map.of(), List.of());

    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata",
        loadResult.config().get("write.metadata.path"));
    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata",
        loadResult.metadata().properties().get("write.metadata.path"));
    List<Map<String, Object>> schemas = loadResult.metadata().schemas();
    assertFalse(schemas.isEmpty(), "schema list should not be empty");
    Object fields = schemas.get(0).get("fields");
    assertTrue(fields instanceof List<?>, "Expected fixture fields");
    List<?> list = (List<?>) fields;
    assertFalse(list.isEmpty(), "Expected fixture fields");
    @SuppressWarnings("unchecked")
    Map<String, Object> field = (Map<String, Object>) list.get(0);
    assertEquals("i", field.get("name"));
  }

  @Test
  void metadataLocationPrefersIcebergMetadataWhenPropertyIsPointer() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties(
                "metadata-location",
                "s3://yb-iceberg-tpcds/trino_test/metadata/00000-abc.metadata.json")
            .build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation(
                "s3://yb-iceberg-tpcds/trino_test/metadata/00001-abc.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata", result.config().get("write.metadata.path"));
    assertFalse(
        result.metadata().properties().containsKey("write.metadata.path"),
        "Original metadata should remain unchanged");
  }

  @Test
  void metadataLocationUsesIcebergMetadataWhenTablePropertyDiffers() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .putProperties(
                "metadata-location",
                "s3://yb-iceberg-tpcds/trino_test/metadata/00002-new.metadata.json")
            .build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation(
                "s3://yb-iceberg-tpcds/trino_test/metadata/00001-old.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata/00001-old.metadata.json",
        result.metadataLocation());
    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata", result.config().get("write.metadata.path"));
  }

  @Test
  void metadataLocationProducesMetadataDirectory() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation(
                "s3://yb-iceberg-tpcds/trino_test/metadata/00000-abc.metadata.json")
            .build();

    LoadTableResultDto result =
        TableResponseMapper.toLoadResult("orders", table, metadata, List.of(), Map.of(), List.of());

    assertEquals(
        "s3://yb-iceberg-tpcds/trino_test/metadata", result.config().get("write.metadata.path"));
  }

  @Test
  void statisticsBlobMetadataDefaultsToArray() {
    Table table =
        FIXTURE.table().toBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation(
                "s3://yb-iceberg-tpcds/trino_test/metadata/00000-abc.metadata.json")
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

  private static String fixtureSchemaJson() {
    Map<String, Object> schema =
        selectById(
            FIXTURE_METADATA_VIEW.schemas(),
            "schema-id",
            FIXTURE_METADATA_VIEW.currentSchemaId(),
            "fixture schema");
    Integer schemaId = FIXTURE_METADATA_VIEW.currentSchemaId();
    if (!schema.containsKey("schema-id")) {
      if (schemaId == null) {
        throw new IllegalStateException("fixture schema requires schema-id");
      }
      schema.put("schema-id", schemaId);
    }
    Integer lastColumnId = FIXTURE_METADATA_VIEW.lastColumnId();
    if (!schema.containsKey("last-column-id")) {
      if (lastColumnId == null) {
        throw new IllegalStateException("fixture schema requires last-column-id");
      }
      schema.put("last-column-id", lastColumnId);
    }
    Map<String, Object> payload = new LinkedHashMap<>(schema);
    try {
      return JSON.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to build schema-json", e);
    }
  }

  private static JsonNode fixturePartitionSpec() {
    Map<String, Object> spec =
        selectById(
            FIXTURE_METADATA_VIEW.partitionSpecs(),
            "spec-id",
            FIXTURE_METADATA_VIEW.defaultSpecId(),
            "fixture partition spec");
    return JSON.valueToTree(spec);
  }

  private static JsonNode fixtureWriteOrder() {
    Map<String, Object> order =
        selectById(
            FIXTURE_METADATA_VIEW.sortOrders(),
            "order-id",
            FIXTURE_METADATA_VIEW.defaultSortOrderId(),
            "fixture sort order");
    return JSON.valueToTree(order);
  }

  private static Map<String, Object> selectById(
      List<Map<String, Object>> candidates, String key, Integer targetId, String label) {
    if (candidates == null || candidates.isEmpty()) {
      throw new IllegalStateException(label + " list is empty");
    }
    if (targetId != null) {
      for (Map<String, Object> candidate : candidates) {
        if (candidate == null) {
          continue;
        }
        Integer value = asInteger(candidate.get(key));
        if (value != null && value.equals(targetId)) {
          return candidate;
        }
      }
    }
    throw new IllegalStateException(label + " not found for " + key + "=" + targetId);
  }

  private static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text) {
      try {
        return Integer.parseInt(text);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }
}
