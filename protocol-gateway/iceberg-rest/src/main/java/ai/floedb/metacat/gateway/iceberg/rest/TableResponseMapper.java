package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.Table;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

final class TableResponseMapper {
  private TableResponseMapper() {}

  static LoadTableResultDto toLoadResult(String tableName, Table table) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation =
        props.getOrDefault("metadata-location", props.get("metadata_location"));
    TableMetadataView metadata = toMetadata(tableName, table, props);
    return new LoadTableResultDto(metadataLocation, metadata, Map.of());
  }

  static CommitTableResponseDto toCommitResponse(String tableName, Table table) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation =
        props.getOrDefault("metadata-location", props.get("metadata_location"));
    return new CommitTableResponseDto(metadataLocation, toMetadata(tableName, table, props));
  }

  private static TableMetadataView toMetadata(
      String tableName, Table table, Map<String, String> props) {
    String location = table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    Long lastUpdatedMs =
        table.hasCreatedAt()
            ? table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    Long currentSnapshotId = maybeLong(props.get("current-snapshot-id"));
    Long lastSequenceNumber = maybeLong(props.get("last-sequence-number"));
    String tableUuid =
        Optional.ofNullable(props.get("table-uuid"))
            .orElseGet(() -> table.hasResourceId() ? table.getResourceId().getId() : tableName);
    Integer formatVersion = maybeInt(props.get("format-version"));
    if (formatVersion == null) {
      formatVersion = 2;
    }
    return new TableMetadataView(
        formatVersion, tableUuid, location, lastUpdatedMs, props, currentSnapshotId, lastSequenceNumber);
  }

  private static Long maybeLong(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static Integer maybeInt(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }
}
