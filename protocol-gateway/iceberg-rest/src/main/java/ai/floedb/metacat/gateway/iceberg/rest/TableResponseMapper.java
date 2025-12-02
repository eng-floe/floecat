package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.IcebergBlobMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergMetadataLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.metacat.catalog.rpc.IcebergSchema;
import ai.floedb.metacat.catalog.rpc.IcebergSnapshotLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.IcebergStatisticsFile;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class TableResponseMapper {
  private static final ObjectMapper JSON = new ObjectMapper();
  private TableResponseMapper() {}

  static LoadTableResultDto toLoadResult(
      String tableName,
      Table table,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation = resolveMetadataLocation(props, metadata);
    TableMetadataView metadataView =
        toMetadata(tableName, table, props, metadata, snapshots, metadataLocation);
    return new LoadTableResultDto(
        metadataLocation, metadataView, configOverrides, storageCredentials);
  }

  static CommitTableResponseDto toCommitResponse(
      String tableName, Table table, IcebergMetadata metadata, List<Snapshot> snapshots) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation = resolveMetadataLocation(props, metadata);
    return new CommitTableResponseDto(
        metadataLocation,
        toMetadata(tableName, table, props, metadata, snapshots, metadataLocation));
  }

  private static String resolveMetadataLocation(
      Map<String, String> props, IcebergMetadata metadata) {
    if (metadata != null && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return props.getOrDefault("metadata-location", props.get("metadata_location"));
  }

  private static TableMetadataView toMetadata(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      String metadataLocation) {
    String location = table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    location = resolveTableLocation(location, metadataLocation);
    Long lastUpdatedMs =
        table.hasCreatedAt()
            ? table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    Long currentSnapshotId = maybeLong(props.get("current-snapshot-id"));
    if ((currentSnapshotId == null || currentSnapshotId <= 0) && metadata != null) {
      currentSnapshotId = metadata.getCurrentSnapshotId();
    }
    Long lastSequenceNumber = maybeLong(props.get("last-sequence-number"));
    if ((lastSequenceNumber == null || lastSequenceNumber <= 0) && metadata != null) {
      lastSequenceNumber = metadata.getLastSequenceNumber();
    }
    Integer lastColumnId = maybeInt(props.get("last-column-id"));
    if ((lastColumnId == null || lastColumnId <= 0) && metadata != null) {
      lastColumnId = metadata.getLastColumnId();
    }
    Integer currentSchemaId = maybeInt(props.get("current-schema-id"));
    if ((currentSchemaId == null || currentSchemaId <= 0) && metadata != null) {
      currentSchemaId = metadata.getCurrentSchemaId();
    }
    Integer defaultSpecId = maybeInt(props.get("default-spec-id"));
    if ((defaultSpecId == null || defaultSpecId <= 0) && metadata != null) {
      defaultSpecId = metadata.getDefaultSpecId();
    }
    Integer lastPartitionId = maybeInt(props.get("last-partition-id"));
    if ((lastPartitionId == null || lastPartitionId <= 0) && metadata != null) {
      lastPartitionId = metadata.getLastPartitionId();
    }
    Integer defaultSortOrderId = maybeInt(props.get("default-sort-order-id"));
    if ((defaultSortOrderId == null || defaultSortOrderId <= 0) && metadata != null) {
      defaultSortOrderId = metadata.getDefaultSortOrderId();
    }
    String tableUuid =
        Optional.ofNullable(props.get("table-uuid"))
            .orElseGet(() -> table.hasResourceId() ? table.getResourceId().getId() : tableName);
    Integer formatVersion = maybeInt(props.get("format-version"));
    if (formatVersion == null) {
      formatVersion = 2;
    }
    return new TableMetadataView(
        formatVersion,
        tableUuid,
        location,
        metadataLocation,
        lastUpdatedMs,
        props,
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        currentSnapshotId,
        lastSequenceNumber,
        schemas(metadata),
        specs(metadata),
        sortOrders(metadata),
        refs(metadata),
        snapshotLog(metadata),
        metadataLog(metadata),
        statistics(metadata),
        partitionStatistics(metadata),
        snapshots(snapshots));
  }

  private static List<Map<String, Object>> schemas(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSchema schema : metadata.getSchemasList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("schema-id", schema.getSchemaId());
      Object schemaObj = parseSchema(schema.getSchemaJson());
      if (schemaObj instanceof Map<?, ?> mapObj) {
        entry.putAll((Map<? extends String, ?>) mapObj);
      }
      if (schema.getIdentifierFieldIdsCount() > 0) {
        entry.put("identifier-field-ids", schema.getIdentifierFieldIdsList());
      }
      if (schema.getLastColumnId() > 0) {
        entry.put("last-column-id", schema.getLastColumnId());
      }
      out.add(entry);
    }
    return out;
  }

  private static Object parseSchema(String json) {
    if (json == null || json.isBlank()) {
      return Map.of();
    }
    try {
      return JSON.readValue(json, Object.class);
    } catch (JsonProcessingException e) {
      return Map.of();
    }
  }

  private static List<Map<String, Object>> specs(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (PartitionSpecInfo spec : metadata.getPartitionSpecsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("spec-id", spec.getSpecId());
      if (!spec.getSpecName().isBlank()) {
        entry.put("spec-name", spec.getSpecName());
      }
      List<Map<String, Object>> fields = new ArrayList<>();
      for (PartitionField field : spec.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("field-id", field.getFieldId());
        f.put("name", field.getName());
        f.put("transform", field.getTransform());
        fields.add(f);
      }
      entry.put("fields", fields);
      out.add(entry);
    }
    return out;
  }

  private static String resolveTableLocation(String location, String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return location;
    }
    if (location != null && !location.isBlank() && !location.startsWith("https://glue")) {
      return location;
    }
    int idx = metadataLocation.indexOf("/metadata/");
    if (idx > 0) {
      return metadataLocation.substring(0, idx);
    }
    int slash = metadataLocation.lastIndexOf('/');
    if (slash > 0) {
      return metadataLocation.substring(0, slash);
    }
    return metadataLocation;
  }

  private static List<Map<String, Object>> sortOrders(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSortOrder order : metadata.getSortOrdersList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("order-id", order.getSortOrderId());
      List<Map<String, Object>> fields = new ArrayList<>();
      for (IcebergSortField field : order.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("source-field-id", field.getSourceFieldId());
        f.put("transform", field.getTransform());
        f.put("direction", field.getDirection());
        f.put("null-order", field.getNullOrder());
        fields.add(f);
      }
      entry.put("fields", fields);
      out.add(entry);
    }
    return out;
  }

  private static Map<String, Object> refs(IcebergMetadata metadata) {
    if (metadata == null || metadata.getRefsCount() == 0) {
      return Map.of();
    }
    Map<String, Object> out = new LinkedHashMap<>();
    metadata
        .getRefsMap()
        .forEach(
            (name, ref) -> {
              Map<String, Object> entry = new LinkedHashMap<>();
              entry.put("snapshot-id", ref.getSnapshotId());
              entry.put("type", ref.getType());
              if (ref.hasMaxReferenceAgeMs()) {
                entry.put("max-reference-age-ms", ref.getMaxReferenceAgeMs());
              }
              if (ref.hasMaxSnapshotAgeMs()) {
                entry.put("max-snapshot-age-ms", ref.getMaxSnapshotAgeMs());
              }
              if (ref.hasMinSnapshotsToKeep()) {
                entry.put("min-snapshots-to-keep", ref.getMinSnapshotsToKeep());
              }
              out.put(name, entry);
            });
    return out;
  }

  private static List<Map<String, Object>> snapshotLog(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSnapshotLogEntry entry : metadata.getSnapshotLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("snapshot-id", entry.getSnapshotId());
      out.add(log);
    }
    return out;
  }

  private static List<Map<String, Object>> metadataLog(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergMetadataLogEntry entry : metadata.getMetadataLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("metadata-file", entry.getFile());
      out.add(log);
    }
    return out;
  }

  private static List<Map<String, Object>> statistics(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergStatisticsFile file : metadata.getStatisticsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", file.getSnapshotId());
      entry.put("statistics-path", file.getStatisticsPath());
      entry.put("file-size-in-bytes", file.getFileSizeInBytes());
      entry.put("file-footer-size-in-bytes", file.getFileFooterSizeInBytes());
      if (!file.getBlobMetadataList().isEmpty()) {
        List<Map<String, Object>> blobs = new ArrayList<>();
        for (IcebergBlobMetadata blob : file.getBlobMetadataList()) {
          Map<String, Object> blobEntry = new LinkedHashMap<>();
          blobEntry.put("type", blob.getType());
          blobEntry.put("snapshot-id", blob.getSnapshotId());
          blobEntry.put("sequence-number", blob.getSequenceNumber());
          blobEntry.put("fields", blob.getFieldsList());
          if (!blob.getPropertiesMap().isEmpty()) {
            blobEntry.put("properties", blob.getPropertiesMap());
          }
          blobs.add(blobEntry);
        }
        entry.put("blob-metadata", blobs);
      }
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> partitionStatistics(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergPartitionStatisticsFile file : metadata.getPartitionStatisticsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", file.getSnapshotId());
      entry.put("statistics-path", file.getStatisticsPath());
      entry.put("file-size-in-bytes", file.getFileSizeInBytes());
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> snapshots(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", snapshot.getSnapshotId());
      if (snapshot.getParentSnapshotId() > 0) {
        entry.put("parent-snapshot-id", snapshot.getParentSnapshotId());
      }
      long sequenceNumber = snapshot.getSequenceNumber();
      if (sequenceNumber > 0) {
        entry.put("sequence-number", sequenceNumber);
      }
      if (snapshot.hasUpstreamCreatedAt()) {
        entry.put("timestamp-ms", snapshot.getUpstreamCreatedAt().getSeconds() * 1000L);
      } else if (snapshot.hasIngestedAt()) {
        entry.put("timestamp-ms", snapshot.getIngestedAt().getSeconds() * 1000L);
      }
      if (!snapshot.getManifestList().isBlank()) {
        entry.put("manifest-list", snapshot.getManifestList());
      }
      int schemaId = snapshot.getSchemaId();
      if (schemaId > 0) {
        entry.put("schema-id", schemaId);
      }
      if (!snapshot.getSummaryMap().isEmpty()) {
        entry.put("summary", snapshot.getSummaryMap());
      }
      out.add(entry);
    }
    return out;
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
