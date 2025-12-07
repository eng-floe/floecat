package ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SnapshotMapper {
  private SnapshotMapper() {}

  static List<Map<String, Object>> snapshotLog(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> metadataLog(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> statistics(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> sanitizeStatistics(List<Map<String, Object>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> sanitized = new ArrayList<>(statistics.size());
    for (Map<String, Object> entry : statistics) {
      Map<String, Object> copy = entry == null ? new LinkedHashMap<>() : new LinkedHashMap<>(entry);
      Object blobs = copy.get("blob-metadata");
      if (!(blobs instanceof List<?>)) {
        copy.put("blob-metadata", List.of());
      }
      sanitized.add(copy);
    }
    return sanitized;
  }

  static List<Map<String, Object>> partitionStatistics(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> snapshots(List<Snapshot> snapshots) {
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
      if (schemaId >= 0) {
        entry.put("schema-id", schemaId);
      }
      if (!snapshot.getSummaryMap().isEmpty()) {
        entry.put("summary", snapshot.getSummaryMap());
      }
      out.add(entry);
    }
    return out;
  }

  static Map<String, Object> refs(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> nonNullMapList(List<Map<String, Object>> value) {
    if (value == null || value.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> copy = new ArrayList<>(value.size());
    for (Map<String, Object> entry : value) {
      copy.add(entry == null ? Map.of() : new LinkedHashMap<>(entry));
    }
    return copy;
  }
}
