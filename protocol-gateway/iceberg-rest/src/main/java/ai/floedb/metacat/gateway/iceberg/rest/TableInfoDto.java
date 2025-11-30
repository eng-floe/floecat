package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;
import java.util.Map;

/**
 * Iceberg-friendly table info.
 */
public record TableInfoDto(
    TableIdentifierDto identifier,
    String schemaJson,
    List<String> partitionKeys,
    Map<String, String> properties,
    String metadataLocation,
    String location,
    Long currentSnapshotId,
    Long lastUpdatedMs) {
  public static TableInfoDto of(
      List<String> namespace,
      String name,
      String schemaJson,
      List<String> partitionKeys,
      Map<String, String> properties,
      String metadataLocation,
      String location,
      Long currentSnapshotId,
      Long lastUpdatedMs) {
    return new TableInfoDto(
        new TableIdentifierDto(namespace, name),
        schemaJson,
        partitionKeys,
        properties,
        metadataLocation,
        location,
        currentSnapshotId,
        lastUpdatedMs);
  }
}
