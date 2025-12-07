package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

class SnapshotUpdateContext {
  Long lastSnapshotId;
  String materializedMetadataLocation;

  boolean hasMaterializedMetadata() {
    return materializedMetadataLocation != null && !materializedMetadataLocation.isBlank();
  }
}
