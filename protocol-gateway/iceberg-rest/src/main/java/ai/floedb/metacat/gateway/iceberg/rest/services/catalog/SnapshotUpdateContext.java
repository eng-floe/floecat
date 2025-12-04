package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

class SnapshotUpdateContext {
  Long lastSnapshotId;
  String mirroredMetadataLocation;

  boolean hasMirroredMetadata() {
    return mirroredMetadataLocation != null && !mirroredMetadataLocation.isBlank();
  }
}
