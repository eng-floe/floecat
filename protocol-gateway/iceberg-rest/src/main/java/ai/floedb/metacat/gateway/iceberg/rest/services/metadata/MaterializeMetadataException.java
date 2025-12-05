package ai.floedb.metacat.gateway.iceberg.rest.services.metadata;

/** Indicates a failure while materializing Iceberg metadata files to external storage. */
public class MaterializeMetadataException extends RuntimeException {
  public MaterializeMetadataException(String message) {
    super(message);
  }

  public MaterializeMetadataException(String message, Throwable cause) {
    super(message, cause);
  }
}
