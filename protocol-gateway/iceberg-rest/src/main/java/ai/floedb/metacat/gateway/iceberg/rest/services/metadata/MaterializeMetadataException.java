package ai.floedb.metacat.gateway.iceberg.rest.services.metadata;

public class MaterializeMetadataException extends RuntimeException {
  public MaterializeMetadataException(String message) {
    super(message);
  }

  public MaterializeMetadataException(String message, Throwable cause) {
    super(message, cause);
  }
}
