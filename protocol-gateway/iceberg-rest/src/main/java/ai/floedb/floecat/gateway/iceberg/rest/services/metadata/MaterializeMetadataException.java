package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

public class MaterializeMetadataException extends RuntimeException {
  public MaterializeMetadataException(String message) {
    super(message);
  }

  public MaterializeMetadataException(String message, Throwable cause) {
    super(message, cause);
  }
}
