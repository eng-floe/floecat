package ai.floedb.metacat.gateway.iceberg.rest.services.metadata;

/** Indicates a failure while mirroring Iceberg metadata files to external storage. */
public class MetadataMirrorException extends RuntimeException {
  public MetadataMirrorException(String message) {
    super(message);
  }

  public MetadataMirrorException(String message, Throwable cause) {
    super(message, cause);
  }
}
