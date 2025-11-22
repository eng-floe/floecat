package ai.floedb.metacat.storage.errors;

public final class StorageAbortRetryableException extends StorageException {
  public StorageAbortRetryableException(String message) {
    super(message);
  }
}
