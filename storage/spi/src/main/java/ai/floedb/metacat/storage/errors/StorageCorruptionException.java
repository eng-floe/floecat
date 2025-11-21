package ai.floedb.metacat.storage.errors;

public final class StorageCorruptionException extends StorageException {
  public StorageCorruptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
