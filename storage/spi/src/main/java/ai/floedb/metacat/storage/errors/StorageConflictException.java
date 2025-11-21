package ai.floedb.metacat.storage.errors;

public final class StorageConflictException extends StorageException {
  public StorageConflictException(String message) {
    super(message);
  }
}
