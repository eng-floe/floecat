package ai.floedb.metacat.storage.errors;

public final class StorageNotFoundException extends StorageException {
  public StorageNotFoundException(String message) {
    super(message);
  }
}
