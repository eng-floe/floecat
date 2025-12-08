package ai.floedb.floecat.storage.errors;

public final class StorageNotFoundException extends StorageException {
  public StorageNotFoundException(String message) {
    super(message);
  }
}
