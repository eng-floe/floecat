package ai.floedb.metacat.storage.errors;

public final class StoragePreconditionFailedException extends StorageException {
  public StoragePreconditionFailedException(String message) {
    super(message);
  }
}
