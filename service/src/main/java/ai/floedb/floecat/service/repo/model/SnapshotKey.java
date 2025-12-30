package ai.floedb.floecat.service.repo.model;

public record SnapshotKey(String accountId, String tableId, long snapshotId, String sha256)
    implements ResourceKey {
  public SnapshotKey(String accountId, String tableId, long snapshotId) {
    this(accountId, tableId, snapshotId, "");
  }
}
