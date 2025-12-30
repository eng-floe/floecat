package ai.floedb.floecat.service.repo.model;

public record TableKey(String accountId, String tableId, String sha256) implements ResourceKey {
  public TableKey(String accountId, String tableId) {
    this(accountId, tableId, "");
  }
}
