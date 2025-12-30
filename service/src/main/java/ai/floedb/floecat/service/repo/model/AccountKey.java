package ai.floedb.floecat.service.repo.model;

public record AccountKey(String accountId, String sha256) implements ResourceKey {
  public AccountKey(String accountId) {
    this(accountId, "");
  }
}
