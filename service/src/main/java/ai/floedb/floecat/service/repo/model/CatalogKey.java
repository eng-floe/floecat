package ai.floedb.floecat.service.repo.model;

public record CatalogKey(String accountId, String catalogId, String sha256) implements ResourceKey {
  public CatalogKey(String accountId, String catalogId) {
    this(accountId, catalogId, "");
  }
}
