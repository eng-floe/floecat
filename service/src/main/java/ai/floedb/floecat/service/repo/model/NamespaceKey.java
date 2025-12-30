package ai.floedb.floecat.service.repo.model;

public record NamespaceKey(String accountId, String namespaceId, String sha256)
    implements ResourceKey {
  public NamespaceKey(String accountId, String namespaceId) {
    this(accountId, namespaceId, "");
  }
}
