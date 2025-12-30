package ai.floedb.floecat.service.repo.model;

public record ViewKey(String accountId, String viewId, String sha256) implements ResourceKey {
  public ViewKey(String accountId, String viewId) {
    this(accountId, viewId, "");
  }
}
