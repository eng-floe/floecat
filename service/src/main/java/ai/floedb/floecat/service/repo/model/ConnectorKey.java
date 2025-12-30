package ai.floedb.floecat.service.repo.model;

public record ConnectorKey(String accountId, String connectorId, String sha256)
    implements ResourceKey {
  public ConnectorKey(String accountId, String connectorId) {
    this(accountId, connectorId, "");
  }
}
