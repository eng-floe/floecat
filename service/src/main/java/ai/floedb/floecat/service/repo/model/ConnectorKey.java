package ai.floedb.floecat.service.repo.model;

public record ConnectorKey(String accountId, String connectorId) implements ResourceKey {}
