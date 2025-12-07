package ai.floedb.floecat.service.repo.model;

public record ConnectorKey(String tenantId, String connectorId) implements ResourceKey {}
