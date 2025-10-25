package ai.floedb.metacat.service.repo.model;

public record ConnectorKey(String tenantId, String connectorId) implements ResourceKey {}
