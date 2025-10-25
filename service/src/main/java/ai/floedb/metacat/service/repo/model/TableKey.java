package ai.floedb.metacat.service.repo.model;

public record TableKey(String tenantId, String tableId) implements ResourceKey {}
