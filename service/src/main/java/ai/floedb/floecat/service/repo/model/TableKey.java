package ai.floedb.floecat.service.repo.model;

public record TableKey(String tenantId, String tableId) implements ResourceKey {}
