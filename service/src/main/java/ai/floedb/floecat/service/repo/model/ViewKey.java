package ai.floedb.floecat.service.repo.model;

public record ViewKey(String tenantId, String viewId) implements ResourceKey {}
