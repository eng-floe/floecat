package ai.floedb.metacat.service.repo.model;

public record ViewKey(String tenantId, String viewId) implements ResourceKey {}

