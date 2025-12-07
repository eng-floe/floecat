package ai.floedb.floecat.service.repo.model;

public record CatalogKey(String tenantId, String catalogId) implements ResourceKey {}
