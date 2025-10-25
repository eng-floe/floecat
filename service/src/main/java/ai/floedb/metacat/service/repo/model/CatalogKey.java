package ai.floedb.metacat.service.repo.model;

public record CatalogKey(String tenantId, String catalogId) implements ResourceKey {}
