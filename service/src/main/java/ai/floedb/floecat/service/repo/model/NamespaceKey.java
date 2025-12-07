package ai.floedb.floecat.service.repo.model;

public record NamespaceKey(String tenantId, String namespaceId) implements ResourceKey {}
