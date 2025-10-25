package ai.floedb.metacat.service.repo.model;

public record NamespaceKey(String tenantId, String namespaceId) implements ResourceKey {}
