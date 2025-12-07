package ai.floedb.floecat.service.repo.model;

public record NamespaceKey(String accountId, String namespaceId) implements ResourceKey {}
