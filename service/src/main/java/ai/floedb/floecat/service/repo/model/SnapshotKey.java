package ai.floedb.floecat.service.repo.model;

public record SnapshotKey(String tenantId, String tableId, long snapshotId)
    implements ResourceKey {}
