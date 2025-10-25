package ai.floedb.metacat.service.repo.model;

public record SnapshotKey(String tenantId, String tableId, long snapshotId)
    implements ResourceKey {}
