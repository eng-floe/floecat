package ai.floedb.floecat.service.repo.model;

public record SnapshotKey(String accountId, String tableId, long snapshotId)
    implements ResourceKey {}
