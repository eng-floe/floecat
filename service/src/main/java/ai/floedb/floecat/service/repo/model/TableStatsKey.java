package ai.floedb.floecat.service.repo.model;

public record TableStatsKey(String accountId, String tableId, long snapshotId, String sha256)
    implements ResourceKey {}
