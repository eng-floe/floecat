package ai.floedb.floecat.service.repo.model;

public record TableStatsKey(String tenantId, String tableId, long snapshotId, String sha256)
    implements ResourceKey {}
