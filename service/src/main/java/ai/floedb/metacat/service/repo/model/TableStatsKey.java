package ai.floedb.metacat.service.repo.model;

public record TableStatsKey(String tenantId, String tableId, long snapshotId)
    implements ResourceKey {}
