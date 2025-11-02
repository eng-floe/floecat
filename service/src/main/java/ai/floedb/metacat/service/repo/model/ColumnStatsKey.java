package ai.floedb.metacat.service.repo.model;

public record ColumnStatsKey(
    String tenantId, String tableId, long snapshotId, String columnId, String sha256)
    implements ResourceKey {}
