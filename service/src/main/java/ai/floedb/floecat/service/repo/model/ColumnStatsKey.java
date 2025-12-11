package ai.floedb.floecat.service.repo.model;

public record ColumnStatsKey(
    String accountId, String tableId, long snapshotId, int columnId, String sha256)
    implements ResourceKey {}
