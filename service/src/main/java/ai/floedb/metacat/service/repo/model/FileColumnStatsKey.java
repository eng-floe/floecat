package ai.floedb.metacat.service.repo.model;

public record FileColumnStatsKey(
    String tenantId, String tableId, long snapshotId, String filePath, String sha256)
    implements ResourceKey {}
