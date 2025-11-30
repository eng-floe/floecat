package ai.floedb.metacat.gateway.iceberg.rest;

public record SnapshotDto(
    String tableId,
    long snapshotId,
    String upstreamCreatedAt,
    String ingestedAt,
    Long parentSnapshotId,
    String schemaJson,
    PartitionSpecDto partitionSpec) {}
