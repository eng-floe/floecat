package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

public record SnapshotDto(
    String tableId,
    long snapshotId,
    String upstreamCreatedAt,
    String ingestedAt,
    Long parentSnapshotId,
    String schemaJson,
    PartitionSpecDto partitionSpec) {}
