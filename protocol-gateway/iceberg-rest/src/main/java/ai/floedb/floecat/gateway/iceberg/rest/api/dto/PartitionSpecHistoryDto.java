package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

public record PartitionSpecHistoryDto(
    long snapshotId, PartitionSpecDto partitionSpec, String upstreamCreatedAt, String ingestedAt) {}
