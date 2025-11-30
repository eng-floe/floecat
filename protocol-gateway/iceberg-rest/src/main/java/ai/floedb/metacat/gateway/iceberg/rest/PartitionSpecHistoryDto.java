package ai.floedb.metacat.gateway.iceberg.rest;

/** Captures the partition spec associated with a particular snapshot. */
public record PartitionSpecHistoryDto(
    long snapshotId, PartitionSpecDto partitionSpec, String upstreamCreatedAt, String ingestedAt) {}
