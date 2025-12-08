package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SnapshotDto(
    String tableId,
    @JsonProperty("snapshot-id") Long snapshotId,
    String upstreamCreatedAt,
    String ingestedAt,
    Long parentSnapshotId,
    String schemaJson,
    PartitionSpecDto partitionSpec) {}
