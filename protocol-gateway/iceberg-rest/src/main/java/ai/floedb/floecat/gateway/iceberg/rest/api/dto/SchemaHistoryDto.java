package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

public record SchemaHistoryDto(
    long snapshotId, String schemaJson, String upstreamCreatedAt, String ingestedAt) {}
