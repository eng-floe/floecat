package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

/** Represents a single schema revision from a snapshot. */
public record SchemaHistoryDto(
    long snapshotId, String schemaJson, String upstreamCreatedAt, String ingestedAt) {}
