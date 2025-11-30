package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableMetadataView(
    @JsonProperty("format-version") Integer formatVersion,
    @JsonProperty("table-uuid") String tableUuid,
    @JsonProperty("location") String location,
    @JsonProperty("last-updated-ms") Long lastUpdatedMs,
    @JsonProperty("properties") Map<String, String> properties,
    @JsonProperty("current-snapshot-id") Long currentSnapshotId,
    @JsonProperty("last-sequence-number") Long lastSequenceNumber) {}
