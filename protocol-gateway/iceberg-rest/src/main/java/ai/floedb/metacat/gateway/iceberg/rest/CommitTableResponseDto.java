package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommitTableResponseDto(
    @JsonProperty("metadata-location") String metadataLocation,
    @JsonProperty("metadata") TableMetadataView metadata) {}
