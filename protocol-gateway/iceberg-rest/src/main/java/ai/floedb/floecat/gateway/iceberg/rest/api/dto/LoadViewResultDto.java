package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LoadViewResultDto(
    @JsonProperty("metadata-location") String metadataLocation,
    @JsonProperty("metadata") ViewMetadataView metadata,
    @JsonProperty("config") Map<String, String> config) {}
