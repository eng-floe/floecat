package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record StageCreateResponseDto(
    @JsonProperty("metadata-location") String metadataLocation,
    @JsonProperty("metadata") TableMetadataView metadata,
    @JsonProperty("config") Map<String, String> configOverrides,
    @JsonProperty("storage-credentials") List<StorageCredentialDto> storageCredentials,
    @JsonProperty("requirements") List<Map<String, Object>> requirements,
    @JsonProperty("stage-id") String stageId) {}
