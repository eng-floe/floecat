package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LoadTableResultDto(
    @JsonProperty("metadata-location") String metadataLocation,
    @JsonProperty("metadata") TableMetadataView metadata,
    @JsonProperty("config") Map<String, String> config,
    @JsonProperty("storage-credentials") List<StorageCredentialDto> storageCredentials) {}
