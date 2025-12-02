package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CredentialsResponseDto(
    @JsonProperty("storage-credentials") List<StorageCredentialDto> storageCredentials) {}

@JsonInclude(JsonInclude.Include.NON_NULL)
record StorageCredentialDto(String prefix, Map<String, String> config) {}
