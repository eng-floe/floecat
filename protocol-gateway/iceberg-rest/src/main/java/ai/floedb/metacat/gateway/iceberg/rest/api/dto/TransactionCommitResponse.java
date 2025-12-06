package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionCommitResponse(
    @JsonProperty("results") List<TransactionCommitResult> results) {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TransactionCommitResult(
      @JsonProperty("table") TableIdentifierDto table,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("metadata-location") String metadataLocation,
      @JsonProperty("metadata") TableMetadataView metadata,
      @JsonProperty("config") Map<String, String> configOverrides,
      @JsonProperty("storage-credentials") List<StorageCredentialDto> storageCredentials) {}
}
