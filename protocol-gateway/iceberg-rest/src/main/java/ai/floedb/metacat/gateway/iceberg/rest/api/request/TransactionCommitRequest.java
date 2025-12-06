package ai.floedb.metacat.gateway.iceberg.rest.api.request;

import ai.floedb.metacat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionCommitRequest(
    @JsonProperty("staged-ref-updates") List<StagedRefUpdate> stagedRefUpdates,
    @JsonProperty("requirements") List<Map<String, Object>> requirements,
    @JsonProperty("updates") List<Map<String, Object>> updates) {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record StagedRefUpdate(
      @JsonProperty("table") TableIdentifierDto table,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}
}
