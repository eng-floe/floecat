package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionCommitRequest(
    @JsonProperty("table-changes") List<TableChange> tableChanges,
    @JsonProperty("staged-ref-updates") List<StagedRefUpdate> stagedRefUpdates,
    @JsonProperty("requirements") List<Map<String, Object>> requirements,
    @JsonProperty("updates") List<Map<String, Object>> updates) {

  public List<TableChange> resolvedTableChanges() {
    if (tableChanges != null && !tableChanges.isEmpty()) {
      return tableChanges;
    }
    if (stagedRefUpdates == null || stagedRefUpdates.isEmpty()) {
      return List.of();
    }
    List<TableChange> converted = new ArrayList<>();
    for (StagedRefUpdate staged : stagedRefUpdates) {
      converted.add(
          new TableChange(
              staged.table(), staged.stageId(), staged.requirements(), staged.updates()));
    }
    return converted;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableChange(
      @JsonProperty("identifier") @JsonAlias("table") TableIdentifierDto identifier,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record StagedRefUpdate(
      @JsonProperty("table") TableIdentifierDto table,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}
}
