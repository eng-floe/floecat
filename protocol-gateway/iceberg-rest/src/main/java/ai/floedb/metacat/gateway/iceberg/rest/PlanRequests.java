package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public final class PlanRequests {
  private PlanRequests() {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Plan(
      @JsonProperty("snapshot-id") Long snapshotId,
      @JsonProperty("start-snapshot-id") Long startSnapshotId,
      @JsonProperty("end-snapshot-id") Long endSnapshotId,
      @JsonProperty("select") List<String> select,
      Map<String, Object> filter,
      @JsonProperty("stats-fields") List<String> statsFields,
      @JsonProperty("case-sensitive") Boolean caseSensitive,
      @JsonProperty("use-snapshot-schema") Boolean useSnapshotSchema,
      @JsonProperty("min-rows-requested") Long minRowsRequested) {
    public static Plan empty() {
      return new Plan(null, null, null, null, null, null, null, null, null);
    }
  }
}
