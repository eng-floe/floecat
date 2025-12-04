package ai.floedb.metacat.gateway.iceberg.rest.api.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public final class MetricsRequests {
  private MetricsRequests() {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Report(
      @JsonProperty("report-type") String reportType,
      @JsonProperty("table-name") String tableName,
      @JsonProperty("snapshot-id") Long snapshotId,
      @JsonProperty("sequence-number") Long sequenceNumber,
      @JsonProperty("operation") String operation,
      @JsonProperty("schema-id") Integer schemaId,
      @JsonProperty("projected-field-ids") List<Integer> projectedFieldIds,
      @JsonProperty("projected-field-names") List<String> projectedFieldNames,
      @JsonProperty("filter") Map<String, Object> filter,
      Map<String, MetricValue> metrics,
      Map<String, String> metadata) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record MetricValue(
      String unit,
      Long value,
      @JsonProperty("time-unit") String timeUnit,
      Long count,
      @JsonProperty("total-duration") Long totalDuration) {}
}
