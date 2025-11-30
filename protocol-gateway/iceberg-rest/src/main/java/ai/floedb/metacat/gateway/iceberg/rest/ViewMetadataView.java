package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ViewMetadataView(
    @JsonProperty("view-uuid") String viewUuid,
    @JsonProperty("format-version") Integer formatVersion,
    @JsonProperty("location") String location,
    @JsonProperty("current-version-id") Integer currentVersionId,
    @JsonProperty("versions") List<ViewVersion> versions,
    @JsonProperty("version-log") List<ViewHistoryEntry> versionLog,
    @JsonProperty("schemas") List<SchemaSummary> schemas,
    @JsonProperty("properties") Map<String, String> properties) {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewVersion(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs,
      @JsonProperty("schema-id") Integer schemaId,
      @JsonProperty("summary") Map<String, String> summary,
      @JsonProperty("representations") List<ViewRepresentation> representations,
      @JsonProperty("default-namespace") List<String> defaultNamespace) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewHistoryEntry(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ViewRepresentation(
      String type, String sql, String dialect) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SchemaSummary(
      @JsonProperty("schema-id") Integer schemaId,
      String type,
      List<Map<String, Object>> fields,
      @JsonProperty("identifier-field-ids") List<Integer> identifierFieldIds) {}
}
