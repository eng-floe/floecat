package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceListDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

public final class ViewRequests {
  private ViewRequests() {}

  public record Create(
      String name,
      String location,
      JsonNode schema,
      @JsonProperty("view-version") ViewVersion viewVersion,
      Map<String, String> properties) {}

  public record ViewVersion(
      @JsonProperty("version-id") Integer versionId,
      @JsonProperty("timestamp-ms") Long timestampMs,
      @JsonProperty("schema-id") Integer schemaId,
      Map<String, String> summary,
      List<ViewRepresentation> representations,
      @JsonProperty("default-namespace") List<String> defaultNamespace,
      @JsonProperty("default-catalog") String defaultCatalog) {}

  public record ViewRepresentation(String type, String sql, String dialect) {}

  public record Update(
      String name,
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      String sql,
      Map<String, String> properties) {}

  public record Commit(List<JsonNode> requirements, List<JsonNode> updates) {}
}
