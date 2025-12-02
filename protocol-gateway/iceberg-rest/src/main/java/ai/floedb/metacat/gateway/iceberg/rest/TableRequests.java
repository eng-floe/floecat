package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/** Minimal DTOs for Iceberg table requests. */
public final class TableRequests {
  private TableRequests() {}

  public record Create(String name, String schemaJson, Map<String, String> properties) {}

  public record Update(
      String name, String namespace, String schemaJson, Map<String, String> properties) {}

  public record Commit(
      String name,
      String namespace,
      String schemaJson,
      Map<String, String> properties,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}

  public record Register(
      String name, @JsonProperty("metadata-location") String metadataLocation, Boolean overwrite) {}
}
