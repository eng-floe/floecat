package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

/** Minimal DTOs for Iceberg table requests. */
public final class TableRequests {
  private TableRequests() {}

  public record Create(
      String name,
      String schemaJson,
      @JsonProperty("schema") JsonNode schema,
      @JsonProperty("location") String location,
      Map<String, String> properties,
      @JsonProperty("partition-spec") JsonNode partitionSpec,
      @JsonProperty("write-order") JsonNode writeOrder,
      @JsonProperty("stage-create") Boolean stageCreate) {}

  public record Update(
      String name,
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      String schemaJson,
      Map<String, String> properties) {}

  public record Commit(
      String name,
      @JsonDeserialize(using = NamespaceListDeserializer.class) List<String> namespace,
      String schemaJson,
      Map<String, String> properties,
      @JsonProperty("stage-id") String stageId,
      @JsonProperty("requirements") List<Map<String, Object>> requirements,
      @JsonProperty("updates") List<Map<String, Object>> updates) {}

  public record Register(
      String name,
      @JsonProperty("metadata-location") String metadataLocation,
      Boolean overwrite) {}
}
