package ai.floedb.metacat.gateway.iceberg.rest.api.request;

import ai.floedb.metacat.gateway.iceberg.rest.support.serialization.NamespaceListDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

/** Minimal DTOs for Iceberg namespace requests. */
public final class NamespaceRequests {
  private NamespaceRequests() {}

  public record Create(
      @JsonProperty("namespace") @JsonDeserialize(using = NamespaceListDeserializer.class)
          List<String> namespace,
      String description,
      Map<String, String> properties,
      String policyRef) {}

  public record Update(String description, Map<String, String> properties, String policyRef) {}
}
