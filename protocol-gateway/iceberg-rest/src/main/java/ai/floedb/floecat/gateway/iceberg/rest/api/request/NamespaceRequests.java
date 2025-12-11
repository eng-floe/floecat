package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceListDeserializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

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
