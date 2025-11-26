package ai.floedb.metacat.trino;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public record MetacatConnectorConfig(
    String endpoint, Optional<String> tenantId, Map<String, String> s3Properties) {

  static MetacatConnectorConfig from(Map<String, String> props) {
    String endpoint = props.getOrDefault("metacat.endpoint", "").trim();
    if (endpoint.isEmpty()) {
      throw new IllegalArgumentException("metacat.endpoint is required");
    }
    Optional<String> tenant =
        Optional.ofNullable(props.get("metacat.tenant-id"))
            .map(String::trim)
            .filter(s -> !s.isEmpty());

    Map<String, String> s3Props =
        props.entrySet().stream()
            .filter(e -> e.getKey().startsWith("s3.") || e.getKey().startsWith("fs.native-s3."))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new MetacatConnectorConfig(endpoint, tenant, s3Props);
  }
}
