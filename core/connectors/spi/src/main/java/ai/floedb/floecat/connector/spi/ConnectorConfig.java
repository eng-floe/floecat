package ai.floedb.floecat.connector.spi;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public record ConnectorConfig(
    Kind kind, String displayName, String uri, Map<String, String> options, Auth auth) {

  public ConnectorConfig {
    Objects.requireNonNull(kind);
    Objects.requireNonNull(displayName);
    Objects.requireNonNull(uri);
    options = options == null ? Map.of() : Collections.unmodifiableMap(options);
    auth = auth == null ? new Auth("none", Map.of(), Map.of(), "") : auth;
  }

  public enum Kind {
    ICEBERG,
    DELTA,
    GLUE,
    UNITY
  }

  public record Auth(
      String scheme, Map<String, String> props, Map<String, String> headerHints, String secretRef) {
    public Auth {
      scheme = Objects.requireNonNullElse(scheme, "none");
      props = props == null ? Map.of() : Map.copyOf(props);
      headerHints = headerHints == null ? Map.of() : Map.copyOf(headerHints);
      secretRef = Objects.requireNonNullElse(secretRef, "");
    }
  }
}
