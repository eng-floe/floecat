package ai.floedb.metacat.connector.spi;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class ConnectorConfig {

  public enum Kind {
    ICEBERG_REST,
    DELTA,
    GLUE,
    UNITY
  }

  public static final class Auth {
    private final String scheme;
    private final Map<String, String> props;
    private final Map<String, String> headerHints;
    private final String secretRef;

    public Auth(String scheme, Map<String, String> props,
        Map<String, String> headerHints, String secretRef) {
      this.scheme = Objects.requireNonNullElse(scheme, "none");
      this.props = props == null ? Map.of() : Map.copyOf(props);
      this.headerHints = headerHints == null ? Map.of() : Map.copyOf(headerHints);
      this.secretRef = secretRef == null ? "" : secretRef;
    }
    public String scheme() {
      return scheme;
    }

    public Map<String,String> props() {
      return props;
    }

    public Map<String,String> headerHints() {
      return headerHints;
    }

    public String secretRef() {
      return secretRef;
    }
  }

  private final Kind kind;
  private final String displayName;
  private final String targetCatalogDisplayName;
  private final String targetTenantId;
  private final String uri;
  private final Map<String, String> options;
  private final Auth auth;

  public ConnectorConfig(
      Kind kind,
      String displayName,
      String targetCatalogDisplayName,
      String targetTenantId,
      String uri,
      Map<String, String> options,
      Auth auth) {
    this.kind = Objects.requireNonNull(kind);
    this.displayName = Objects.requireNonNull(displayName);
    this.targetCatalogDisplayName = Objects.requireNonNull(targetCatalogDisplayName);
    this.targetTenantId = targetTenantId == null ? "" : targetTenantId;
    this.uri = Objects.requireNonNull(uri);
    this.options = options == null ? Map.of() : Collections.unmodifiableMap(options);
    this.auth = auth == null ? new Auth("none", Map.of(), Map.of(), "") : auth;
  }

  public Kind kind() {
    return kind;
  }

  public String displayName() {
    return displayName;
  }

  public String targetCatalogDisplayName() {
    return targetCatalogDisplayName;
  }

  public String targetTenantId() {
    return targetTenantId;
  }

  public String uri() {
    return uri;
  }

  public Map<String, String> options() {
    return options;
  }

  public Auth auth() {
    return auth;
  }
}
