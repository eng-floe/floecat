package ai.floedb.metacat.connector.spi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ConnectorConfig(
    Kind kind,
    String displayName,
    String destinationTenantId,
    String destinationCatalogDisplayName,
    List<List<String>> destinationNamespacePaths,
    String destinationTableDisplayName,
    List<String> destinationTableColumns,
    String uri,
    Map<String, String> options,
    Auth auth) {

  public ConnectorConfig {
    Objects.requireNonNull(kind);
    Objects.requireNonNull(displayName);
    Objects.requireNonNull(destinationCatalogDisplayName);
    Objects.requireNonNull(uri);

    options = options == null ? Map.of() : Collections.unmodifiableMap(options);
    auth = auth == null ? new Auth("none", Map.of(), Map.of(), "") : auth;
    destinationTenantId = Objects.requireNonNullElse(destinationTenantId, "");
    destinationNamespacePaths =
        destinationNamespacePaths == null
            ? List.of()
            : deepImmutableCopy(destinationNamespacePaths);
    destinationTableDisplayName = Objects.requireNonNullElse(destinationTableDisplayName, "");
    destinationTableColumns =
        destinationTableColumns == null ? List.of() : List.copyOf(destinationTableColumns);
  }

  private static List<List<String>> deepImmutableCopy(List<List<String>> src) {
    return src.stream()
        .map(inner -> inner == null ? List.<String>of() : List.copyOf(inner))
        .toList();
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
