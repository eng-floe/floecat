package ai.floedb.metacat.connector.spi;

import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import java.util.List;

public final class ConnectorConfigMapper {
  public static ConnectorConfig fromProto(Connector c) {

    var kind =
        switch (c.getKind()) {
          case CK_ICEBERG -> ConnectorConfig.Kind.ICEBERG;
          case CK_DELTA -> ConnectorConfig.Kind.DELTA;
          case CK_GLUE -> ConnectorConfig.Kind.GLUE;
          case CK_UNITY -> ConnectorConfig.Kind.UNITY;
          default -> throw new IllegalArgumentException("unsupported kind: " + c.getKind());
        };

    var auth =
        new ConnectorConfig.Auth(
            c.getAuth().getScheme(),
            c.getAuth().getPropsMap(),
            c.getAuth().getHeaderHintsMap(),
            c.getAuth().getSecretRef());

    return new ConnectorConfig(
        kind,
        c.getDisplayName(),
        c.getDestinationTenantId(),
        c.getDestinationCatalogDisplayName(),
        toPaths(c.getDestinationNamespacePathsList()),
        c.getDestinationTableDisplayName().isBlank() ? null : c.getDestinationTableDisplayName(),
        c.getDestinationTableColumnsList(),
        c.getUri(),
        c.getOptionsMap(),
        auth);
  }

  public static List<List<String>> toPaths(List<NamespacePath> in) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }

    return in.stream()
        .map(
            np -> {
              var segs = np.getSegmentsList();
              var cleaned =
                  segs.stream()
                      .map(s -> s == null ? "" : s.trim())
                      .filter(s -> !s.isEmpty())
                      .toList();
              return List.copyOf(cleaned);
            })
        .toList();
  }
}
