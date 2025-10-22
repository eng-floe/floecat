package ai.floedb.metacat.connector.spi;

import java.util.ServiceLoader;

public final class ConnectorFactory {
  private static final ServiceLoader<ConnectorProvider> LOADER =
      ServiceLoader.load(ConnectorProvider.class);

  private ConnectorFactory() {}

  public static MetacatConnector create(ConnectorConfig config) {
    String kindId =
        switch (config.kind()) {
          case ICEBERG_REST -> "iceberg-rest";
          case DELTA -> "delta";
          case GLUE -> "glue";
          case UNITY -> "unity";
        };
    ConnectorProvider p =
        LOADER.stream()
            .map(ServiceLoader.Provider::get)
            .filter(cp -> cp.kind().equalsIgnoreCase(kindId))
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException("No ConnectorProvider for kind=" + kindId));
    return p.create(config);
  }
}
