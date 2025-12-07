package ai.floedb.floecat.connector.spi;

import java.util.ServiceLoader;

public final class ConnectorFactory {
  private static final ServiceLoader<ConnectorProvider> LOADER =
      ServiceLoader.load(ConnectorProvider.class);

  private ConnectorFactory() {}

  public static FloecatConnector create(ConnectorConfig config) {
    String kindId =
        switch (config.kind()) {
          case ICEBERG -> "iceberg";
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
