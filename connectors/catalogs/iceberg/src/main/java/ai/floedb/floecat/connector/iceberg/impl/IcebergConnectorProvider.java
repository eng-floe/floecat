package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.HashMap;
import java.util.Map;

public final class IcebergConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "iceberg";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    Map<String, String> options = new HashMap<>(cfg.options());
    var auth = cfg.auth();

    return IcebergConnector.create(
        cfg.uri(),
        options,
        auth.scheme(),
        new HashMap<>(auth.props()),
        new HashMap<>(auth.headerHints()));
  }
}
