package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorProvider;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.HashMap;
import java.util.Map;

public final class IcebergConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "iceberg";
  }

  @Override
  public MetacatConnector create(ConnectorConfig cfg) {
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
