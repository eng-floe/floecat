package ai.floedb.metacat.connector.iceberg.impl;

import java.util.HashMap;
import java.util.Map;

import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorProvider;
import ai.floedb.metacat.connector.spi.MetacatConnector;

public final class IcebergConnectorProvider implements ConnectorProvider {
  @Override public String kind() { return "iceberg-rest"; }

  @Override
  public MetacatConnector create(ConnectorConfig cfg) {
    Map<String,String> options = new HashMap<>(cfg.options());
    var auth = cfg.auth();
    return IcebergConnector.create(
        cfg.uri(),
        options,
        auth.scheme(),
        new HashMap<>(auth.props()),
        new HashMap<>(auth.headerHints())
    );
  }
}
