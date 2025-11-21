package ai.floedb.metacat.connector.delta.uc.impl;

import ai.floedb.metacat.connector.spi.AuthProvider;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorProvider;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.HashMap;
import java.util.Map;

public final class DeltaConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "delta";
  }

  @Override
  public MetacatConnector create(ConnectorConfig cfg) {
    Map<String, String> options = new HashMap<>(cfg.options());
    AuthProvider authProvider = DatabricksAuthFactory.from(cfg.auth());
    return UnityDeltaConnector.create(cfg.uri(), options, authProvider);
  }
}
