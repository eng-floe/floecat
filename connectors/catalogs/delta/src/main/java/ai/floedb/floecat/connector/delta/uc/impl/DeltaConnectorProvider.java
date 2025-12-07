package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.HashMap;
import java.util.Map;

public final class DeltaConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "delta";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    Map<String, String> options = new HashMap<>(cfg.options());
    AuthProvider authProvider = DatabricksAuthFactory.from(cfg.auth());
    return UnityDeltaConnector.create(cfg.uri(), options, authProvider);
  }
}
