package ai.floedb.floecat.connector.dummy;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;

public final class DummyConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "unity";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    return DummyConnector.create();
  }
}
