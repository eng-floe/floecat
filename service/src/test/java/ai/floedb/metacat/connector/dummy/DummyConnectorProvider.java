package ai.floedb.metacat.connector.dummy;

import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorProvider;
import ai.floedb.metacat.connector.spi.MetacatConnector;

public final class DummyConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "unity";
  }

  @Override
  public MetacatConnector create(ConnectorConfig cfg) {
    return DummyConnector.create();
  }
}
