package ai.floedb.metacat.client.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

public class MetacatPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return List.of(new MetacatConnectorFactory());
  }
}
