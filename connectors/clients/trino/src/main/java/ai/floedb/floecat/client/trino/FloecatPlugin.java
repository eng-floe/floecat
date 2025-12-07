package ai.floedb.floecat.client.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

public class FloecatPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return List.of(new FloecatConnectorFactory());
  }
}
