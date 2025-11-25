package ai.floedb.metacat.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/** The main entry point for Trino to load the Metacat connector. */
public class MetacatPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new MetacatConnectorFactory());
  }
}
