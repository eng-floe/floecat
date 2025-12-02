package ai.floedb.metacat.client.trino;

import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class MetacatConnectorFactory implements ConnectorFactory {

  @Override
  public String getName() {
    return "metacat";
  }

  @Override
  public Connector create(
      String catalogName, Map<String, String> config, ConnectorContext context) {
    requireNonNull(catalogName, "catalogName is null");
    requireNonNull(config, "config is null");

    boolean coordinatorFileCaching =
        Boolean.parseBoolean(config.getOrDefault("metacat.coordinator-file-caching", "false"));

    Bootstrap app =
        new Bootstrap(
            new MetacatModule(
                catalogName,
                context.getCatalogHandle(),
                context.getTypeManager(),
                context.getNodeManager(),
                context.getOpenTelemetry(),
                context.getTracer(),
                coordinatorFileCaching));

    Injector injector =
        app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();
    MetacatConnector connector = injector.getInstance(MetacatConnector.class);

    return connector;
  }
}
