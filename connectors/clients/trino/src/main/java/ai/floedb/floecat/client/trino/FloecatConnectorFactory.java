package ai.floedb.floecat.client.trino;

import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class FloecatConnectorFactory implements ConnectorFactory {

  @Override
  public String getName() {
    return "floecat";
  }

  @Override
  public Connector create(
      String catalogName, Map<String, String> config, ConnectorContext context) {
    requireNonNull(catalogName, "catalogName is null");
    requireNonNull(config, "config is null");

    boolean coordinatorFileCaching =
        Boolean.parseBoolean(config.getOrDefault("floecat.coordinator-file-caching", "false"));

    Bootstrap app =
        new Bootstrap(
            new FloecatModule(
                catalogName,
                context.getCatalogHandle(),
                context.getTypeManager(),
                context.getNodeManager(),
                context.getOpenTelemetry(),
                context.getTracer(),
                coordinatorFileCaching));

    Injector injector =
        app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();
    FloecatConnector connector = injector.getInstance(FloecatConnector.class);

    return connector;
  }
}
