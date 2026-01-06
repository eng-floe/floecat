/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
