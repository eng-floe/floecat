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

package ai.floedb.floecat.connector.spi;

import java.util.ServiceLoader;

public final class ConnectorFactory {
  private static final ServiceLoader<ConnectorProvider> LOADER =
      ServiceLoader.load(ConnectorProvider.class);

  private ConnectorFactory() {}

  public static FloecatConnector create(ConnectorConfig config) {
    String kindId =
        switch (config.kind()) {
          case ICEBERG -> "iceberg";
          case DELTA -> "delta";
          case GLUE -> "glue";
          case UNITY -> "unity";
        };
    ConnectorProvider p =
        LOADER.stream()
            .map(ServiceLoader.Provider::get)
            .filter(cp -> cp.kind().equalsIgnoreCase(kindId))
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException("No ConnectorProvider for kind=" + kindId));
    return p.create(config);
  }
}
