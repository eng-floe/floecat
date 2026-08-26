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

package ai.floedb.floecat.connector.dummy;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.concurrent.atomic.AtomicReference;

public final class DummyConnectorProvider implements ConnectorProvider {
  private static final AtomicReference<ConnectorConfig> LAST_CONFIG = new AtomicReference<>();

  /**
   * {@code glue} is the one {@link ConnectorConfig.Kind} with no production provider: a real AWS
   * Glue catalog is reached as {@code delta} with {@code delta.source=glue}. Every other kind now
   * ships one, and {@code ConnectorFactory} builds its provider map with a duplicate-rejecting
   * collector in a class initializer -- so squatting on a kind that has a real provider poisons the
   * class on first use and fails every later connector call with {@code NoClassDefFoundError}.
   *
   * <p>Squatting also silently inherits whatever behaviour the service gives that kind. This dummy
   * sat on {@code unity} until the Unity Catalog connector landed, at which point the storage
   * authority and credential-vending paths started treating dummy connectors as Delta-family.
   */
  @Override
  public String kind() {
    return "glue";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    LAST_CONFIG.set(cfg);
    return DummyConnector.create(cfg);
  }

  public static ConnectorConfig lastConfig() {
    return LAST_CONFIG.get();
  }

  public static void reset() {
    LAST_CONFIG.set(null);
  }
}
