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

  @Override
  public String kind() {
    return "unity";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    LAST_CONFIG.set(cfg);
    return DummyConnector.create();
  }

  public static ConnectorConfig lastConfig() {
    return LAST_CONFIG.get();
  }

  public static void reset() {
    LAST_CONFIG.set(null);
  }
}
