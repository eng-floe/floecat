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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.HashMap;
import java.util.Map;

public final class DeltaConnectorProvider implements ConnectorProvider {
  @Override
  public String kind() {
    return "delta";
  }

  @Override
  public FloecatConnector create(ConnectorConfig cfg) {
    Map<String, String> options = new HashMap<>(cfg.options());
    AuthProvider authProvider = DatabricksAuthFactory.from(cfg.auth());
    return UnityDeltaConnector.create(cfg.uri(), options, authProvider);
  }
}
