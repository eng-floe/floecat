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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

class UnityConnectorProviderTest {

  @Test
  void connectorFactoryCreatesUnityConnectors() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.UNITY,
            "unity",
            "https://workspace.example.com",
            Map.of("databricks.sql.warehouse_id", "warehouse-1"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    try (var connector = ConnectorFactory.create(config)) {
      assertThat(connector).isInstanceOf(UnityDeltaConnector.class);
      assertThat(connector.id()).isEqualTo("delta-unity");
    }
  }
}
