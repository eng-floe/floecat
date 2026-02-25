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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DatabricksAuthFactoryTest {

  @Test
  void oauth2StaticTokenProducesBearerHeaders() {
    ConnectorConfig.Auth auth =
        new ConnectorConfig.Auth("oauth2", Map.of("token", "tok-123"), Map.of());

    AuthProvider provider = DatabricksAuthFactory.from(auth, "https://dbc.example.com");

    assertTrue(provider instanceof OAuth2BearerAuthProvider);
    Map<String, String> headers = provider.applyHeaders(Map.of());
    assertEquals("Bearer tok-123", headers.get("Authorization"));
  }

  @Test
  void cliModeRequiresHost() {
    ConnectorConfig.Auth auth =
        new ConnectorConfig.Auth("oauth2", Map.of("cli.provider", "databricks"), Map.of());

    assertThrows(IllegalArgumentException.class, () -> DatabricksAuthFactory.from(auth, ""));
  }
}
