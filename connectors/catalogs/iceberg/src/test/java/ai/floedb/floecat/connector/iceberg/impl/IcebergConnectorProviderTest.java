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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergConnectorProviderTest {

  @Test
  void applies_auth_credentials_for_oauth2() {
    ConnectorConfig.Auth auth =
        new ConnectorConfig.Auth(
            "oauth2",
            Map.of(),
            Map.of(),
            AuthCredentials.newBuilder()
                .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("token"))
                .build());
    ConnectorConfig cfg =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "iceberg-test",
            "http://localhost:8181",
            Map.of("iceberg.source", "rest"),
            auth);

    ConnectorConfig resolved = IcebergConnectorProvider.resolveCredentials(cfg);
    assertEquals("token", resolved.auth().props().get("token"));
  }

  @Test
  void applies_auth_credentials_for_oauth_client() {
    ConnectorConfig.Auth auth =
        new ConnectorConfig.Auth(
            "oauth2",
            Map.of(),
            Map.of(),
            AuthCredentials.newBuilder()
                .setClient(
                    AuthCredentials.ClientCredentials.newBuilder()
                        .setClientId("client-id")
                        .setClientSecret("client-secret"))
                .build());
    ConnectorConfig cfg =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "iceberg-test",
            "http://localhost:8181",
            Map.of("iceberg.source", "rest"),
            auth);

    ConnectorConfig resolved = IcebergConnectorProvider.resolveCredentials(cfg);
    assertEquals("client-id", resolved.auth().props().get("oauth.client_id"));
    assertEquals("client-secret", resolved.auth().props().get("oauth.client_secret"));
  }
}
