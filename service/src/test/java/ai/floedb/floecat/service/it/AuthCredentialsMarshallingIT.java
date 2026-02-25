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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.connector.dummy.DummyConnectorProvider;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class AuthCredentialsMarshallingIT {
  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @BeforeEach
  void resetCapture() {
    DummyConnectorProvider.reset();
  }

  @Test
  void passesInlineCredentialsToConnector() {
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("dummy")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("https://example.invalid")
            .setAuth(
                AuthConfig.newBuilder()
                    .setScheme("oauth2")
                    .setCredentials(
                        ai.floedb.floecat.connector.rpc.AuthCredentials.newBuilder()
                            .setBearer(
                                ai.floedb.floecat.connector.rpc.AuthCredentials.BearerToken
                                    .newBuilder()
                                    .setToken("inline-token"))
                            .build())
                    .build())
            .build();

    var resp =
        connectors.validateConnector(ValidateConnectorRequest.newBuilder().setSpec(spec).build());

    assertEquals(true, resp.getOk());

    var cfg = DummyConnectorProvider.lastConfig();
    assertNotNull(cfg);
    assertEquals("oauth2", cfg.auth().scheme());
    assertEquals("inline-token", cfg.auth().props().get("token"));
  }
}
