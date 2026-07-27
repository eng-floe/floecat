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

package ai.floedb.floecat.service.connector.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ExportConnectorRequest;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConnectorsImplExportConnectorTest {
  @Test
  void exportsUnmaskedConnectorAndSeparateCredentials() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("upstream")
            .setAuth(AuthConfig.newBuilder().putProperties("client_id", "not-masked"))
            .build();
    var credentials =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("secret-token"))
            .build();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.export")
            .build();

    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(connector));
    when(service.credentialResolver.resolve("acct", "connector-1"))
        .thenReturn(Optional.of(credentials));

    var response =
        service
            .exportConnector(
                ExportConnectorRequest.newBuilder()
                    .setConnectorId(connectorId)
                    .setIncludeCredentials(true)
                    .build())
            .await()
            .indefinitely();

    assertThat(response.getConnector()).isEqualTo(connector);
    assertThat(response.getCredentials()).isEqualTo(credentials);
    verify(service.authz).require(principal, "connector.export");
  }

  private static void installBasePrincipal(
      ConnectorsImpl service, PrincipalProvider principalProvider) {
    try {
      Field field =
          ai.floedb.floecat.service.common.BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, principalProvider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject BaseServiceImpl principal provider", e);
    }
  }
}
