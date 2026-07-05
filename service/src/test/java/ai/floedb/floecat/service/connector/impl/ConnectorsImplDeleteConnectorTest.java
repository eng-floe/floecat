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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

class ConnectorsImplDeleteConnectorTest {
  @Test
  void deleteConnectorDeletesCanonicalConnectorAndCredentialsOnly() throws Exception {
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
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.manage")
            .build();
    var meta =
        MutationMeta.newBuilder()
            .setPointerKey("/accounts/acct/connectors/by-id/connector-1")
            .setBlobUri("blob://connector-1")
            .setPointerVersion(7L)
            .build();

    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.metaFor(connectorId)).thenReturn(meta);
    when(service.connectorRepo.deleteWithPrecondition(connectorId, 7L)).thenReturn(true);
    when(service.connectorRepo.metaForSafe(connectorId)).thenReturn(meta);

    var response =
        service
            .deleteConnector(
                DeleteConnectorRequest.newBuilder().setConnectorId(connectorId).build())
            .await()
            .indefinitely();

    assertEquals(7L, response.getMeta().getPointerVersion());
    verify(service.connectorRepo).deleteWithPrecondition(connectorId, 7L);
    verify(service.credentialResolver).delete("acct", "connector-1");
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
