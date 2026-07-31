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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConnectorsImplCreateConnectorTest {
  @Test
  void createsPausedConnectorAtomically() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
    service.namespaceRepo = mock(NamespaceRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr")
            .addPermissions("connector.manage")
            .addPermissions("connector.create")
            .build();
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("catalog-id")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    when(service.principalProvider.get()).thenReturn(principal);
    when(service.catalogRepo.getByName("acct", "catalog"))
        .thenReturn(Optional.of(Catalog.newBuilder().setResourceId(catalogId).build()));
    when(service.namespaceRepo.getByPath(any(), any(), any())).thenReturn(Optional.empty());
    when(service.connectorRepo.getByName("acct", "connector")).thenReturn(Optional.empty());
    when(service.connectorRepo.metaFor(any())).thenReturn(MutationMeta.getDefaultInstance());

    var response =
        service
            .createConnector(
                CreateConnectorRequest.newBuilder()
                    .setSpec(
                        ConnectorSpec.newBuilder()
                            .setDisplayName("connector")
                            .setKind(ConnectorKind.CK_ICEBERG)
                            .setUri("https://source.example")
                            .setSource(
                                SourceSelector.newBuilder()
                                    .setNamespace(
                                        NamespacePath.newBuilder().addSegments("source")))
                            .setDestination(
                                DestinationTarget.newBuilder()
                                    .setCatalogDisplayName("catalog"))
                            .setState(ConnectorState.CS_PAUSED))
                    .build())
            .await()
            .indefinitely();

    var created = ArgumentCaptor.forClass(Connector.class);
    org.mockito.Mockito.verify(service.connectorRepo).create(created.capture());
    assertThat(created.getValue().getState()).isEqualTo(ConnectorState.CS_PAUSED);
    assertThat(response.getConnector().getState()).isEqualTo(ConnectorState.CS_PAUSED);
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
