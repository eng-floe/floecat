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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorsImplTest {
  private static final ResourceId CONNECTOR_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CONNECTOR)
          .setId("conn-1")
          .build();
  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CATALOG)
          .setId("cat-1")
          .build();

  private ConnectorsImpl service;
  private ConnectorRepository connectorRepo;
  private ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  private ai.floedb.floecat.connector.spi.CredentialResolver credentialResolver;
  private AtomicReference<Connector> persistedConnector;

  @BeforeEach
  void setUp() {
    service = new ConnectorsImpl();
    connectorRepo = mock(ConnectorRepository.class);
    idempotencyStore = mock(ai.floedb.floecat.service.repo.IdempotencyRepository.class);
    credentialResolver = mock(ai.floedb.floecat.connector.spi.CredentialResolver.class);
    persistedConnector = new AtomicReference<>();

    service.connectorRepo = connectorRepo;
    service.catalogRepo = mock(CatalogRepository.class);
    service.namespaceRepo = mock(NamespaceRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = idempotencyStore;
    service.credentialResolver = credentialResolver;
    installBasePrincipal(service, service.principalProvider);

    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr")
            .addPermissions("connector.manage")
            .addPermissions("connector.create")
            .build();
    when(service.principalProvider.get()).thenReturn(principal);
    when(connectorRepo.metaFor(any(ResourceId.class)))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1).build());
    when(connectorRepo.metaForSafe(any(ResourceId.class)))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1).build());
    when(connectorRepo.getByName(eq("acct"), any())).thenReturn(Optional.empty());
    doAnswer(
            invocation -> {
              persistedConnector.set(invocation.getArgument(0, Connector.class));
              return null;
            })
        .when(connectorRepo)
        .create(any(Connector.class));
    when(connectorRepo.update(any(Connector.class), anyLong()))
        .thenAnswer(
            invocation -> {
              persistedConnector.set(invocation.getArgument(0, Connector.class));
              return true;
            });
  }

  @Test
  void createConnectorPersistsRecordWithoutInlineCredentials() {
    ConnectorSpec spec =
        baseSpec().toBuilder()
            .setAuth(
                AuthConfig.newBuilder()
                    .setScheme("oauth2")
                    .setCredentials(
                        AuthCredentials.newBuilder()
                            .setClient(
                                AuthCredentials.ClientCredentials.newBuilder()
                                    .setEndpoint("https://auth.example.test/token")
                                    .setClientId("client-id")
                                    .setClientSecret("client-secret"))))
            .build();

    service
        .createConnector(CreateConnectorRequest.newBuilder().setSpec(spec).build())
        .await()
        .indefinitely();

    Connector persisted = persistedConnector.get();
    assertTrue(persisted.hasAuth());
    assertFalse(persisted.getAuth().hasCredentials());
  }

  @Test
  void updateConnectorPersistsRecordWithoutInlineCredentials() {
    Connector current =
        Connector.newBuilder()
            .setResourceId(CONNECTOR_ID)
            .setDisplayName("orders")
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("http://warehouse.example.test")
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("db")))
            .setDestination(DestinationTarget.newBuilder().setCatalogId(CATALOG_ID))
            .setAuth(AuthConfig.newBuilder().setScheme("oauth2"))
            .build();
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.of(current));
    when(credentialResolver.resolve("acct", "conn-1")).thenReturn(Optional.empty());

    ConnectorSpec patch =
        ConnectorSpec.newBuilder()
            .setAuth(
                AuthConfig.newBuilder()
                    .setCredentials(
                        AuthCredentials.newBuilder()
                            .setClient(
                                AuthCredentials.ClientCredentials.newBuilder()
                                    .setEndpoint("https://auth.example.test/token")
                                    .setClientId("client-id")
                                    .setClientSecret("client-secret"))))
            .build();

    service
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(CONNECTOR_ID)
                .setSpec(patch)
                .setUpdateMask(FieldMask.newBuilder().addPaths("auth.credentials").build())
                .build())
        .await()
        .indefinitely();

    Connector persisted = persistedConnector.get();
    assertTrue(persisted.hasAuth());
    assertFalse(persisted.getAuth().hasCredentials());
  }

  private static ConnectorSpec baseSpec() {
    return ConnectorSpec.newBuilder()
        .setDisplayName("orders")
        .setKind(ConnectorKind.CK_ICEBERG)
        .setUri("http://warehouse.example.test")
        .setSource(
            SourceSelector.newBuilder().setNamespace(NamespacePath.newBuilder().addSegments("db")))
        .setDestination(DestinationTarget.newBuilder().setCatalogId(CATALOG_ID))
        .build();
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
