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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConnectorsImplUpdateConnectorTest {
  @Test
  void uriUpdateDoesNotResolveExistingDestinationReferences() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    var connectorId = resourceId("connector-1", ResourceKind.RK_CONNECTOR);
    var destination =
        DestinationTarget.newBuilder()
            .setCatalogId(resourceId("catalog-1", ResourceKind.RK_CATALOG))
            .setNamespaceId(resourceId("namespace-1", ResourceKind.RK_NAMESPACE))
            .setTableDisplayName("orders")
            .build();
    var current =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("connector")
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("old-uri")
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("sales")))
            .setDestination(destination)
            .build();
    stubSuccessfulUpdate(service, connectorId, current);

    service
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(ConnectorSpec.newBuilder().setUri("new-uri"))
                .setUpdateMask(FieldMask.newBuilder().addPaths("uri"))
                .build())
        .await()
        .indefinitely();

    var updated = captureUpdatedConnector(service);
    assertEquals(destination, updated.getDestination());
    verifyNoInteractions(service.tableRepo);
  }

  @Test
  void destinationUpdateDoesNotPinNamespaceDiscoveryConnectorToExistingTable() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    var connectorId = resourceId("connector-1", ResourceKind.RK_CONNECTOR);
    var current =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("connector")
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("uri")
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("sales")))
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(resourceId("catalog-1", ResourceKind.RK_CATALOG))
                    .setNamespaceId(resourceId("namespace-1", ResourceKind.RK_NAMESPACE))
                    .setTableDisplayName("old-name"))
            .build();
    var replacement =
        DestinationTarget.newBuilder()
            .setCatalogId(resourceId("catalog-1", ResourceKind.RK_CATALOG))
            .setNamespaceId(resourceId("namespace-1", ResourceKind.RK_NAMESPACE))
            .setTableDisplayName("orders")
            .build();
    stubSuccessfulUpdate(service, connectorId, current);

    service
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(ConnectorSpec.newBuilder().setDestination(replacement))
                .setUpdateMask(FieldMask.newBuilder().addPaths("destination"))
                .build())
        .await()
        .indefinitely();

    var updated = captureUpdatedConnector(service);
    assertEquals(replacement, updated.getDestination());
    verifyNoInteractions(service.tableRepo);
  }

  @Test
  void destinationUpdateStillResolvesTableForSingleTableConnector() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    var connectorId = resourceId("connector-1", ResourceKind.RK_CONNECTOR);
    var catalogId = resourceId("catalog-1", ResourceKind.RK_CATALOG);
    var namespaceId = resourceId("namespace-1", ResourceKind.RK_NAMESPACE);
    var tableId = resourceId("table-1", ResourceKind.RK_TABLE);
    var current =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("connector")
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("uri")
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("sales"))
                    .setTable("orders"))
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(catalogId)
                    .setNamespaceId(namespaceId)
                    .setTableDisplayName("old-name"))
            .build();
    var replacement =
        DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableDisplayName("orders")
            .build();
    stubSuccessfulUpdate(service, connectorId, current);
    when(service.tableRepo.getByName("acct", "catalog-1", "namespace-1", "orders"))
        .thenReturn(Optional.of(Table.newBuilder().setResourceId(tableId).build()));

    service
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(ConnectorSpec.newBuilder().setDestination(replacement))
                .setUpdateMask(FieldMask.newBuilder().addPaths("destination"))
                .build())
        .await()
        .indefinitely();

    var updated = captureUpdatedConnector(service);
    assertEquals(tableId, updated.getDestination().getTableId());
  }

  @Test
  void restoresRemovedCredentialsWhenRepositoryUpdateFails() throws Exception {
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
    var current =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("connector")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("old-uri")
            .setAuth(AuthConfig.newBuilder().setScheme("bearer"))
            .build();
    var previous =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("previous"))
            .build();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.manage")
            .build();

    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.metaFor(connectorId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(current));
    when(service.credentialResolver.resolve("acct", "connector-1"))
        .thenReturn(Optional.of(previous));
    when(service.connectorRepo.update(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(7L)))
        .thenThrow(new IllegalStateException("repository unavailable"));

    var request =
        UpdateConnectorRequest.newBuilder()
            .setConnectorId(connectorId)
            .setSpec(
                ConnectorSpec.newBuilder()
                    .setUri("new-uri")
                    .setAuth(AuthConfig.newBuilder().setScheme("none")))
            .setUpdateMask(FieldMask.newBuilder().addPaths("uri").addPaths("auth"))
            .build();

    assertThrows(
        RuntimeException.class, () -> service.updateConnector(request).await().indefinitely());

    var ordered = inOrder(service.credentialResolver);
    ordered.verify(service.credentialResolver).delete("acct", "connector-1");
    ordered.verify(service.credentialResolver).store("acct", "connector-1", previous);
  }

  @Test
  void rejectsUnresolvedReplacementCatalogBeforeUpdatingConnector() throws Exception {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
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
    var current = Connector.newBuilder().setResourceId(connectorId).build();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.manage")
            .build();

    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.metaFor(connectorId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(current));
    when(service.catalogRepo.getByName("acct", "missing-catalog")).thenReturn(Optional.empty());

    var request =
        UpdateConnectorRequest.newBuilder()
            .setConnectorId(connectorId)
            .setSpec(
                ConnectorSpec.newBuilder()
                    .setDestination(
                        DestinationTarget.newBuilder()
                            .setCatalogDisplayName("missing-catalog")
                            .setNamespace(NamespacePath.newBuilder().addSegments("namespace"))))
            .setUpdateMask(FieldMask.newBuilder().addPaths("destination"))
            .build();

    assertThrows(
        RuntimeException.class, () -> service.updateConnector(request).await().indefinitely());

    verify(service.connectorRepo, never())
        .update(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong());
  }

  @Test
  void restoresPriorCredentialsWhenRepositoryUpdateFailsUnexpectedly() throws Exception {
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
    var current =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("connector")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("old-uri")
            .setAuth(AuthConfig.newBuilder().setScheme("bearer"))
            .build();
    var previous =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("previous"))
            .build();
    var replacement =
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("replacement"))
            .build();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.manage")
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7L).build();

    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.metaFor(connectorId)).thenReturn(meta);
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(current));
    when(service.credentialResolver.resolve("acct", "connector-1"))
        .thenReturn(Optional.of(previous));
    when(service.connectorRepo.update(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(7L)))
        .thenThrow(new IllegalStateException("repository unavailable"));

    var request =
        UpdateConnectorRequest.newBuilder()
            .setConnectorId(connectorId)
            .setSpec(
                ConnectorSpec.newBuilder()
                    .setUri("new-uri")
                    .setAuth(
                        AuthConfig.newBuilder().setScheme("bearer").setCredentials(replacement)))
            .setUpdateMask(FieldMask.newBuilder().addPaths("uri").addPaths("auth"))
            .build();

    assertThrows(
        RuntimeException.class, () -> service.updateConnector(request).await().indefinitely());

    var ordered = inOrder(service.credentialResolver);
    ordered.verify(service.credentialResolver).store("acct", "connector-1", replacement);
    ordered.verify(service.credentialResolver).store("acct", "connector-1", previous);
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

  private static ResourceId resourceId(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(id).setKind(kind).build();
  }

  private static void stubSuccessfulUpdate(
      ConnectorsImpl service, ResourceId connectorId, Connector current) {
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr-1")
            .addPermissions("connector.manage")
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    when(service.principalProvider.get()).thenReturn(principal);
    when(service.connectorRepo.metaFor(connectorId)).thenReturn(meta);
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(current));
    when(service.connectorRepo.update(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(7L)))
        .thenReturn(true);
    when(service.connectorRepo.metaForSafe(connectorId)).thenReturn(meta);
  }

  private static Connector captureUpdatedConnector(ConnectorsImpl service) {
    var captor = ArgumentCaptor.forClass(Connector.class);
    verify(service.connectorRepo).update(captor.capture(), org.mockito.ArgumentMatchers.eq(7L));
    return captor.getValue();
  }
}
