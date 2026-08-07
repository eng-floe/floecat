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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
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
    when(service.connectorRepo.credentialCleanupReadyGuard(connectorId))
        .thenReturn(BatchGuard.NONE);
    when(service.connectorRepo.deleteWithPrecondition(connectorId, 7L, BatchGuard.NONE))
        .thenReturn(true);
    when(service.connectorRepo.metaForSafe(connectorId)).thenReturn(meta);
    var cleanup =
        new ConnectorRepository.CredentialCleanup(
            connectorId, "connector-1", "/credential-cleanup/connector-1", 1L);
    var claimed =
        new ConnectorRepository.CredentialCleanup(
            connectorId, "connector-1", "/credential-cleanup/connector-1", 2L);
    when(service.connectorRepo.prepareCredentialCleanup(connectorId)).thenReturn(List.of(cleanup));
    when(service.connectorRepo.claimCredentialCleanup(cleanup, BatchGuard.NONE))
        .thenReturn(Optional.of(claimed));

    var response =
        service
            .deleteConnector(
                DeleteConnectorRequest.newBuilder().setConnectorId(connectorId).build())
            .await()
            .indefinitely();

    assertEquals(7L, response.getMeta().getPointerVersion());
    verify(service.connectorRepo).deleteWithPrecondition(connectorId, 7L, BatchGuard.NONE);
    verify(service.connectorRepo).prepareCredentialCleanup(connectorId);
    verify(service.connectorRepo).claimCredentialCleanup(cleanup, BatchGuard.NONE);
    verify(service.credentialResolver).delete("acct", "connector-1");
    verify(service.connectorRepo).completeCredentialCleanup(claimed);
  }

  @Test
  void absentConnectorDrainsPendingCredentialsBeforeReturningNotFound() throws Exception {
    var fixture = fixture();
    var cleanup = cleanup(fixture.connectorId(), 1L);
    var claimed = cleanup(fixture.connectorId(), 2L);
    when(fixture.service().connectorRepo.metaFor(fixture.connectorId()))
        .thenThrow(
            new ai.floedb.floecat.service.repo.util.BaseResourceRepository.NotFoundException(
                "missing"));
    when(fixture.service().connectorRepo.metaForSafe(fixture.connectorId()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(fixture.service().connectorRepo.pendingCredentialCleanups(fixture.connectorId()))
        .thenReturn(List.of(cleanup));
    when(fixture.service().connectorRepo.claimCredentialCleanup(cleanup, BatchGuard.NONE))
        .thenReturn(Optional.of(claimed));

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                fixture
                    .service()
                    .deleteConnector(
                        DeleteConnectorRequest.newBuilder()
                            .setConnectorId(fixture.connectorId())
                            .setPrecondition(Precondition.newBuilder().setExpectedVersion(7L))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, failure.getStatus().getCode());
    verify(fixture.service().credentialResolver).delete("acct", "connector-1");
    verify(fixture.service().connectorRepo).completeCredentialCleanup(claimed);
  }

  @Test
  void lostDeleteCasDrainsPendingCredentialsWhenConnectorIsNowAbsent() throws Exception {
    var fixture = fixture();
    var live =
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setPointerKey("/accounts/acct/connectors/by-id/connector-1")
            .build();
    var cleanup = cleanup(fixture.connectorId(), 1L);
    var claimed = cleanup(fixture.connectorId(), 2L);
    when(fixture.service().connectorRepo.metaFor(fixture.connectorId())).thenReturn(live);
    when(fixture.service().connectorRepo.prepareCredentialCleanup(fixture.connectorId()))
        .thenReturn(List.of(cleanup));
    when(fixture
            .service()
            .connectorRepo
            .deleteWithPrecondition(fixture.connectorId(), 7L, BatchGuard.NONE))
        .thenReturn(false);
    when(fixture.service().connectorRepo.metaForSafe(fixture.connectorId()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(fixture.service().connectorRepo.claimCredentialCleanup(cleanup, BatchGuard.NONE))
        .thenReturn(Optional.of(claimed));

    var response =
        fixture
            .service()
            .deleteConnector(
                DeleteConnectorRequest.newBuilder().setConnectorId(fixture.connectorId()).build())
            .await()
            .indefinitely();

    assertEquals(0L, response.getMeta().getPointerVersion());
    verify(fixture.service().credentialResolver).delete("acct", "connector-1");
    verify(fixture.service().connectorRepo).completeCredentialCleanup(claimed);
  }

  private static Fixture fixture() throws Exception {
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
    when(service.principalProvider.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr-1")
                .addPermissions("connector.manage")
                .build());
    when(service.connectorRepo.credentialCleanupReadyGuard(connectorId))
        .thenReturn(BatchGuard.NONE);
    return new Fixture(service, connectorId);
  }

  private static ConnectorRepository.CredentialCleanup cleanup(
      ResourceId connectorId, long version) {
    return new ConnectorRepository.CredentialCleanup(
        connectorId, "connector-1", "/credential-cleanup/connector-1", version);
  }

  private record Fixture(ConnectorsImpl service, ResourceId connectorId) {}

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
