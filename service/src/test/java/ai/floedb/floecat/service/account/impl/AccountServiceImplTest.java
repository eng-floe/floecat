/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.account.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.integration.CatalogIntegrationCredentialCleanup;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountServiceImplTest {
  private AccountServiceImpl service;
  private ResourceId accountId;
  private InMemoryPointerStore pointers;

  @BeforeEach
  void setUp() {
    service = new AccountServiceImpl();
    service.accountRepo = mock(AccountRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
    service.catalogIntegrationRepo = mock(CatalogIntegrationRepository.class);
    service.namespaceRepo = mock(NamespaceRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.tableRootRepo = mock(TableRootRepository.class);
    service.connectorRepo = mock(ConnectorRepository.class);
    service.viewRepo = mock(ViewRepository.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.metadataGraph = mock(UserGraph.class);
    service.markerStore = mock(MarkerStore.class);
    pointers = new InMemoryPointerStore();
    service.pointerStore = pointers;
    service.credentialResolver = mock(DefaultCredentialResolver.class);
    service.catalogIntegrationCredentialCleanup = mock(CatalogIntegrationCredentialCleanup.class);
    installBasePrincipal(service, service.principal);
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("account.delete")
                .build());
    accountId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("acct")
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
  }

  @Test
  void descendantCleanupFailureRetriesBehindDurableDeletionFence() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Connector connector = Connector.newBuilder().setResourceId(connectorId).build();
    when(service.accountRepo.metaFor(accountId))
        .thenReturn(meta)
        .thenThrow(new BaseResourceRepository.NotFoundException("deleted"));
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    when(service.connectorRepo.listConsistent(eq("acct"), eq(200), anyString(), any()))
        .thenReturn(List.of(connector), List.of(connector));
    when(service.connectorRepo.delete(connectorId)).thenReturn(false, true);
    when(service.connectorRepo.getById(connectorId)).thenReturn(Optional.of(connector));

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(meta, response.getMeta());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    verify(service.accountRepo).deleteWithPrecondition(accountId, 7L);
    verify(service.connectorRepo, org.mockito.Mockito.times(2)).delete(connectorId);
  }

  private static void installBasePrincipal(
      AccountServiceImpl service, PrincipalProvider principalProvider) {
    try {
      Field field = BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, principalProvider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject BaseServiceImpl principal provider", e);
    }
  }
}
