/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.account.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountServiceImplTest {
  private AccountServiceImpl service;
  private ResourceId accountId;
  private TrackingPointerStore pointers;

  @BeforeEach
  void setUp() {
    service = new AccountServiceImpl();
    service.accountRepo = mock(AccountRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
    service.namespaceRepo = mock(NamespaceRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.tableRootRepo = mock(TableRootRepository.class);
    service.connectorRepo = mock(ConnectorRepository.class);
    service.storageAuthorityRepo = mock(StorageAuthorityRepository.class);
    service.viewRepo = mock(ViewRepository.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.metadataGraph = mock(UserGraph.class);
    service.markerStore = mock(MarkerStore.class);
    pointers = new TrackingPointerStore();
    service.pointerStore = pointers;
    service.credentialResolver = mock(DefaultCredentialResolver.class);
    service.secretsManager = mock(SecretsManager.class);
    installBasePrincipal(service, service.principal);
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("account.delete")
                .addPermissions("account.write")
                .build());
    accountId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("acct")
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
  }

  @Test
  void accountDeletionRemovesStorageAuthoritySecretBeforeRepositoryState() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId authorityId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("authority")
            .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
            .build();
    StorageAuthority authority = StorageAuthority.newBuilder().setResourceId(authorityId).build();
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    when(service.storageAuthorityRepo.listConsistent(eq("acct"), eq(200), anyString(), any()))
        .thenReturn(List.of(authority));

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    var ordered = inOrder(service.secretsManager, service.storageAuthorityRepo);
    ordered
        .verify(service.secretsManager)
        .delete(
            "acct", StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId.getId());
    ordered.verify(service.storageAuthorityRepo).deleteOrConfirmAbsent(authorityId);
  }

  @Test
  void accountDeletionPurgesSnapshotConstraintPointers() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId catalogId = resourceId("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = resourceId("namespace", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = resourceId("table", ResourceKind.RK_TABLE);
    Catalog catalog = Catalog.newBuilder().setResourceId(catalogId).build();
    Namespace namespace =
        Namespace.newBuilder().setResourceId(namespaceId).setCatalogId(catalogId).build();
    Table table = Table.newBuilder().setResourceId(tableId).setNamespaceId(namespaceId).build();
    String constraintKey = Keys.snapshotConstraintsPointer("acct", "table", 7L);
    pointers.compareAndSet(
        constraintKey, 0L, PointerReferences.opaqueMarkerPointer(constraintKey, "constraint", 1L));
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    when(service.catalogRepo.listConsistent(eq("acct"), eq(200), anyString(), any()))
        .thenReturn(List.of(catalog));
    when(service.namespaceRepo.listIdsConsistent("acct", "catalog"))
        .thenReturn(List.of(namespaceId));
    when(service.namespaceRepo.getByIdForMutation(namespaceId)).thenReturn(Optional.of(namespace));
    when(service.tableRepo.listConsistent(
            eq("acct"), eq("catalog"), eq("namespace"), eq(200), anyString(), any()))
        .thenReturn(List.of(table));

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    assertFalse(pointers.get(constraintKey).isPresent());
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
    doThrow(new BaseResourceRepository.AbortRetryableException("connector changed"))
        .doNothing()
        .when(service.connectorRepo)
        .deleteOrConfirmAbsent(connectorId);

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(meta, response.getMeta());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    verify(service.accountRepo).deleteWithPrecondition(accountId, 7L);
    verify(service.connectorRepo, org.mockito.Mockito.times(2)).deleteOrConfirmAbsent(connectorId);
  }

  @Test
  void deletingUnknownAccountDoesNotInstallFenceOrRunCleanup() {
    MutationMeta missing = MutationMeta.newBuilder().setPointerVersion(0L).build();
    when(service.accountRepo.metaFor(accountId))
        .thenThrow(new BaseResourceRepository.NotFoundException("missing"));
    when(service.accountRepo.metaForSafe(accountId)).thenReturn(missing);

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(missing, response.getMeta());
    assertFalse(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    verify(service.connectorRepo, never())
        .listConsistent(anyString(), anyInt(), anyString(), any());
  }

  @Test
  void transactionConflictKeepsFenceWhileSameAccountVersionCanStillCommit() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta, meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(false, true);
    when(service.accountRepo.metaForSafe(accountId)).thenReturn(meta);

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(meta, response.getMeta());
    assertEquals(0, pointers.compareAndDeleteCalls);
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
  }

  @Test
  void changedAccountVersionAdvancesFenceWithoutReopeningMutations() {
    MutationMeta version7 = MutationMeta.newBuilder().setPointerVersion(7L).build();
    MutationMeta version8 = MutationMeta.newBuilder().setPointerVersion(8L).build();
    when(service.accountRepo.metaFor(accountId)).thenReturn(version7, version8);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(false);
    when(service.accountRepo.deleteWithPrecondition(accountId, 8L)).thenReturn(true);
    when(service.accountRepo.metaForSafe(accountId)).thenReturn(version8);

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(version8, response.getMeta());
    assertEquals(0, pointers.compareAndDeleteCalls);
    assertEquals(2L, pointers.get(Keys.accountDeletionMarker("acct")).orElseThrow().getVersion());
  }

  @Test
  void changedAccountVersionReleasesStaleFenceWhenPreconditionCannotBeRetried() {
    MutationMeta version7 = MutationMeta.newBuilder().setPointerVersion(7L).build();
    MutationMeta version8 = MutationMeta.newBuilder().setPointerVersion(8L).build();
    when(service.accountRepo.metaFor(accountId)).thenReturn(version7);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(false);
    when(service.accountRepo.metaForSafe(accountId)).thenReturn(version8);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .deleteAccount(
                    DeleteAccountRequest.newBuilder()
                        .setAccountId(accountId)
                        .setPrecondition(Precondition.newBuilder().setExpectedVersion(7L))
                        .build())
                .await()
                .indefinitely());

    assertEquals(1, pointers.compareAndDeleteCalls);
    assertFalse(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
  }

  @Test
  void fenceBlockedCreateUsesStructuredFailedPrecondition() {
    when(service.accountRepo.getByName("alpha")).thenReturn(Optional.empty());
    doThrow(new BaseResourceRepository.AccountDeletionInProgressException("acct"))
        .when(service.accountRepo)
        .create(any(Account.class));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createAccount(
                        CreateAccountRequest.newBuilder()
                            .setAccountId(accountId)
                            .setSpec(AccountSpec.newBuilder().setDisplayName("alpha"))
                            .build())
                    .await()
                    .indefinitely());

    FloecatStatus decoded = FloecatStatus.fromThrowable(failure);
    assertEquals(Status.Code.FAILED_PRECONDITION, decoded.canonicalCode());
    assertEquals(ErrorCode.MC_PRECONDITION_FAILED, decoded.errorCode());
    assertEquals("account.deletion.in.progress", decoded.messageKey());
    assertEquals("acct", decoded.params().get("account_id"));
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

  private static ResourceId resourceId(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(id).setKind(kind).build();
  }

  private static final class TrackingPointerStore extends InMemoryPointerStore {
    private int compareAndDeleteCalls;

    @Override
    public synchronized boolean compareAndDelete(String key, long expectedVersion) {
      compareAndDeleteCalls++;
      return super.compareAndDelete(key, expectedVersion);
    }
  }
}
