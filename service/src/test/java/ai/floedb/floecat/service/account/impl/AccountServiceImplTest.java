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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.UpdateAccountRequest;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.integration.CatalogIntegrationCredentialCleanup;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountServiceImplTest {
  private AccountServiceImpl service;
  private ResourceId accountId;
  private TrackingPointerStore pointers;
  private InMemoryBlobStore blobs;

  @BeforeEach
  void setUp() {
    service = new AccountServiceImpl();
    service.accountRepo = mock(AccountRepository.class);
    service.catalogIntegrationRepo = mock(CatalogIntegrationRepository.class);
    service.tableRootRepo = mock(TableRootRepository.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.metadataGraph = mock(UserGraph.class);
    pointers = new TrackingPointerStore();
    service.pointerStore = pointers;
    blobs = new InMemoryBlobStore();
    service.blobStore = blobs;
    service.credentialResolver = mock(DefaultCredentialResolver.class);
    service.secretsManager = mock(SecretsManager.class);
    service.durableReconcileJobStore = mock(Instance.class);
    when(service.durableReconcileJobStore.isResolvable()).thenReturn(false);
    service.catalogIntegrationCredentialCleanup = mock(CatalogIntegrationCredentialCleanup.class);
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
    String authorityPointer = Keys.storageAuthorityPointerById("acct", authorityId.getId());
    putPointer(authorityPointer, "/accounts/acct/storage-authorities/authority/broken.pb");
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    doAnswer(
            ignored -> {
              assertTrue(pointers.get(authorityPointer).isPresent());
              return null;
            })
        .when(service.secretsManager)
        .delete(
            "acct", StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId.getId());

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    verify(service.secretsManager)
        .delete(
            "acct", StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId.getId());
    assertTrue(pointers.get(authorityPointer).isEmpty());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    assertTrue(
        Keys.accountDeletionFenceShards("acct").stream()
            .allMatch(key -> pointers.get(key).isPresent()));
  }

  @Test
  void accountDeletionPurgesSnapshotConstraintPointers() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId catalogId = resourceId("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = resourceId("namespace", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = resourceId("table", ResourceKind.RK_TABLE);
    putPointer(Keys.catalogPointerById("acct", "catalog"), "broken-catalog");
    putPointer(Keys.namespacePointerById("acct", "namespace"), "broken-namespace");
    putPointer(Keys.tablePointerById("acct", "table"), "broken-table");
    putPointer(Keys.catalogPointerByIdPrefix("acct"), "malformed-canonical-key");
    String constraintKey = Keys.snapshotConstraintsPointer("acct", "table", 7L);
    String catalogMarker = Keys.catalogChildrenMarker("acct", "catalog");
    String namespaceMarker = Keys.namespaceChildrenMarker("acct", "namespace");
    String tableBlobPrefix = Keys.tableBlobPrefix("acct", "table");
    String statsBlob = tableBlobPrefix + "target-stats/orphan.pb";
    String indexSidecar = tableBlobPrefix + "index-sidecars/orphan.parquet";
    String residualBlob = Keys.accountRootPrefix("acct") + "worker-output/orphan.pb";
    String otherAccountBlob =
        Keys.tableBlobPrefix("other-acct", "other-table") + "target-stats/live.pb";
    pointers.compareAndSet(
        constraintKey, 0L, PointerReferences.opaqueMarkerPointer(constraintKey, "constraint", 1L));
    pointers.compareAndSet(
        catalogMarker, 0L, PointerReferences.opaqueMarkerPointer(catalogMarker, "marker", 1L));
    pointers.compareAndSet(
        namespaceMarker, 0L, PointerReferences.opaqueMarkerPointer(namespaceMarker, "marker", 1L));
    blobs.put(statsBlob, new byte[] {1}, "application/x-protobuf");
    blobs.put(indexSidecar, new byte[] {2}, "application/octet-stream");
    blobs.put(residualBlob, new byte[] {3}, "application/x-protobuf");
    blobs.put(otherAccountBlob, new byte[] {4}, "application/x-protobuf");
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    doAnswer(
            ignored -> {
              assertEquals(1, pointers.countByPrefixConsistent(Keys.accountRootPrefix("acct")));
              return null;
            })
        .when(service.metadataGraph)
        .invalidate(any(ResourceId.class));

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    assertFalse(pointers.get(constraintKey).isPresent());
    assertFalse(pointers.get(catalogMarker).isPresent());
    assertFalse(pointers.get(namespaceMarker).isPresent());
    assertEquals(1, pointers.countByPrefixConsistent(Keys.accountRootPrefix("acct")));
    assertTrue(blobs.list(tableBlobPrefix, 100, "").keys().isEmpty());
    assertTrue(blobs.get(residualBlob) == null);
    assertTrue(blobs.get(otherAccountBlob) != null);
  }

  @Test
  void accountDeletionPurgesPreparedTransactionsAndIntents() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    String transactionPointer = Keys.transactionPointerById("acct", "tx-1");
    String intentPointer =
        Keys.transactionIntentPointerByTarget("acct", "/accounts/acct/tables/by-id/table-1");
    String transactionBlob = Keys.transactionBlobUri("acct", "tx-1", "transaction-sha");
    String intentBlob = Keys.transactionIntentBlobUri("acct", "tx-1", "intent-sha");
    pointers.compareAndSet(
        transactionPointer,
        0L,
        PointerReferences.blobPointer(transactionPointer, transactionBlob, 1L));
    pointers.compareAndSet(
        intentPointer, 0L, PointerReferences.blobPointer(intentPointer, intentBlob, 1L));
    blobs.put(transactionBlob, new byte[] {1}, "application/x-protobuf");
    blobs.put(intentBlob, new byte[] {2}, "application/x-protobuf");
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    assertTrue(pointers.get(transactionPointer).isEmpty());
    assertTrue(pointers.get(intentPointer).isEmpty());
    assertTrue(blobs.get(transactionBlob) == null);
    assertTrue(blobs.get(intentBlob) == null);
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
    putPointer(Keys.connectorPointerById("acct", "connector"), "broken-connector");
    when(service.accountRepo.metaFor(accountId))
        .thenReturn(meta)
        .thenThrow(new BaseResourceRepository.NotFoundException("deleted"));
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);
    pointers.failNextExcludedSweep = true;

    var response =
        service
            .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
            .await()
            .indefinitely();

    assertEquals(meta, response.getMeta());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    verify(service.accountRepo).deleteWithPrecondition(accountId, 7L);
    verify(service.credentialResolver, org.mockito.Mockito.times(2))
        .delete("acct", connectorId.getId());
  }

  @Test
  void incompletePointerSweepReportsBoundedUnexpectedKeys() {
    String accountPrefix = Keys.accountRootPrefix("acct");
    String deletionFence = Keys.accountDeletionMarker("acct");
    putPointer(deletionFence, "deletion-fence");
    for (char suffix = 'a'; suffix <= 'l'; suffix++) {
      putPointer(accountPrefix + "residual-" + suffix, "residual-" + suffix);
    }

    BaseResourceRepository.AbortRetryableException failure =
        assertThrows(
            BaseResourceRepository.AbortRetryableException.class,
            () -> service.assertAccountPointerSweepComplete(accountPrefix, deletionFence));

    String message = failure.getMessage();
    assertTrue(message.contains("account pointer sweep left 13 rows under " + accountPrefix));
    assertTrue(message.contains("deletion_fence_present=true"));
    assertTrue(message.contains("unexpected_pointer_count=12"));
    for (char suffix = 'a'; suffix <= 'j'; suffix++) {
      assertTrue(message.contains(accountPrefix + "residual-" + suffix));
    }
    assertFalse(message.contains(accountPrefix + "residual-k"));
    assertFalse(message.contains(accountPrefix + "residual-l"));
    assertTrue(message.contains("unexpected_pointer_keys_truncated=true"));
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
    assertTrue(
        Keys.accountDeletionFenceShards("acct").stream()
            .allMatch(key -> pointers.get(key).isEmpty()));
    assertEquals(0, pointers.excludedSweepCalls);
  }

  @Test
  void existingDeletionMarkerRepairsMissingFenceShards() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    installDeletionMarker(meta);
    String existingShard = Keys.accountDeletionFenceShards("acct").get(17);
    pointers.compareAndSet(
        existingShard, 0L, PointerReferences.opaqueMarkerPointer(existingShard, "deleting", 1L));
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    assertTrue(
        Keys.accountDeletionFenceShards("acct").stream()
            .allMatch(key -> pointers.get(key).isPresent()));
  }

  @Test
  void orphanedFenceShardIsRemovedBeforeFenceCreationRetries() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    String orphan = Keys.accountDeletionFenceShards("acct").get(23);
    pointers.compareAndSet(orphan, 0L, PointerReferences.opaqueMarkerPointer(orphan, "orphan", 9L));
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.deleteWithPrecondition(accountId, 7L)).thenReturn(true);

    service
        .deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(accountId).build())
        .await()
        .indefinitely();

    assertEquals(1L, pointers.get(orphan).orElseThrow().getVersion());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
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

    assertEquals(0, pointers.compareAndDeleteCalls);
    assertFalse(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    assertTrue(
        Keys.accountDeletionFenceShards("acct").stream()
            .allMatch(key -> pointers.get(key).isEmpty()));
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

  @Test
  void fenceBlockedUpdateUsesStructuredFailedPrecondition() {
    MutationMeta meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    Account current = Account.newBuilder().setResourceId(accountId).setDisplayName("alpha").build();
    when(service.accountRepo.metaFor(accountId)).thenReturn(meta);
    when(service.accountRepo.getById(accountId)).thenReturn(Optional.of(current));
    doThrow(new BaseResourceRepository.AccountDeletionInProgressException("acct"))
        .when(service.accountRepo)
        .update(any(Account.class), eq(7L));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateAccount(
                        UpdateAccountRequest.newBuilder()
                            .setAccountId(accountId)
                            .setSpec(AccountSpec.newBuilder().setDisplayName("beta"))
                            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
                            .build())
                    .await()
                    .indefinitely());

    FloecatStatus decoded = FloecatStatus.fromThrowable(failure);
    assertEquals(Status.Code.FAILED_PRECONDITION, decoded.canonicalCode());
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

  private void putPointer(String key, String blobUri) {
    pointers.compareAndSet(key, 0L, PointerReferences.blobPointer(key, blobUri, 1L));
  }

  private void installDeletionMarker(MutationMeta meta) {
    String key = Keys.accountDeletionMarker("acct");
    String payload = Base64.getEncoder().encodeToString(meta.toByteArray());
    pointers.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L));
  }

  private static final class TrackingPointerStore extends InMemoryPointerStore {
    private int compareAndDeleteCalls;
    private int excludedSweepCalls;
    private boolean failNextExcludedSweep;

    @Override
    public synchronized boolean compareAndDelete(String key, long expectedVersion) {
      compareAndDeleteCalls++;
      return super.compareAndDelete(key, expectedVersion);
    }

    @Override
    public synchronized int deleteByPrefixExcluding(String prefix, String excludedKey) {
      excludedSweepCalls++;
      if (failNextExcludedSweep) {
        failNextExcludedSweep = false;
        throw new BaseResourceRepository.AbortRetryableException("injected sweep conflict");
      }
      return super.deleteByPrefixExcluding(prefix, excludedKey);
    }
  }
}
