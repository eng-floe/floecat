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
package ai.floedb.floecat.service.account.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.RecursiveResourceDropper;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AccountCleanupGuardTest {

  private static final ResourceId ACCOUNT_ID =
      ResourceId.newBuilder().setId("acct").setKind(ResourceKind.RK_ACCOUNT).build();
  private static final Timestamp CREATED = Timestamp.newBuilder().setSeconds(10L).build();

  @Test
  void lostDeleteCasStillEstablishesAbsenceAndRequiresTheSweep() {
    var before = MutationMeta.newBuilder().setPointerVersion(7L).setEtag("before").build();
    var absent = MutationMeta.newBuilder().setPointerVersion(0L).build();

    assertEquals(true, AccountServiceImpl.deleteEstablishedAbsence(before, absent));
  }

  @Test
  void deleteRetryAcceptsAbsenceAfterItsPinnedAccountWasRemoved() {
    var service = new AccountServiceImpl();
    var target = new AtomicReference<AccountServiceImpl.AccountDeleteTarget>();
    var live =
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setBlobUri("blob://old")
            .setEtag("old")
            .build();

    service.pinDeleteTarget(target, live, CREATED, ACCOUNT_ID, "corr");
    service.pinDeleteTarget(
        target, MutationMeta.newBuilder().setPointerVersion(0L).build(), null, ACCOUNT_ID, "corr");
  }

  @Test
  void deleteRetryAcceptsAnUpdateOfThePinnedAccountInstance() {
    var service = new AccountServiceImpl();
    var target = new AtomicReference<AccountServiceImpl.AccountDeleteTarget>();
    service.pinDeleteTarget(
        target,
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setBlobUri("blob://before")
            .setEtag("before")
            .build(),
        CREATED,
        ACCOUNT_ID,
        "corr");

    service.pinDeleteTarget(
        target,
        MutationMeta.newBuilder()
            .setPointerVersion(8L)
            .setBlobUri("blob://after")
            .setEtag("after")
            .build(),
        CREATED,
        ACCOUNT_ID,
        "corr");
  }

  @Test
  void corruptAccountBlobFallsBackToStrictPointerIdentity() {
    var service = new AccountServiceImpl();
    service.accountRepo = mock(AccountRepository.class);
    var meta =
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setPointerKey(Keys.accountPointerById("acct"))
            .setBlobUri("blob://corrupt-account")
            .build();
    when(service.accountRepo.getByBlobUri(meta.getBlobUri()))
        .thenThrow(new BaseResourceRepository.CorruptionException("parse failed"));

    assertNull(service.accountInstanceCreatedAt(meta));
  }

  @Test
  void deleteRetryRefusesARecreatedLiveAccount() {
    var service = new AccountServiceImpl();
    var target = new AtomicReference<AccountServiceImpl.AccountDeleteTarget>();
    service.pinDeleteTarget(
        target,
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setBlobUri("blob://old")
            .setEtag("old")
            .build(),
        CREATED,
        ACCOUNT_ID,
        "corr");

    var thrown =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.pinDeleteTarget(
                    target,
                    MutationMeta.newBuilder()
                        .setPointerVersion(1L)
                        .setBlobUri("blob://new")
                        .setEtag("new")
                        .build(),
                    Timestamp.newBuilder().setSeconds(20L).build(),
                    ACCOUNT_ID,
                    "corr"));
    assertEquals(
        "account.reappeared.during.delete", FloecatStatus.fromThrowable(thrown).messageKey());
  }

  @Test
  void childPublishGuardFailureRemainsRetryableWhileAccountIsAbsent() {
    var fixture = fixture(BatchGuard.Outcome.HOLDS);

    var thrown =
        assertThrows(
            BaseResourceRepository.BatchGuardFailedException.class,
            () -> fixture.service.cleanupAccountResources(ACCOUNT_ID, "corr"));

    assertSame(fixture.failure, thrown);
  }

  @Test
  void accountReuseBecomesANonRetryableConflictWithStructuredState() {
    var fixture = fixture(BatchGuard.Outcome.BROKEN);

    var thrown =
        assertThrows(
            StatusRuntimeException.class,
            () -> fixture.service.cleanupAccountResources(ACCOUNT_ID, "corr"));

    var status = FloecatStatus.fromThrowable(thrown);
    assertEquals(io.grpc.Status.Code.ABORTED, status.canonicalCode());
    assertEquals(ErrorCode.MC_CONFLICT, status.errorCode());
    assertEquals("account.reappeared.during.delete", status.messageKey());
    assertEquals("acct", status.params().get("id"));
  }

  @Test
  void connectorDeleteFalseRetriesWhenThePointerIsStillLive() {
    var service = emptyCleanupService();
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    doAnswer(
            invocation -> {
              java.util.function.Consumer<ResourceId> consumer = invocation.getArgument(1);
              consumer.accept(connectorId);
              return null;
            })
        .when(service.connectorRepo)
        .forEachId(eq("acct"), any());
    when(service.connectorRepo.prepareCredentialCleanup(connectorId)).thenReturn(List.of());
    when(service.connectorRepo.credentialCleanupReadyGuard(connectorId))
        .thenReturn(BatchGuard.NONE);
    when(service.connectorRepo.delete(eq(connectorId), any())).thenReturn(false);
    when(service.connectorRepo.metaForSafe(connectorId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(4L).build());

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> service.cleanupAccountResources(ACCOUNT_ID, "corr"));
  }

  @Test
  void storageAuthorityDeleteFalseRetriesWhenThePointerIsStillLive() {
    var service = emptyCleanupService();
    var authorityId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("authority")
            .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
            .build();
    doAnswer(
            invocation -> {
              java.util.function.Consumer<ResourceId> consumer = invocation.getArgument(1);
              consumer.accept(authorityId);
              return null;
            })
        .when(service.storageAuthorityRepo)
        .forEachId(eq("acct"), any());
    when(service.storageAuthorityRepo.prepareCredentialCleanup(authorityId))
        .thenReturn(mock(StorageAuthorityRepository.CredentialCleanup.class));
    when(service.storageAuthorityRepo.credentialCleanupReadyGuard(authorityId))
        .thenReturn(BatchGuard.NONE);
    when(service.storageAuthorityRepo.delete(eq(authorityId), any())).thenReturn(false);
    when(service.storageAuthorityRepo.metaForSafe(authorityId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(4L).build());

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> service.cleanupAccountResources(ACCOUNT_ID, "corr"));
  }

  @Test
  void accountCleanupExecutesResidualTableWorkBeforeDeletingItsHandles() {
    var service = emptyCleanupService();
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("orphan-table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    doAnswer(
            invocation -> {
              java.util.function.Consumer<ResourceId> consumer = invocation.getArgument(1);
              consumer.accept(tableId);
              return null;
            })
        .when(service.tableCleanupRepo)
        .forEachResidualTableId(eq("acct"), any());
    when(service.recursiveDropper.cleanupDeletedTable(eq(tableId), any(), any())).thenReturn(3);

    service.cleanupAccountResources(ACCOUNT_ID, "corr");

    var order = inOrder(service.recursiveDropper, service.tableCleanupRepo);
    order.verify(service.recursiveDropper).cleanupDeletedTable(eq(tableId), any(), any());
    order.verify(service.tableCleanupRepo).deleteResidualRows(eq("acct"), any(), any());
    verify(service.idempotencyStore).deleteAccountResources(eq("acct"), any(), any());
  }

  private static Fixture fixture(BatchGuard.Outcome outcome) {
    var service = new AccountServiceImpl();
    service.markerStore = mock(MarkerStore.class);
    service.connectorRepo = mock(ConnectorRepository.class);
    var accountGone = mock(BatchGuard.class);
    when(service.markerStore.pointerAbsentGuard(
            eq("account acct"), eq(Keys.accountPointerById("acct"))))
        .thenReturn(accountGone);
    when(accountGone.reevaluate()).thenReturn(outcome);
    var failure = new BaseResourceRepository.BatchGuardFailedException("child marker moved");
    doThrow(failure).when(service.connectorRepo).forEachId(eq("acct"), any());
    return new Fixture(service, failure);
  }

  private static AccountServiceImpl emptyCleanupService() {
    var service = new AccountServiceImpl();
    service.markerStore = mock(MarkerStore.class);
    service.connectorRepo = mock(ConnectorRepository.class);
    service.storageAuthorityRepo = mock(StorageAuthorityRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
    service.tableCleanupRepo = mock(TableCleanupRepository.class);
    service.transactionRepo = mock(TransactionRepository.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.recursiveDropper = mock(RecursiveResourceDropper.class);
    when(service.markerStore.pointerAbsentGuard(
            eq("account acct"), eq(Keys.accountPointerById("acct"))))
        .thenReturn(BatchGuard.NONE);
    when(service.transactionRepo.deleteAccountResources(eq("acct"), any(), any()))
        .thenReturn(new TransactionRepository.CleanupResult(0, 0));
    when(service.idempotencyStore.deleteAccountResources(eq("acct"), any(), any()))
        .thenReturn(new IdempotencyRepository.CleanupResult(0, 0));
    return service;
  }

  private record Fixture(
      AccountServiceImpl service, BaseResourceRepository.BatchGuardFailedException failure) {}
}
