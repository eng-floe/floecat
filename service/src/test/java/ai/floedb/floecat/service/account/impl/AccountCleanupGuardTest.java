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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
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

  private record Fixture(
      AccountServiceImpl service, BaseResourceRepository.BatchGuardFailedException failure) {}
}
