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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageAuthorityCredentialCleanupTest {
  private static final String ACCOUNT = "acct-1";
  private static final ResourceId ACCOUNT_ID =
      ResourceId.newBuilder().setId(ACCOUNT).setKind(ResourceKind.RK_ACCOUNT).build();
  private static final ResourceId AUTHORITY_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("authority-1")
          .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
          .build();

  private InMemoryPointerStore pointers;
  private StorageAuthorityRepository authorities;
  private AccountRepository accounts;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    authorities = new StorageAuthorityRepository(pointers, blobs);
    accounts = new AccountRepository(pointers, blobs);
    accounts.create(Account.newBuilder().setResourceId(ACCOUNT_ID).setDisplayName("acct").build());
    authorities.create(
        StorageAuthority.newBuilder()
            .setResourceId(AUTHORITY_ID)
            .setDisplayName("warehouse")
            .build());
  }

  @Test
  void cleanupHandleSurvivesPointerDeletionUntilSecretCleanupCompletes() {
    var cleanup = authorities.prepareCredentialCleanup(AUTHORITY_ID);

    assertThat(authorities.delete(AUTHORITY_ID)).isTrue();
    assertThat(authorities.pendingCredentialCleanup(AUTHORITY_ID)).contains(cleanup);
    var claimed = authorities.claimCredentialCleanup(cleanup, BatchGuard.NONE).orElseThrow();
    assertThat(pointers.get(cleanup.pointerKey())).isPresent();

    authorities.completeCredentialCleanup(claimed);

    assertThat(pointers.get(cleanup.pointerKey())).isEmpty();
    var remaining = new ArrayList<StorageAuthorityRepository.CredentialCleanup>();
    authorities.forEachCredentialCleanup(ACCOUNT, remaining::add);
    assertThat(remaining).isEmpty();
  }

  @Test
  void accountRecreationPreventsAnOldTeardownFromClaimingTheSecretTask() {
    var cleanup = authorities.prepareCredentialCleanup(AUTHORITY_ID);
    assertThat(authorities.delete(AUTHORITY_ID)).isTrue();
    assertThat(accounts.delete(ACCOUNT_ID)).isTrue();
    BatchGuard accountGone =
        new BatchGuard() {
          @Override
          public List<ai.floedb.floecat.storage.spi.PointerStore.CasOp> ops() {
            return List.of(
                new ai.floedb.floecat.storage.spi.PointerStore.CasCheckAbsent(
                    Keys.accountPointerById(ACCOUNT)));
          }

          @Override
          public Outcome reevaluate() {
            return pointers.get(Keys.accountPointerById(ACCOUNT)).isPresent()
                ? Outcome.BROKEN
                : Outcome.HOLDS;
          }

          @Override
          public String describe() {
            return "account " + ACCOUNT;
          }
        };

    accounts.create(Account.newBuilder().setResourceId(ACCOUNT_ID).setDisplayName("new").build());

    assertThatThrownBy(() -> authorities.claimCredentialCleanup(cleanup, accountGone))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class);
    assertThat(pointers.get(cleanup.pointerKey())).isPresent();
  }

  @Test
  void inFlightCredentialWriteBlocksAuthorityDeletionUntilUpdateCommits() {
    var write = authorities.beginCredentialWrite(AUTHORITY_ID, 1L, BatchGuard.NONE);
    var pending = authorities.pendingCredentialCleanup(AUTHORITY_ID).orElseThrow();

    assertThatThrownBy(() -> authorities.claimCredentialCleanup(pending, BatchGuard.NONE))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);
    assertThatThrownBy(() -> authorities.prepareCredentialCleanup(AUTHORITY_ID))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);

    var current = authorities.getById(AUTHORITY_ID).orElseThrow();
    assertThat(
            authorities.update(
                current.toBuilder().setDescription("updated").build(),
                1L,
                authorities.credentialWriteCommitGuard(write)))
        .isTrue();
    var ready = authorities.prepareCredentialCleanup(AUTHORITY_ID);
    assertThat(
            authorities.deleteWithPrecondition(
                AUTHORITY_ID, 2L, authorities.credentialCleanupReadyGuard(AUTHORITY_ID)))
        .isTrue();
    assertThat(authorities.claimCredentialCleanup(ready, BatchGuard.NONE)).isPresent();
  }

  @Test
  void staleUpdaterCannotAcquireCredentialOwnershipAfterAuthorityDeletion() {
    assertThat(authorities.delete(AUTHORITY_ID)).isTrue();

    assertThatThrownBy(() -> authorities.beginCredentialWrite(AUTHORITY_ID, 1L, BatchGuard.NONE))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class);
    assertThat(authorities.pendingCredentialCleanup(AUTHORITY_ID)).isEmpty();
  }

  @Test
  void elapsedCredentialWriterRemainsFencedFromAuthorityCleanup() {
    var write = authorities.beginCredentialWrite(AUTHORITY_ID, 1L, BatchGuard.NONE);
    Pointer writing = pointers.get(write.pointerKey()).orElseThrow();
    assertThat(
            pointers.compareAndSet(
                write.pointerKey(),
                writing.getVersion(),
                writing.toBuilder().setBlobUri("credential-write:0:expired").build()))
        .isTrue();
    var pending = authorities.pendingCredentialCleanup(AUTHORITY_ID).orElseThrow();
    assertThatThrownBy(() -> authorities.claimCredentialCleanup(pending, BatchGuard.NONE))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);
    assertThatThrownBy(
            () ->
                authorities.deleteWithPrecondition(
                    AUTHORITY_ID, 1L, authorities.credentialCleanupReadyGuard(AUTHORITY_ID)))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);
    assertThat(authorities.credentialWriteCommitted(write)).isFalse();
  }
}
