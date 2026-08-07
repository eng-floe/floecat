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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.CredentialCleanupState;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorCredentialCleanupTest {
  @Test
  void cleanupHandleSurvivesPointerDeleteAndCannotBeClaimedBeforeIt() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    put(pointers, Keys.connectorPointerById("acct", "connector-1"));

    var cleanup = repo.prepareCredentialCleanup(connectorId).getFirst();
    assertTrue(repo.claimCredentialCleanup(cleanup, BatchGuard.NONE).isEmpty());

    pointers.delete(Keys.connectorPointerById("acct", "connector-1"));
    var claimed = repo.claimCredentialCleanup(cleanup, BatchGuard.NONE);

    assertTrue(claimed.isPresent(), "pointer removal makes the external cleanup safe");
    assertTrue(pointers.get(cleanup.pointerKey()).isPresent(), "the task survives a crash");
    repo.completeCredentialCleanup(claimed.orElseThrow());
    assertFalse(pointers.get(cleanup.pointerKey()).isPresent());
  }

  @Test
  void cleanupCannotBeClaimedWhenAccountReappearsWithConnectorGone() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var cleanup = repo.prepareCredentialCleanup(connectorId).getFirst();
    String accountPointer = Keys.accountPointerById("acct");
    put(pointers, accountPointer);
    BatchGuard accountGone =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of(new PointerStore.CasCheckAbsent(accountPointer));
          }

          @Override
          public Outcome reevaluate() {
            return pointers.get(accountPointer).isPresent() ? Outcome.BROKEN : Outcome.HOLDS;
          }

          @Override
          public String describe() {
            return "account acct";
          }
        };

    assertThrows(
        BaseResourceRepository.BatchGuardFailedException.class,
        () -> repo.claimCredentialCleanup(cleanup, accountGone));
    assertTrue(
        pointers.get(cleanup.pointerKey()).isPresent(),
        "account reactivation leaves the external credential task intact");
  }

  @Test
  void inFlightCredentialWriteBlocksCleanupUntilResourcePublishReleasesIt() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();

    var write = repo.beginCredentialWrite(connectorId, 0L, BatchGuard.NONE);
    var pending = repo.pendingCredentialCleanups(connectorId).getFirst();
    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> repo.claimCredentialCleanup(pending, BatchGuard.NONE));
    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> repo.prepareCredentialCleanup(connectorId));

    repo.create(
        Connector.newBuilder().setResourceId(connectorId).setDisplayName("connector").build(),
        repo.credentialWriteCommitGuard(write));
    var ready = repo.prepareCredentialCleanup(connectorId).getFirst();
    assertTrue(
        repo.deleteWithPrecondition(
            connectorId, 1L, repo.credentialCleanupReadyGuard(connectorId)));
    assertTrue(repo.claimCredentialCleanup(ready, BatchGuard.NONE).isPresent());
  }

  @Test
  void elapsedCredentialWriterRemainsFencedFromCleanup() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();

    var write = repo.beginCredentialWrite(connectorId, 0L, BatchGuard.NONE);
    var writing = pointers.get(write.pointerKey()).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            write.pointerKey(),
            writing.getVersion(),
            writing.toBuilder().setBlobUri("credential-write:0:expired").build()));

    var pending = repo.pendingCredentialCleanups(connectorId).getFirst();
    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> repo.claimCredentialCleanup(pending, BatchGuard.NONE));
    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> repo.beginCredentialWrite(connectorId, 0L, BatchGuard.NONE));
    assertFalse(repo.credentialWriteCommitted(write));
    assertTrue(CredentialCleanupState.isWriting(pointers.get(write.pointerKey()).orElseThrow()));
  }

  @Test
  void failedCreateCanRemoveItsCompensatedCleanupRow() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();

    var write = repo.beginCredentialWrite(connectorId, 0L, BatchGuard.NONE);
    repo.abortCredentialCreate(write);

    assertTrue(repo.pendingCredentialCleanups(connectorId).isEmpty());
  }

  private static void put(InMemoryPointerStore pointers, String key) {
    pointers.compareAndSet(key, 0L, Pointer.newBuilder().setKey(key).setVersion(1L).build());
  }
}
