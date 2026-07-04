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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.ConflictingBatchPointerStore;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.DuplicateKeyRejectingPointerStore;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.FailingBatchPointerStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behavioral tests for {@link ai.floedb.floecat.service.repo.util.GenericResourceRepository#create}
 * exercised through {@link AccountRepository} (canonical by-id pointer + secondary by-name
 * pointer). The contract is verified at the repository level because {@code create()} only relies
 * on the backend-agnostic {@link PointerStore#compareAndSetBatch} primitive; backend atomicity
 * parity is covered separately at the SPI level (in-memory and DynamoDB-local).
 */
class GenericResourceRepositoryCreateTest {

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
  }

  private static Account account(String id, String name, String description) {
    return Account.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(name)
        .setDescription(description)
        .build();
  }

  @Test
  void create_happyPath_resolvesByIdAndByName_atVersionOne() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");

    repo.create(account);

    assertThat(repo.getById(account.getResourceId())).isPresent();
    assertThat(repo.getByName("alpha")).isPresent();
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(ptr.get(Keys.accountPointerByName("alpha")).orElseThrow().getVersion())
        .isEqualTo(1L);
  }

  @Test
  void create_isIdempotent_forByteIdenticalValue() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");

    repo.create(account);
    assertThatCode(() -> repo.create(account)).doesNotThrowAnyException();

    // The idempotent re-create is a no-op: it does not advance either pointer's version.
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(ptr.get(Keys.accountPointerByName("alpha")).orElseThrow().getVersion())
        .isEqualTo(1L);
  }

  @Test
  void create_sameIdDifferentBytes_throwsNameConflictOnCanonicalPointer() {
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "alpha", "v1"));

    assertThatThrownBy(() -> repo.create(account("acct-1", "alpha", "v2")))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining("pointer bound to different blob")
        .hasMessageContaining(Keys.accountPointerById("acct-1"));
  }

  @Test
  void create_sameNameDifferentId_throwsNameConflict_andLeavesNoCanonicalOrphan() {
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "shared", "v1"));

    assertThatThrownBy(() -> repo.create(account("acct-2", "shared", "v2")))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining("pointer bound to different blob")
        .hasMessageContaining(Keys.accountPointerByName("shared"));

    // Atomicity: acct-2's canonical CAS (expectedVersion 0 against an absent key) would have
    // individually succeeded, but the whole batch failed on the by-name conflict, so no by-id
    // orphan is left behind.
    assertThat(ptr.get(Keys.accountPointerById("acct-2"))).isEmpty();
  }

  @Test
  void create_whenBatchThrows_leavesNoPointerState() {
    var failing = new FailingBatchPointerStore(ptr);
    var repo = new AccountRepository(failing, blobs);

    assertThatThrownBy(() -> repo.create(account("acct-1", "alpha", "")))
        .isInstanceOf(FailingBatchPointerStore.InjectedBatchFailure.class);

    // This is the regression guard: a mid-create storage failure leaves the pointer store
    // byte-for-byte unchanged, so a later create cannot collide with a stranded orphan.
    assertThat(ptr.isEmpty()).isTrue();
    assertThat(ptr.get(Keys.accountPointerById("acct-1"))).isEmpty();
    assertThat(ptr.get(Keys.accountPointerByName("alpha"))).isEmpty();
  }

  @Test
  void create_withPartialState_throwsCorruption() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    repo.create(account);

    // Mimic a legacy / partially-applied state: the canonical pointer survives (bound to our
    // blob) but the secondary is gone. An atomic create can never produce this, so under the
    // strict no-repair contract it is surfaced terminally rather than healed or retried forever.
    assertThat(ptr.delete(Keys.accountPointerByName("alpha"))).isTrue();

    assertThatThrownBy(() -> repo.create(account))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("partial create state");
  }

  @Test
  void create_whenBatchConflictsButNothingPresent_signalsRetryable() {
    // The batch reports a conflict (as a DynamoDB TransactionConflict would) but commits nothing,
    // and the read-back finds no pointer at all. That is transient, not a stable inconsistency, so
    // create() asks the caller to retry rather than surfacing a (terminal) corruption.
    var conflicting = new ConflictingBatchPointerStore(ptr);
    var repo = new AccountRepository(conflicting, blobs);

    assertThatThrownBy(() -> repo.create(account("acct-1", "alpha", "")))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);
  }

  @Test
  void create_doesNotEmitDuplicateBatchKeys_whenCanonicalIsAlsoASecondary() {
    // The snapshot schema exposes the canonical by-id pointer as a secondary too, so the batch
    // would contain two ops on the same key unless create() de-duplicates. DynamoDB rejects such
    // transactions outright; reproduce that rejection in-memory so the dedupe is locked in.
    var rejecting = new DuplicateKeyRejectingPointerStore(ptr);
    var tableRepo = new TableRepository(rejecting, blobs);
    var snapshotRepo = new SnapshotRepository(rejecting, blobs, tableRepo);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("tbl-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var snapshot = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(42L).build();

    assertThatCode(() -> snapshotRepo.create(snapshot)).doesNotThrowAnyException();
    assertThat(snapshotRepo.getById(tableId, 42L)).isPresent();
  }

  @Test
  void delete_whenBatchThrows_leavesCanonicalAndSecondaryUntouched() {
    var baseRepo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    baseRepo.create(account);

    var failingRepo = new AccountRepository(new FailingBatchPointerStore(ptr), blobs);

    assertThatThrownBy(() -> failingRepo.delete(account.getResourceId()))
        .isInstanceOf(FailingBatchPointerStore.InjectedBatchFailure.class);

    assertThat(ptr.get(Keys.accountPointerById("acct-1"))).isPresent();
    assertThat(ptr.get(Keys.accountPointerByName("alpha"))).isPresent();
    assertThat(baseRepo.getById(account.getResourceId())).isPresent();
    assertThat(baseRepo.getByName("alpha")).isPresent();
  }

  @Test
  void deleteWithPrecondition_whenBatchThrows_leavesCanonicalAndSecondaryUntouched() {
    var baseRepo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    baseRepo.create(account);
    long expectedVersion = ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion();

    var failingRepo = new AccountRepository(new FailingBatchPointerStore(ptr), blobs);

    assertThatThrownBy(
            () -> failingRepo.deleteWithPrecondition(account.getResourceId(), expectedVersion))
        .isInstanceOf(FailingBatchPointerStore.InjectedBatchFailure.class);

    assertThat(ptr.get(Keys.accountPointerById("acct-1"))).isPresent();
    assertThat(ptr.get(Keys.accountPointerByName("alpha"))).isPresent();
    assertThat(baseRepo.getById(account.getResourceId())).isPresent();
    assertThat(baseRepo.getByName("alpha")).isPresent();
  }

  @Test
  void delete_whenCanonicalDisappearsDuringRead_returnsFalse() {
    var baseRepo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    baseRepo.create(account);

    var racing =
        new RepoTestPointerStores.DelegatingPointerStore(ptr) {
          private final AtomicInteger canonicalReads = new AtomicInteger();

          @Override
          public Optional<ai.floedb.floecat.common.rpc.Pointer> get(String key) {
            if (Keys.accountPointerById("acct-1").equals(key)
                && canonicalReads.incrementAndGet() == 2) {
              ptr.delete(Keys.accountPointerById("acct-1"));
              ptr.delete(Keys.accountPointerByName("alpha"));
              return Optional.empty();
            }
            return super.get(key);
          }
        };
    var repo = new AccountRepository(racing, blobs);

    assertThat(repo.delete(account.getResourceId())).isFalse();
  }

  @Test
  void delete_whenSecondaryAlreadyAbsent_checksItStayedAbsent() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    repo.create(account);
    assertThat(ptr.delete(Keys.accountPointerByName("alpha"))).isTrue();

    var capturing =
        new RepoTestPointerStores.DelegatingPointerStore(ptr) {
          private List<PointerStore.CasOp> capturedOps = List.of();

          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            capturedOps = new ArrayList<>(ops);
            return super.compareAndSetBatch(ops);
          }
        };
    var capturingRepo = new AccountRepository(capturing, blobs);

    assertThat(capturingRepo.delete(account.getResourceId())).isTrue();
    assertThat(capturing.capturedOps)
        .anySatisfy(
            op -> {
              assertThat(op).isInstanceOf(PointerStore.CasCheckAbsent.class);
              assertThat(((PointerStore.CasCheckAbsent) op).key())
                  .isEqualTo(Keys.accountPointerByName("alpha"));
            });
  }

  @Test
  void delete_whenResourceBlobIsCorrupt_removesCanonicalPointer() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    repo.create(account);

    String blobUri = ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getBlobUri();
    assertThat(blobs.delete(blobUri)).isTrue();

    assertThat(repo.delete(account.getResourceId())).isTrue();

    assertThat(ptr.get(Keys.accountPointerById("acct-1"))).isEmpty();
    assertThat(ptr.get(Keys.accountPointerByName("alpha"))).isPresent();
  }
}
