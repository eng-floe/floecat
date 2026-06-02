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
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  void create_withMissingSecondaryPointer_signalsRetryable() {
    var repo = new AccountRepository(ptr, blobs);
    var account = account("acct-1", "alpha", "");
    repo.create(account);

    // Mimic a legacy / partially-applied state: the canonical pointer survives (bound to our
    // blob) but the secondary is gone.
    assertThat(ptr.delete(Keys.accountPointerByName("alpha"))).isTrue();

    assertThatThrownBy(() -> repo.create(account))
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

  /** Forwards every {@link PointerStore} call to a delegate; subclasses override one behavior. */
  private abstract static class DelegatingPointerStore implements PointerStore {
    final PointerStore delegate;

    DelegatingPointerStore(PointerStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      return delegate.compareAndSetBatch(ops);
    }

    @Override
    public Optional<Pointer> get(String key) {
      return delegate.get(key);
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      return delegate.compareAndSet(key, expectedVersion, next);
    }

    @Override
    public boolean delete(String key) {
      return delegate.delete(key);
    }

    @Override
    public boolean compareAndDelete(String key, long expectedVersion) {
      return delegate.compareAndDelete(key, expectedVersion);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    @Override
    public int deleteByPrefix(String prefix) {
      return delegate.deleteByPrefix(prefix);
    }

    @Override
    public int countByPrefix(String prefix) {
      return delegate.countByPrefix(prefix);
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }
  }

  /** PointerStore decorator that fails every batch CAS, simulating a transient storage error. */
  private static final class FailingBatchPointerStore extends DelegatingPointerStore {
    private FailingBatchPointerStore(PointerStore delegate) {
      super(delegate);
    }

    static final class InjectedBatchFailure extends RuntimeException {
      InjectedBatchFailure(String message) {
        super(message);
      }
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      throw new InjectedBatchFailure("injected batch storage failure");
    }
  }

  /** Mimics DynamoDB: a transaction with two operations on the same key is rejected. */
  private static final class DuplicateKeyRejectingPointerStore extends DelegatingPointerStore {
    private DuplicateKeyRejectingPointerStore(PointerStore delegate) {
      super(delegate);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      Set<String> seen = new HashSet<>();
      for (CasOp op : ops) {
        String key = op instanceof CasUpsert u ? u.key() : ((CasDelete) op).key();
        if (!seen.add(key)) {
          throw new IllegalArgumentException("duplicate key in transactional batch: " + key);
        }
      }
      return delegate.compareAndSetBatch(ops);
    }
  }
}
