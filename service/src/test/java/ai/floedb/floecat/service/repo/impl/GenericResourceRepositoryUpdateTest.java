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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.FailingBatchPointerStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behavioral tests for {@link ai.floedb.floecat.service.repo.util.GenericResourceRepository#update}
 * exercised through {@link AccountRepository}. The update now applies the canonical advance, new
 * secondary reservations, kept-secondary blob refreshes and removed-secondary deletes in a single
 * atomic {@code compareAndSetBatch}, so it can never leave partial pointer state.
 */
class GenericResourceRepositoryUpdateTest {

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
  }

  private static ResourceId id(String accountId) {
    return ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build();
  }

  private static Account account(String accountId, String name, String description) {
    return Account.newBuilder()
        .setResourceId(id(accountId))
        .setDisplayName(name)
        .setDescription(description)
        .build();
  }

  @Test
  void update_rename_movesSecondaryAtomically() {
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "alpha", ""));

    assertThat(repo.update(account("acct-1", "beta", ""), 1L)).isTrue();

    assertThat(repo.getByName("beta")).isPresent();
    assertThat(repo.getByName("alpha")).isEmpty();
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(2L);
    assertThat(ptr.get(Keys.accountPointerByName("beta")).orElseThrow().getVersion()).isEqualTo(1L);
  }

  @Test
  void update_keepsName_refreshesKeptSecondaryOntoNewBlob() {
    // Account uses content-addressed (casBlobs) blobs, so changing the description moves the blob
    // URI. The kept by-name secondary must be advanced onto the new blob in the same batch.
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "alpha", "v1"));

    assertThat(repo.update(account("acct-1", "alpha", "v2"), 1L)).isTrue();

    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(2L);
    assertThat(ptr.get(Keys.accountPointerByName("alpha")).orElseThrow().getVersion())
        .isEqualTo(2L);
    // The by-name secondary still resolves, and to the updated content.
    assertThat(repo.getByName("alpha").orElseThrow().getDescription()).isEqualTo("v2");
  }

  @Test
  void update_wrongExpectedVersion_returnsFalse_andLeavesPointersUnchanged() {
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "alpha", ""));

    assertThat(repo.update(account("acct-1", "beta", ""), 5L)).isFalse();

    // Nothing moved: the rename neither created the new name nor dropped the old one.
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(repo.getByName("alpha")).isPresent();
    assertThat(ptr.get(Keys.accountPointerByName("beta"))).isEmpty();
  }

  @Test
  void update_newNameOwnedByAnotherResource_throwsNameConflict_andLeavesNoPartialState() {
    var repo = new AccountRepository(ptr, blobs);
    repo.create(account("acct-1", "alpha", ""));
    repo.create(account("acct-2", "beta", ""));

    assertThatThrownBy(() -> repo.update(account("acct-1", "beta", ""), 1L))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining(Keys.accountPointerByName("beta"));

    // acct-1 is untouched and "beta" still belongs to acct-2.
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(repo.getByName("alpha")).isPresent();
    assertThat(repo.getByName("beta").orElseThrow().getResourceId().getId()).isEqualTo("acct-2");
  }

  @Test
  void update_whenBatchThrows_leavesNoPartialState() {
    var realRepo = new AccountRepository(ptr, blobs);
    realRepo.create(account("acct-1", "alpha", ""));

    var failingRepo = new AccountRepository(new FailingBatchPointerStore(ptr), blobs);

    assertThatThrownBy(() -> failingRepo.update(account("acct-1", "beta", ""), 1L))
        .isInstanceOf(FailingBatchPointerStore.InjectedBatchFailure.class);

    // The rename committed nothing: old name kept, new name absent, canonical version unchanged.
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(realRepo.getByName("alpha")).isPresent();
    assertThat(ptr.get(Keys.accountPointerByName("beta"))).isEmpty();
  }

  @Test
  void update_missingResource_throwsNotFound() {
    var repo = new AccountRepository(ptr, blobs);

    assertThatThrownBy(() -> repo.update(account("acct-missing", "alpha", ""), 1L))
        .isInstanceOf(BaseResourceRepository.NotFoundException.class);
  }
}
