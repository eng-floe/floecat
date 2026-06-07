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
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.ConflictingBatchPointerStore;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores.FailingBatchPointerStore;
import ai.floedb.floecat.service.repo.model.AccountKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behavioral tests for {@link GenericResourceRepository#createIfAbsent}. Like {@code create()} it
 * now reserves the canonical and every secondary pointer in a single atomic {@code
 * compareAndSetBatch}, so it can never leave partial pointer state; the classification of a
 * non-committing batch is what these tests pin down.
 */
class GenericResourceRepositoryCreateIfAbsentTest {

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
  }

  private GenericResourceRepository<Account, AccountKey> repo() {
    return repoWith(ptr);
  }

  private GenericResourceRepository<Account, AccountKey> repoWith(PointerStore pointerStore) {
    return new GenericResourceRepository<>(
        pointerStore,
        blobs,
        Schemas.ACCOUNT,
        Account::parseFrom,
        Account::toByteArray,
        "application/x-protobuf");
  }

  private static Account account(String id, String name, String description) {
    return Account.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(name)
        .setDescription(description)
        .build();
  }

  @Test
  void createIfAbsent_happyPath_returnsTrue_resolvesByIdAndName() {
    var repo = repo();

    assertThat(repo.createIfAbsent(account("acct-1", "alpha", ""))).isTrue();

    assertThat(repo.getByKey(new AccountKey("acct-1"))).isPresent();
    assertThat(repo.get(Keys.accountPointerByName("alpha"))).isPresent();
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(ptr.get(Keys.accountPointerByName("alpha")).orElseThrow().getVersion())
        .isEqualTo(1L);
  }

  @Test
  void createIfAbsent_whenAlreadyPresent_returnsFalse_withoutAdvancingVersions() {
    var repo = repo();
    repo.createIfAbsent(account("acct-1", "alpha", ""));

    assertThat(repo.createIfAbsent(account("acct-1", "alpha", ""))).isFalse();

    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(ptr.get(Keys.accountPointerByName("alpha")).orElseThrow().getVersion())
        .isEqualTo(1L);
  }

  @Test
  void createIfAbsent_whenCanonicalExistsBoundToDifferentBlob_returnsFalse() {
    // createIfAbsent reports a lost race (false) whenever the canonical pointer already exists,
    // regardless of which blob it is bound to — unlike create(), which would throw NameConflict.
    var repo = repo();
    repo.createIfAbsent(account("acct-1", "alpha", "v1"));

    assertThat(repo.createIfAbsent(account("acct-1", "alpha", "v2"))).isFalse();

    // The incumbent pointer is untouched.
    assertThat(ptr.get(Keys.accountPointerById("acct-1")).orElseThrow().getVersion()).isEqualTo(1L);
    assertThat(repo.getByKey(new AccountKey("acct-1")).orElseThrow().getDescription())
        .isEqualTo("v1");
  }

  @Test
  void createIfAbsent_sameNameDifferentId_throwsNameConflict_andLeavesNoCanonicalOrphan() {
    var repo = repo();
    repo.createIfAbsent(account("acct-1", "shared", "v1"));

    assertThatThrownBy(() -> repo.createIfAbsent(account("acct-2", "shared", "v2")))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining(Keys.accountPointerByName("shared"));

    assertThat(ptr.get(Keys.accountPointerById("acct-2"))).isEmpty();
  }

  @Test
  void createIfAbsent_withCanonicalAbsentButSecondaryPresent_throwsCorruption() {
    var repo = repo();
    repo.createIfAbsent(account("acct-1", "alpha", ""));

    // Legacy / partially-applied state an atomic path can never produce: the canonical by-id
    // pointer is gone but the by-name secondary survives (bound to our blob). Strict no-repair:
    // surface it terminally.
    assertThat(ptr.delete(Keys.accountPointerById("acct-1"))).isTrue();

    assertThatThrownBy(() -> repo.createIfAbsent(account("acct-1", "alpha", "")))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("partial create state");
  }

  @Test
  void createIfAbsent_whenBatchConflictsButNothingPresent_signalsRetryable() {
    var repo = repoWith(new ConflictingBatchPointerStore(ptr));

    assertThatThrownBy(() -> repo.createIfAbsent(account("acct-1", "alpha", "")))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);
  }

  @Test
  void createIfAbsent_whenBatchThrows_leavesNoPointerState() {
    var repo = repoWith(new FailingBatchPointerStore(ptr));

    assertThatThrownBy(() -> repo.createIfAbsent(account("acct-1", "alpha", "")))
        .isInstanceOf(FailingBatchPointerStore.InjectedBatchFailure.class);

    assertThat(ptr.isEmpty()).isTrue();
  }
}
