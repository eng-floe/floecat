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

package ai.floedb.floecat.service.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.account.AccountGcAuthority.AccountMode;
import ai.floedb.floecat.service.repo.cache.AccountFencedPointerStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AccountGcAuthorityTest {

  private static final String ACCOUNT = "account-1";
  private static final String INCARNATION = "pod-a/start-1";

  @Test
  void standaloneOwnsEveryAccountWithoutControlTraffic() {
    AccountGcAuthority authority = AccountGcAuthority.standaloneForTesting();

    try (var resolution = authority.admitResolution(ACCOUNT);
        var mutation = authority.admitMutation(ACCOUNT);
        var gc = authority.tryAcquireGc(ACCOUNT).orElseThrow()) {
      assertThat(gc.valid()).isTrue();
      assertThat(authority.status(ACCOUNT).mode()).isEqualTo(AccountMode.SERVING);
    }
  }

  @Test
  void managedModeFailsClosedUntilCoreAssignsTheAccount() {
    AccountGcAuthority authority = managed(new AtomicLong());

    assertThatThrownBy(() -> authority.admitResolution(ACCOUNT))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThatThrownBy(() -> authority.admitMutation(ACCOUNT))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(authority.tryAcquireGc(ACCOUNT)).isEmpty();
    assertThat(authority.status(ACCOUNT).mode()).isEqualTo(AccountMode.UNASSIGNED);
  }

  @Test
  void drainRejectsNewWorkAndRevokesAnActiveGcPermit() {
    AtomicLong references = new AtomicLong(3);
    AccountGcAuthority authority = managed(references);
    authority.apply(ACCOUNT, 1, INCARNATION, AccountMode.SERVING, true);
    var resolution = authority.admitResolution(ACCOUNT);
    var mutation = authority.admitMutation(ACCOUNT);
    var gc = authority.tryAcquireGc(ACCOUNT).orElseThrow();

    authority.apply(ACCOUNT, 2, INCARNATION, AccountMode.DRAINING, false);

    assertThat(gc.valid()).isFalse();
    assertThat(authority.tryAcquireGc(ACCOUNT)).isEmpty();
    assertThatThrownBy(() -> authority.admitResolution(ACCOUNT))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThatThrownBy(() -> authority.admitMutation(ACCOUNT))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(authority.status(ACCOUNT))
        .extracting(
            AccountGcAuthority.Status::activeResolutions,
            AccountGcAuthority.Status::activeMutations,
            AccountGcAuthority.Status::activeGc,
            AccountGcAuthority.Status::referencedRoots)
        .containsExactly(1L, 1L, 1L, 3L);

    gc.close();
    mutation.close();
    resolution.close();

    assertThat(authority.status(ACCOUNT).drained()).isFalse();
    references.set(0);
    assertThat(authority.status(ACCOUNT).drained()).isTrue();
  }

  @Test
  void commandsAreIncarnationTargetedVersionedAndIdempotent() {
    AtomicInteger warms = new AtomicInteger();
    AccountGcAuthority authority =
        AccountGcAuthority.managedForTesting(
            INCARNATION, ignored -> 0L, ignored -> warms.incrementAndGet(), ignored -> "UNLOADED");

    assertThatThrownBy(
            () -> authority.apply(ACCOUNT, 1, "pod-b/start-9", AccountMode.SERVING, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("incarnation");

    authority.apply(ACCOUNT, 2, INCARNATION, AccountMode.SERVING, false);
    authority.apply(ACCOUNT, 2, INCARNATION, AccountMode.SERVING, false);
    assertThat(warms).hasValue(1);

    assertThatThrownBy(() -> authority.apply(ACCOUNT, 1, INCARNATION, AccountMode.DRAINING, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stale");
    assertThatThrownBy(() -> authority.apply(ACCOUNT, 2, INCARNATION, AccountMode.DRAINING, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("conflicting");
  }

  @Test
  void thePointerStoreSeamFencesEveryAccountScopedPublication() {
    AccountGcAuthority authority = managed(new AtomicLong());
    InMemoryPointerStore durable = new InMemoryPointerStore();
    AccountFencedPointerStore pointers = new AccountFencedPointerStore(durable, authority);
    String scopedKey = Keys.tablePointerById(ACCOUNT, "table-1");
    String cleanupKey =
        Keys.catalogIntegrationCredentialCleanupPointer(ACCOUNT, "integration-1", 1L);
    String globalKey = Keys.accountPointerById(ACCOUNT);

    assertThatThrownBy(() -> pointers.compareAndSet(scopedKey, 0L, pointer(scopedKey)))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThatThrownBy(() -> pointers.compareAndSet(cleanupKey, 0L, pointer(cleanupKey)))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThat(pointers.compareAndSet(globalKey, 0L, pointer(globalKey))).isTrue();

    authority.apply(ACCOUNT, 1, INCARNATION, AccountMode.SERVING, false);
    assertThat(pointers.compareAndSet(scopedKey, 0L, pointer(scopedKey))).isTrue();

    authority.apply(ACCOUNT, 2, INCARNATION, AccountMode.DRAINING, false);
    assertThatThrownBy(() -> pointers.delete(scopedKey))
        .isInstanceOf(StorageAbortRetryableException.class);
    assertThatThrownBy(
            () -> pointers.deleteByPrefix(Keys.accountRootPrefix(ACCOUNT).replaceFirst("/$", "")))
        .isInstanceOf(StorageAbortRetryableException.class);
  }

  private static Pointer pointer(String key) {
    return Pointer.newBuilder().setKey(key).setBlobUri("s3://blob").build();
  }

  private static AccountGcAuthority managed(AtomicLong references) {
    return AccountGcAuthority.managedForTesting(
        INCARNATION, ignored -> references.get(), ignored -> {}, ignored -> "UNLOADED");
  }
}
