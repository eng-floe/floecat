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
package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class AccountDeletionFenceTest {
  @Test
  void reservedAccountDirectoriesDoNotCreateFenceChecks() {
    Pointer pointer = Pointer.getDefaultInstance();

    assertTrue(
        AccountDeletionFence.checksForAccountWrites(
                List.of(
                    new PointerStore.CasUpsert(Keys.accountPointerById("account"), 0L, pointer),
                    new PointerStore.CasUpsert(
                        Keys.accountPointerByName("display-name"), 0L, pointer)))
            .isEmpty());
  }

  @Test
  void accountScopedWriteCreatesItsExactFenceCheck() {
    String accountId = "account/with+encoding";
    String writeKey = Keys.reconcileJobPointerById(accountId, "job");

    assertEquals(
        List.of(
            new PointerStore.CasCheckAbsent(Keys.accountDeletionFenceShard(accountId, writeKey))),
        AccountDeletionFence.checksForAccountWrites(
            List.of(new PointerStore.CasUpsert(writeKey, 0L, Pointer.getDefaultInstance()))));
  }

  @Test
  void accountScopedBatchUsesOnlyOneShardPerAccount() {
    String accountId = "account";

    assertEquals(
        1,
        AccountDeletionFence.checksForAccountWrites(
                List.of(
                    new PointerStore.CasUpsert(
                        Keys.reconcileJobPointerById(accountId, "job-a"),
                        0L,
                        Pointer.getDefaultInstance()),
                    new PointerStore.CasUpsert(
                        Keys.reconcileJobPointerById(accountId, "job-b"),
                        0L,
                        Pointer.getDefaultInstance())))
            .size());
  }

  @Test
  void independentWritesSpreadAcrossShards() {
    String accountId = "account";

    long distinctShards =
        java.util.stream.IntStream.range(0, 64)
            .mapToObj(i -> Keys.reconcileJobPointerById(accountId, "job-" + i))
            .map(key -> Keys.accountDeletionFenceShard(accountId, key))
            .distinct()
            .count();

    assertTrue(distinctShards > 1);
  }

  @Test
  void selectedNonzeroShardBlocksItsWrite() {
    String accountId = "account";
    String firstShard = Keys.accountDeletionFenceShards(accountId).getFirst();
    String writeKey =
        java.util.stream.IntStream.range(0, 1024)
            .mapToObj(i -> Keys.reconcileJobPointerById(accountId, "job-" + i))
            .filter(key -> !Keys.accountDeletionFenceShard(accountId, key).equals(firstShard))
            .findFirst()
            .orElseThrow();
    String selectedShard = Keys.accountDeletionFenceShard(accountId, writeKey);
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    assertTrue(
        pointers.compareAndSet(
            selectedShard,
            0L,
            PointerReferences.opaqueMarkerPointer(selectedShard, "deleting", 1L)));

    assertFalse(
        AccountDeletionFence.compareAndSet(
            pointers, accountId, writeKey, 0L, Pointer.getDefaultInstance()));
  }
}
