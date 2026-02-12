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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Test;

class TransactionIntentRepositoryTest {

  @Test
  void createEnforcesUniqueTargetPointerAcrossTransactions() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var repo = new TransactionIntentRepository(pointers, blobs);

    var first =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/tables/by-id/t1")
            .setBlobUri("s3://bucket/intent-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    repo.create(first);

    var second =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-2")
            .setTargetPointerKey("/accounts/acct/tables/by-id/t1")
            .setBlobUri("s3://bucket/intent-2")
            .setCreatedAt(Timestamps.fromMillis(2))
            .build();

    assertThrows(BaseResourceRepository.NameConflictException.class, () -> repo.create(second));

    var byTarget = repo.getByTarget("acct", "/accounts/acct/tables/by-id/t1");
    assertTrue(byTarget.isPresent());
    assertEquals("tx-1", byTarget.get().getTxId());
    assertEquals(1, repo.listByTx("acct", "tx-1").size());
    assertTrue(repo.listByTx("acct", "tx-2").isEmpty());
  }

  @Test
  void deleteBothIndicesUsesOwnerIdentityNotMutableBlobHash() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var repo = new TransactionIntentRepository(pointers, blobs);

    var initial =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/tables/by-id/t1")
            .setBlobUri("s3://bucket/intent-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    repo.create(initial);

    var mutated =
        initial.toBuilder()
            .setApplyErrorCode("EXPECTED_VERSION_MISMATCH")
            .setApplyErrorMessage("mismatch")
            .setApplyErrorAt(Timestamps.fromMillis(2))
            .build();
    assertTrue(repo.update(mutated));

    assertTrue(repo.deleteBothIndicesBestEffort(initial));
    assertTrue(repo.getByTarget("acct", "/accounts/acct/tables/by-id/t1").isEmpty());
    assertTrue(repo.listByTx("acct", "tx-1").isEmpty());
  }
}
