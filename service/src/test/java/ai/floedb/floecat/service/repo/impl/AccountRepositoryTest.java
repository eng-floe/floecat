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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import org.junit.jupiter.api.Test;

class AccountRepositoryTest {

  @Test
  void listFailsWhenPointerBlobIsMissing() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    AccountRepository repo = new AccountRepository(pointerStore, blobStore);
    Account account =
        Account.newBuilder()
            .setResourceId(
                ResourceId.newBuilder().setId("acct-1").setKind(ResourceKind.RK_ACCOUNT).build())
            .setDisplayName("alpha")
            .build();

    repo.create(account);
    String pointerKey = Keys.accountPointerByName(account.getDisplayName());
    String blobUri = pointerStore.get(pointerKey).orElseThrow().getBlobUri();
    blobStore.delete(blobUri);

    assertThatThrownBy(() -> repo.list(10, "", new StringBuilder()))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("missing blob");
  }
}
