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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
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

    assertEquals(
        List.of(new PointerStore.CasCheckAbsent(Keys.accountDeletionMarker(accountId))),
        AccountDeletionFence.checksForAccountWrites(
            List.of(
                new PointerStore.CasUpsert(
                    Keys.reconcileJobPointerById(accountId, "job"),
                    0L,
                    Pointer.getDefaultInstance()))));
  }
}
