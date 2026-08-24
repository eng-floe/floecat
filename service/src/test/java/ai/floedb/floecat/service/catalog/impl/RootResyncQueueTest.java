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

package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RootResyncQueueTest {

  @Test
  void enqueueBacksOffBetweenTransientStoreFailures() {
    var transientFailures = new AtomicInteger(2);
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            if (transientFailures.getAndDecrement() > 0) {
              throw new IllegalStateException("transient pointer-store failure");
            }
            return super.compareAndSetBatch(ops);
          }
        };
    var backoffs = new ArrayList<Long>();
    var queue = new RootResyncQueue(pointers, backoffs::add);
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-transient")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    queue.enqueue(tableId);

    assertEquals(List.of(10L, 25L), backoffs);
    assertTrue(
        pointers.get(Keys.rootResyncPendingPointer("acct", "tbl-transient")).isPresent(),
        "the recovered enqueue still records the pending root resync marker");
  }

  @Test
  void enqueueDoesNotRecreateMarkerForDeletingAccount() {
    var pointers = new InMemoryPointerStore();
    var queue = new RootResyncQueue(pointers);
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-deleting")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String fence = Keys.accountDeletionMarker("acct");
    pointers.compareAndSet(fence, 0L, PointerReferences.opaqueMarkerPointer(fence, "acct", 1L));

    queue.enqueue(tableId);

    assertTrue(pointers.get(Keys.rootResyncPendingPointer("acct", "tbl-deleting")).isEmpty());
  }
}
