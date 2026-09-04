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

package ai.floedb.floecat.service.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.catalog.impl.RootResyncQueue;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PinnedReadContractTest {

  private PinnedReadContract contract;
  private InMemoryPointerStore repairPointers;

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("t")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @BeforeEach
  void setUp() {
    // A real repair pipeline over an in-memory store, so tests can assert which integrity
    // failures durably enqueue the table for the resync re-drive and which do not.
    repairPointers = new InMemoryPointerStore();
    contract = new PinnedReadContract(new RootRepairRequests(new RootResyncQueue(repairPointers)));
  }

  private boolean repairEnqueued(ResourceId tableId) {
    return repairPointers
        .get(Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId()))
        .isPresent();
  }

  @Test
  void aMissingPinnedTableBlobRaisesInternalAndEnqueuesRepair() {
    assertThrows(
        StatusRuntimeException.class,
        () -> contract.requirePinnedTableBlob(java.util.Optional.empty(), "corr", TABLE));
    // The committed root names a blob no read can load; without a re-derived root every future
    // query fails identically, so the failure durably enqueues the table for repair.
    assertTrue(repairEnqueued(TABLE));
  }

  @Test
  void aMissingPinnedSnapshotBlobRaisesInternalAndEnqueuesRepair() {
    assertThrows(
        StatusRuntimeException.class,
        () -> contract.requirePinnedSnapshotBlob(java.util.Optional.empty(), "corr", TABLE, 7L));
    assertTrue(repairEnqueued(TABLE));
  }

  @Test
  void aPresentPinnedBlobUnwrapsWithoutRepair() {
    assertEquals(
        "blob", contract.requirePinnedTableBlob(java.util.Optional.of("blob"), "corr", TABLE));
    assertFalse(repairEnqueued(TABLE));
  }
}
