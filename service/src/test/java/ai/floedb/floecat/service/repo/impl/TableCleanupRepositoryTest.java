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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TableCleanupRepositoryTest {

  @Test
  void pendingTaskProbeContinuesPastAnEmptyPage() {
    var calls = new AtomicInteger();
    var task = Pointer.newBuilder().setKey("/cleanup").setVersion(1L).build();
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public List<Pointer> listPointersByPrefix(
              String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
            if (calls.getAndIncrement() == 0) {
              nextTokenOut.append("next");
              return List.of();
            }
            return List.of(task);
          }
        };
    var repository = new TableCleanupRepository(pointers);
    var namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns").build();

    assertThat(repository.hasAny(namespaceId)).isTrue();
    assertThat(calls).hasValue(2);
  }

  @Test
  void malformedNamespaceTaskIsReclaimedInsteadOfWedgingEmptiness() {
    var pointers = new InMemoryPointerStore();
    var repository = new TableCleanupRepository(pointers);
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    String key = Keys.namespaceTableCleanupPointer("acct", "ns", "broken");
    pointers.compareAndSet(
        key,
        0L,
        Pointer.newBuilder().setKey(key).setVersion(1L).setBlobUri("cleanup://broken").build());

    assertThat(repository.hasAny(namespaceId)).isTrue();
    repository.forEach(namespaceId, cleanup -> {});

    assertThat(repository.hasAny(namespaceId)).isFalse();
  }

  @Test
  void durableTaskSurvivesUntilTheTableIsAbsentAndCleanupCompletes() {
    var pointers = new InMemoryPointerStore();
    var repository = new TableCleanupRepository(pointers);
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String tableKey = Keys.tablePointerById("acct", "table");
    pointers.compareAndSet(
        tableKey,
        0L,
        PointerReferences.blobPointer(tableKey, "blob://table", 1L, tableId, "orders"));

    var staged = repository.prepare(namespaceId, tableId);
    var listed = new ArrayList<TableCleanupRepository.Cleanup>();
    repository.forEach(namespaceId, listed::add);

    assertThat(listed).hasSize(1);
    assertThat(repository.claim(staged, BatchGuard.NONE)).isEmpty();
    assertThat(pointers.get(staged.pointerKey())).isPresent();

    pointers.delete(tableKey);
    var claimed = repository.claim(staged, BatchGuard.NONE).orElseThrow();
    repository.complete(claimed);

    assertThat(pointers.get(staged.pointerKey())).isEmpty();
  }

  @Test
  void claimedCleanupCannotDeleteOwnedStateAfterTheTableIdIsReused() {
    var pointers = new InMemoryPointerStore();
    var repository = new TableCleanupRepository(pointers);
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var staged = repository.prepare(namespaceId, tableId);
    var claimed = repository.claim(staged, BatchGuard.NONE).orElseThrow();
    String snapshotKey = Keys.snapshotPointerById("acct", "table", 1L);
    pointers.compareAndSet(
        snapshotKey, 0L, PointerReferences.opaqueMarkerPointer(snapshotKey, "snapshot", 1L));

    String tableKey = Keys.tablePointerById("acct", "table");
    pointers.compareAndSet(
        tableKey,
        0L,
        PointerReferences.blobPointer(tableKey, "blob://replacement", 1L, tableId, "orders"));

    assertThrows(
        BaseResourceRepository.BatchGuardFailedException.class,
        () -> repository.deleteSnapshotPointers(tableId, repository.claimedGuard(claimed)));
    assertThat(pointers.get(snapshotKey)).isPresent();
    assertThat(pointers.get(claimed.pointerKey())).isPresent();
  }

  @Test
  void snapshotCleanupBatchesRowsWithinTransactionCapacity() {
    var transactions = new AtomicInteger();
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            transactions.incrementAndGet();
            return super.compareAndSetBatch(ops);
          }
        };
    var repository = new TableCleanupRepository(pointers);
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    for (long snapshotId = 1; snapshotId <= 150; snapshotId++) {
      String key = Keys.snapshotPointerById("acct", "table", snapshotId);
      pointers.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, "snapshot", 1L));
    }

    assertThat(repository.deleteSnapshotPointers(tableId, BatchGuard.NONE)).isEqualTo(150);
    assertThat(transactions).hasValue(2);
  }
}
