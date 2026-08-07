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
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
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
  void staleTaskIsDiscardedWhileTableIsLiveAndADeleteTaskRemainsClaimable() {
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
    assertThat(pointers.get(staged.pointerKey())).isEmpty();

    pointers.delete(tableKey);
    staged = repository.prepare(namespaceId, tableId);
    var claimed = repository.claim(staged, BatchGuard.NONE).orElseThrow();
    repository.complete(claimed);

    assertThat(pointers.get(staged.pointerKey())).isEmpty();
  }

  @Test
  void deletePlanPublishesTaskOnlyWhenTableDeleteAndNamespacePinCommit() {
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
    String namespaceKey = Keys.namespacePointerById("acct", "ns");
    String tableKey = Keys.tablePointerById("acct", "table");
    pointers.compareAndSet(
        namespaceKey, 0L, PointerReferences.blobPointer(namespaceKey, "blob://namespace", 1L));
    pointers.compareAndSet(
        tableKey,
        0L,
        PointerReferences.blobPointer(tableKey, "blob://table", 1L, tableId, "orders"));
    BatchGuard namespacePin = pinnedGuard(pointers, namespaceKey, 1L);

    var lostPlan = repository.planDelete(namespaceId, tableId, namespacePin);
    var lostOps = new ArrayList<PointerStore.CasOp>();
    lostOps.add(new PointerStore.CasDelete(tableKey, 2L));
    lostOps.addAll(lostPlan.guard().ops());
    assertThat(pointers.compareAndSetBatch(lostOps)).isFalse();
    assertThat(pointers.get(lostPlan.cleanup().pointerKey())).isEmpty();

    var committedPlan = repository.planDelete(namespaceId, tableId, namespacePin);
    var committedOps = new ArrayList<PointerStore.CasOp>();
    committedOps.add(new PointerStore.CasDelete(tableKey, 1L));
    committedOps.addAll(committedPlan.guard().ops());
    assertThat(pointers.compareAndSetBatch(committedOps)).isTrue();
    assertThat(pointers.get(committedPlan.cleanup().pointerKey())).isPresent();
    assertThat(pointers.get(committedPlan.cleanup().indexKey())).isPresent();

    var recovered = new ArrayList<TableCleanupRepository.Cleanup>();
    repository.forTable(tableId, recovered::add);
    assertThat(recovered).hasSize(1);
    assertThat(recovered.getFirst().namespaceId()).isEqualTo(namespaceId);
  }

  @Test
  void tableLookupUsesTheDirectIndexWithoutScanningNamespaceRows() {
    var scans = new AtomicInteger();
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public List<ai.floedb.floecat.common.rpc.Pointer> listPointersByPrefix(
              String prefix, int limit, String pageToken, StringBuilder nextPageToken) {
            scans.incrementAndGet();
            return super.listPointersByPrefix(prefix, limit, pageToken, nextPageToken);
          }
        };
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
    repository.prepare(namespaceId, tableId);

    var found = new ArrayList<TableCleanupRepository.Cleanup>();
    repository.forTable(tableId, found::add);

    assertThat(found).hasSize(1);
    assertThat(found.getFirst().namespaceId()).isEqualTo(namespaceId);
    assertThat(scans).hasValue(0);
  }

  @Test
  void completingAnOldClaimCannotDeleteASuccessorGeneration() {
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
    var oldClaim =
        repository.claim(repository.prepare(namespaceId, tableId), BatchGuard.NONE).orElseThrow();
    repository.complete(oldClaim);
    var successor =
        repository.claim(repository.prepare(namespaceId, tableId), BatchGuard.NONE).orElseThrow();

    repository.complete(oldClaim);

    assertThat(pointers.get(successor.pointerKey())).isPresent();
    assertThat(pointers.get(successor.indexKey())).isPresent();
    assertThat(repository.pending(successor)).isPresent();
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

  @Test
  void accountResidualSweepRemovesBothCleanupFamiliesAndPreservesNamespaceRows() {
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
    String namespaceKey = Keys.namespacePointerById("acct", "ns");
    pointers.compareAndSet(
        namespaceKey,
        0L,
        PointerReferences.blobPointer(namespaceKey, "blob://namespace", 1L, namespaceId, "ns"));
    var staged = repository.prepare(namespaceId, tableId);

    var otherNamespace = namespaceId.toBuilder().setAccountId("other").build();
    var otherTable = tableId.toBuilder().setAccountId("other").build();
    var otherStaged = repository.prepare(otherNamespace, otherTable);

    var progress = new BaseResourceRepository.GuardedDeleteProgress();
    assertThat(repository.deleteResidualRows("acct", BatchGuard.NONE, progress)).isEqualTo(2);

    assertThat(pointers.get(staged.pointerKey())).isEmpty();
    assertThat(pointers.get(staged.indexKey())).isEmpty();
    assertThat(pointers.get(namespaceKey)).isPresent();
    assertThat(pointers.get(otherStaged.pointerKey())).isPresent();
    assertThat(pointers.get(otherStaged.indexKey())).isPresent();
  }

  @Test
  void accountResidualEnumerationRecoversTaskOnlyAndIndexOnlyTables() {
    var pointers = new InMemoryPointerStore();
    var repository = new TableCleanupRepository(pointers);
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("missing-namespace")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var taskOnlyTable =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("task-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var indexOnlyTable = taskOnlyTable.toBuilder().setId("index-only").build();
    var taskOnly = repository.prepare(namespaceId, taskOnlyTable);
    var indexOnly = repository.prepare(namespaceId, indexOnlyTable);
    pointers.delete(taskOnly.indexKey());
    pointers.delete(indexOnly.pointerKey());
    var recovered = new ArrayList<ResourceId>();

    repository.forEachResidualTableId("acct", recovered::add);

    assertThat(recovered).extracting(ResourceId::getId).contains("task-only", "index-only");
  }

  @Test
  void rootGuardBreakReportsSnapshotRowsDeletedEarlierInTheCleanup() {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String tableKey = Keys.tablePointerById("acct", "table");
    String rootKey = Keys.tableRootByTable("acct", "table");
    var pointers =
        new InMemoryPointerStore() {
          private boolean tableReappeared;

          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            if (!tableReappeared
                && ops.stream()
                    .anyMatch(
                        op ->
                            op instanceof PointerStore.CasDelete delete
                                && delete.key().equals(rootKey))) {
              tableReappeared = true;
              super.compareAndSet(
                  tableKey, 0L, PointerReferences.blobPointer(tableKey, "blob://replacement", 1L));
            }
            return super.compareAndSetBatch(ops);
          }
        };
    var cleanupRepository = new TableCleanupRepository(pointers);
    var roots = new TableRootRepository(pointers, new InMemoryBlobStore());
    String snapshotKey = Keys.snapshotPointerById("acct", "table", 1L);
    pointers.compareAndSet(
        snapshotKey, 0L, PointerReferences.opaqueMarkerPointer(snapshotKey, "snapshot", 1L));
    pointers.compareAndSet(rootKey, 0L, PointerReferences.blobPointer(rootKey, "blob://root", 1L));
    var progress = new BaseResourceRepository.GuardedDeleteProgress();
    BatchGuard tableAbsent = cleanupRepository.tableAbsentGuard(tableId);

    assertThat(cleanupRepository.deleteSnapshotPointers(tableId, tableAbsent, progress))
        .isEqualTo(1);
    assertThrows(
        BaseResourceRepository.BatchGuardFailedAfterWriteException.class,
        () -> roots.purgeRoot(tableId, tableAbsent, progress));
    assertThat(pointers.get(rootKey)).isPresent();
  }

  private static BatchGuard pinnedGuard(InMemoryPointerStore pointers, String key, long version) {
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return List.of(new PointerStore.CasCheck(key, version));
      }

      @Override
      public Outcome reevaluate() {
        return pointers.get(key).filter(pointer -> pointer.getVersion() == version).isPresent()
            ? Outcome.HOLDS
            : Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return key;
      }
    };
  }
}
