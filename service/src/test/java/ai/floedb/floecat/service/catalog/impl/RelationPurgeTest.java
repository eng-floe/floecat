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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.TableWriteFence;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class RelationPurgeTest {

  @Test
  void purgeTableStateDeletesEveryTableScopedPointerFamily() {
    String accountId = "account/id";
    String tableId = "123e4567-e89b-12d3-a456-426614174000";
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(tableId)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var pointers = new InMemoryPointerStore();
    var hints = mock(HintCache.class);
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.hints = hints;

    List<String> tableScoped =
        List.of(
            Keys.snapshotPointerById(accountId, tableId, 7L),
            Keys.snapshotTargetStatsManifestPointer(accountId, tableId, 7L),
            Keys.snapshotIndexArtifactActiveGenerationPointer(accountId, tableId, 7L),
            Keys.snapshotConstraintsPointer(accountId, tableId, 7L),
            Keys.reconcileFinalizedSnapshotIdentityPointer(accountId, tableId, 7L),
            Keys.tableOwnerReuseLeasePointer(accountId, tableId, "publication"),
            Keys.tableOwnerPublicationProgressPointer(accountId, tableId, "publication"),
            Keys.tableRootByTable(accountId, tableId),
            Keys.rootResyncPendingPointer(accountId, tableId),
            Keys.relationHintsPointer(accountId, tableId, "engine", "1"));
    tableScoped.forEach(key -> put(pointers, key));
    String tableRow = Keys.tablePointerById(accountId, tableId);
    String otherTable = Keys.snapshotPointerById(accountId, "other", 7L);
    String marker = Keys.tablePurgePointer(accountId, tableId);
    put(pointers, tableRow);
    put(pointers, otherTable);
    put(pointers, marker);

    completePurge(purge, id);

    tableScoped.forEach(key -> assertTrue(pointers.get(key).isEmpty(), key));
    assertTrue(pointers.get(marker).isEmpty(), "purge marker should be acknowledged");
    assertTrue(pointers.get(tableRow).isPresent());
    assertTrue(pointers.get(otherTable).isPresent());
    verify(hints).evictRelation(id);
  }

  @Test
  void purgeTableStateDeletesAtMostItsBatchBudgetPerCall() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    put(pointers, marker);
    putSnapshots(pointers, 100);

    assertFalse(purge.purgeTableState(tableId(), 1));

    assertEquals(1L, remainingSnapshots(pointers, 100));
    assertTrue(pointers.get(marker).isPresent(), "bounded work must leave recovery queued");
  }

  @Test
  void inlinePurgeFinishesATableWithinItsBatchBudget() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    put(pointers, marker);
    putSnapshots(pointers, 150);

    assertTrue(purge.purgeTableState(tableId()));

    assertEquals(0L, remainingSnapshots(pointers, 150));
    assertTrue(pointers.get(marker).isEmpty());
  }

  @Test
  void inlinePurgeLeavesATableBeyondItsBatchBudgetQueued() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    put(pointers, marker);
    int snapshots = RelationPurge.DELETE_BATCH * RelationPurge.INLINE_BATCH_BUDGET + 1;
    putSnapshots(pointers, snapshots);

    assertFalse(purge.purgeTableState(tableId()));

    assertEquals(1L, remainingSnapshots(pointers, snapshots));
    assertTrue(pointers.get(marker).isPresent(), "bounded work must leave recovery queued");
  }

  @Test
  void purgeFinishesWhenItsLastBudgetedBatchEmptiesTheTable() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    put(pointers, marker);
    putSnapshots(pointers, RelationPurge.DELETE_BATCH);

    assertTrue(purge.purgeTableState(tableId(), 1));

    assertEquals(0L, remainingSnapshots(pointers, RelationPurge.DELETE_BATCH));
    assertTrue(pointers.get(marker).isEmpty());
  }

  @Test
  void rootResyncDeleteCountsAgainstTheBatchBudget() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    String resync = Keys.rootResyncPendingPointer(ACCOUNT, TABLE);
    put(pointers, marker);
    put(pointers, resync);
    putSnapshots(pointers, 1);

    assertFalse(purge.purgeTableState(tableId(), 1));
    assertEquals(0L, remainingSnapshots(pointers, 1));
    assertTrue(pointers.get(resync).isPresent(), "a spent budget must not delete the resync");
    assertTrue(pointers.get(marker).isPresent());

    assertTrue(purge.purgeTableState(tableId(), 1));
    assertTrue(pointers.get(resync).isEmpty());
    assertTrue(pointers.get(marker).isEmpty());
  }

  private static void putSnapshots(InMemoryPointerStore pointers, int count) {
    for (int snapshot = 0; snapshot < count; snapshot++) {
      put(pointers, Keys.snapshotPointerById(ACCOUNT, TABLE, snapshot));
    }
  }

  private static long remainingSnapshots(InMemoryPointerStore pointers, int count) {
    return java.util.stream.IntStream.range(0, count)
        .mapToObj(snapshot -> Keys.snapshotPointerById(ACCOUNT, TABLE, snapshot))
        .filter(key -> pointers.get(key).isPresent())
        .count();
  }

  @Test
  void purgeTableStateWithoutAMarkerOwesNothing() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    String state = Keys.snapshotPointerById(ACCOUNT, TABLE, 7L);
    String resync = Keys.rootResyncPendingPointer(ACCOUNT, TABLE);
    put(pointers, state);
    put(pointers, resync);

    assertFalse(purge.purgeTableState(tableId()));

    assertTrue(pointers.get(state).isPresent());
    assertTrue(pointers.get(resync).isPresent());
    verifyNoInteractions(purge.hints);
  }

  @Test
  void purgeViewStateDeletesHintsAndAcknowledgesItsMarker() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("view")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    String hint = Keys.relationHintsPointer("account", "view", "engine", "1");
    String otherHint = Keys.relationHintsPointer("account", "other", "engine", "1");
    String marker = Keys.viewPurgePointer("account", "view");
    put(pointers, hint);
    put(pointers, otherHint);
    put(pointers, marker);

    assertTrue(purge.purgeViewState(view));

    assertTrue(pointers.get(hint).isEmpty());
    assertTrue(pointers.get(marker).isEmpty(), "purge marker should be acknowledged");
    assertTrue(pointers.get(otherHint).isPresent());
    verify(purge.hints).evictRelation(view);
  }

  @Test
  void purgeViewStateLeavesARecreatedViewsHintsAlone() {
    var pointers = new InMemoryPointerStore();
    var purge = purge(pointers);
    ResourceId view =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("view")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    String hint = Keys.relationHintsPointer("account", "view", "engine", "1");
    put(pointers, Keys.viewPointerById("account", "view"));
    put(pointers, hint);

    assertFalse(purge.purgeViewState(view), "no marker and a live row: nothing is owed");

    assertTrue(pointers.get(hint).isPresent());
  }

  @Test
  void purgeTableStateKeepsItsMarkerWhenCleanupFails() {
    String accountId = "account";
    String tableId = "123e4567-e89b-12d3-a456-426614174000";
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(tableId)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var pointers = new InMemoryPointerStore();
    var hints = mock(HintCache.class);
    doThrow(new IllegalStateException("storage unavailable")).when(hints).evictRelation(id);
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.hints = hints;
    String marker = Keys.tablePurgePointer(accountId, tableId);
    put(pointers, marker);

    assertThrows(IllegalStateException.class, () -> purge.purgeTableState(id));

    assertTrue(pointers.get(marker).isPresent(), "a failed purge must stay queued");
  }

  @Test
  void purgeTableStateRejectsReservedIndexSegmentBeforeDeletingAnything() {
    String accountId = "account";
    ResourceId crafted =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("by-id")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var pointers = new InMemoryPointerStore();
    var hints = mock(HintCache.class);
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.hints = hints;
    String liveTableRow = Keys.tablePointerById(accountId, "live-table");
    put(pointers, liveTableRow);

    assertThrows(IllegalArgumentException.class, () -> purge.purgeTableState(crafted));

    assertTrue(pointers.get(liveTableRow).isPresent());
    verifyNoInteractions(hints);
  }

  /**
   * A purge that stalls after reading its marker can never meet a table recreated under the same
   * id: the id is retired by the delete, so the create is refused even after another purge has
   * finished and acknowledged the marker.
   */
  @Test
  void stalledPurgeCannotMeetARecreatedTable() {
    var pointers = new StallingPointerStore();
    String row = Keys.tablePointerById(ACCOUNT, TABLE);
    String state = Keys.snapshotPointerById(ACCOUNT, TABLE, 7L);
    put(pointers, row);
    put(pointers, state);
    deleteRow(pointers, row);
    var stalled = purge(pointers);
    var recovery = purge(pointers);
    pointers.stallOnce(
        () -> {
          completePurge(recovery, tableId());
          assertFalse(write(pointers, row, 0L), "a dropped table id must never be recreated");
        });

    assertFalse(stalled.purgeTableState(tableId()));

    assertTrue(pointers.get(row).isEmpty());
    assertTrue(pointers.get(state).isEmpty());
    assertTrue(pointers.get(Keys.tablePurgePointer(ACCOUNT, TABLE)).isEmpty());
  }

  private static final String ACCOUNT = "account";
  private static final String TABLE = "123e4567-e89b-12d3-a456-426614174000";

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT)
        .setId(TABLE)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static RelationPurge purge(PointerStore pointers) {
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.hints = mock(HintCache.class);
    return purge;
  }

  private static void completePurge(RelationPurge purge, ResourceId tableId) {
    for (int pass = 0; pass < 20; pass++) {
      if (purge.purgeTableState(tableId)) {
        return;
      }
    }
    fail("purge did not complete within 20 bounded passes");
  }

  /** Writes through the table fence, as the production store seam does. */
  private static boolean write(PointerStore pointers, String key, long expectedVersion) {
    var fenced =
        TableWriteFence.fence(
            pointers,
            List.of(
                new PointerStore.CasUpsert(
                    key,
                    expectedVersion,
                    Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build())));
    return fenced.writable() && pointers.compareAndSetBatch(fenced.operations());
  }

  private static void deleteRow(PointerStore pointers, String row) {
    long version = pointers.get(row).orElseThrow().getVersion();
    assertTrue(
        pointers.compareAndSetBatch(
            TableWriteFence.fence(pointers, List.of(new PointerStore.CasDelete(row, version)))
                .operations()));
  }

  /** Runs one injected interleaving the first time anything lists, then behaves normally. */
  private static final class StallingPointerStore extends InMemoryPointerStore {
    private Runnable stall;

    void stallOnce(Runnable interleaving) {
      stall = interleaving;
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String token, StringBuilder next) {
      Runnable interleaving = stall;
      stall = null;
      if (interleaving != null) {
        interleaving.run();
      }
      return super.listPointersByPrefix(prefix, limit, token, next);
    }
  }

  private static void put(InMemoryPointerStore pointers, String key) {
    assertTrue(
        pointers.compareAndSet(
            key, 0L, Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build()));
  }
}
