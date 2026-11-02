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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    purge.purgeTableState(id);

    tableScoped.forEach(key -> assertTrue(pointers.get(key).isEmpty(), key));
    assertTrue(pointers.get(marker).isEmpty(), "purge marker should be acknowledged");
    assertTrue(pointers.get(tableRow).isPresent());
    assertTrue(pointers.get(otherTable).isPresent());
    verify(hints).evictRelation(id);
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

  /** A stalled purge must not clear a root resync the recreated table queued for itself. */
  @Test
  void stalledPurgeLeavesARecreatedTablesRootResyncAlone() {
    var pointers = new StallingPointerStore();
    String row = Keys.tablePointerById(ACCOUNT, TABLE);
    String resync = Keys.rootResyncPendingPointer(ACCOUNT, TABLE);
    put(pointers, row);
    deleteRow(pointers, row);
    var stalled = purge(pointers);
    var recovery = purge(pointers);
    pointers.stallOnce(
        () -> {
          assertTrue(recovery.purgeTableState(tableId()));
          assertTrue(write(pointers, row, 0L));
          assertTrue(write(pointers, resync, 0L));
        });

    assertFalse(stalled.purgeTableState(tableId()));

    assertTrue(pointers.get(resync).isPresent(), "the recreated table's resync must survive");
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
   * A purge that stalls after reading its marker must not touch the state of a table recreated
   * under the same id while it was stalled, after another purge finished and acknowledged.
   */
  @Test
  void stalledPurgeLeavesARecreatedTableAlone() {
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
          assertTrue(recovery.purgeTableState(tableId()));
          assertTrue(write(pointers, row, 0L));
          assertTrue(write(pointers, state, 0L));
        });

    assertFalse(stalled.purgeTableState(tableId()));

    assertTrue(pointers.get(row).isPresent());
    assertTrue(pointers.get(state).isPresent(), "the recreated table's state must survive");
  }

  /** Nor may it acknowledge the marker of a later delete of that recreated table. */
  @Test
  void stalledPurgeDoesNotAcknowledgeALaterDeletesMarker() {
    var pointers = new StallingPointerStore();
    String row = Keys.tablePointerById(ACCOUNT, TABLE);
    String state = Keys.snapshotPointerById(ACCOUNT, TABLE, 7L);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    put(pointers, row);
    deleteRow(pointers, row);
    var stalled = purge(pointers);
    var recovery = purge(pointers);
    pointers.stallOnce(
        () -> {
          assertTrue(recovery.purgeTableState(tableId()));
          assertTrue(write(pointers, row, 0L));
          assertTrue(write(pointers, state, 0L));
          deleteRow(pointers, row);
        });

    assertFalse(stalled.purgeTableState(tableId()));

    assertTrue(pointers.get(marker).isPresent(), "the later delete's purge must stay queued");
    assertTrue(purge(pointers).purgeTableState(tableId()));
    assertTrue(pointers.get(state).isEmpty());
    assertTrue(pointers.get(marker).isEmpty());
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
