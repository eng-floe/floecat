/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TableWriteFenceTest {

  @Test
  void tableScopedWriteRequiresTheTableRow() {
    var pointers = new InMemoryPointerStore();
    String artifact = Keys.snapshotPointerById("account", "table", 7L);

    var fenced =
        TableWriteFence.fence(
            pointers, List.of(new PointerStore.CasUpsert(artifact, 0L, pointer(artifact))));

    assertFalse(fenced.writable());
  }

  @Test
  void tableScopedWriteChecksTheObservedTableVersion() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.tablePointerById("account", "table");
    String artifact = Keys.snapshotPointerById("account", "table", 7L);
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));

    var fenced =
        TableWriteFence.fence(
            pointers, List.of(new PointerStore.CasUpsert(artifact, 0L, pointer(artifact))));

    assertTrue(fenced.writable());
    assertEquals(2, fenced.operations().size());
    assertTrue(fenced.operations().getFirst() instanceof PointerStore.CasCheck);
    assertTrue(pointers.compareAndSetBatch(fenced.operations()));
  }

  @Test
  void deletionClassificationUsesStrongTableRowReads() {
    var pointers = new EventuallyConsistentGetStore();
    String row = Keys.tablePointerById("account", "table");
    String artifact = Keys.snapshotPointerById("account", "table", 7L);
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));
    List<PointerStore.CasOp> operation =
        List.of(new PointerStore.CasUpsert(artifact, 0L, pointer(artifact)));

    var fenced = TableWriteFence.fence(pointers, operation);

    assertTrue(fenced.writable());
    TableWriteFence.requireTableRowsPresent(pointers, operation);
  }

  @Test
  void tableCreationCanPublishTableScopedStateInTheSameBatch() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.tablePointerById("account", "table");
    String artifact = Keys.snapshotPointerById("account", "table", 7L);
    List<PointerStore.CasOp> operations =
        List.of(
            new PointerStore.CasUpsert(row, 0L, pointer(row)),
            new PointerStore.CasUpsert(artifact, 0L, pointer(artifact)));

    var fenced = TableWriteFence.fence(pointers, operations);

    assertTrue(fenced.writable());
    assertEquals(
        List.of(
            operations.get(0),
            operations.get(1),
            new PointerStore.CasCheckAbsent(Keys.tableRetiredPointer("account", "table"))),
        fenced.operations());
    assertTrue(pointers.compareAndSetBatch(fenced.operations()));
  }

  @Test
  void addedOperationCountMatchesWhatTheFenceAdds() {
    var pointers = new InMemoryPointerStore();
    String rowA = Keys.tablePointerById("account", "a");
    String rowB = Keys.tablePointerById("account", "b");
    String rowC = Keys.tablePointerById("account", "c");
    assertTrue(pointers.compareAndSet(rowA, 0L, pointer(rowA)));
    assertTrue(pointers.compareAndSet(rowB, 0L, pointer(rowB)));
    // Two writes below table a share one row check; deleting b adds its purge and retired markers;
    // creating c adds the retired-marker absence check and fences its own state.
    String snapshotA1 = Keys.snapshotPointerById("account", "a", 1L);
    String snapshotA2 = Keys.snapshotPointerById("account", "a", 2L);
    String snapshotC = Keys.snapshotPointerById("account", "c", 1L);
    List<PointerStore.CasOp> operations =
        List.of(
            new PointerStore.CasUpsert(snapshotA1, 0L, pointer(snapshotA1)),
            new PointerStore.CasUpsert(snapshotA2, 0L, pointer(snapshotA2)),
            new PointerStore.CasDelete(rowB, 1L),
            new PointerStore.CasUpsert(rowC, 0L, pointer(rowC)),
            new PointerStore.CasUpsert(snapshotC, 0L, pointer(snapshotC)));

    var fenced = TableWriteFence.fence(pointers, operations);

    assertTrue(fenced.writable());
    assertEquals(4, TableWriteFence.addedOperationCount(operations));
    assertEquals(
        operations.size() + TableWriteFence.addedOperationCount(operations),
        fenced.operations().size());
    assertEquals(0, TableWriteFence.addedOperationCount(fenced.operations()));
  }

  @Test
  void atomicChunkEndLeavesRoomForTableRowChecksAndReservedOperations() {
    List<String> oneTable = new ArrayList<>();
    for (long i = 0; i < 150; i++) {
      oneTable.add(Keys.snapshotPointerById("account", "a", i));
    }
    // 98 writes + 1 table-row check + 1 reserved = 100.
    assertEquals(98, TableWriteFence.atomicChunkEnd(oneTable, 0, key -> key, 1));
    assertEquals(150, TableWriteFence.atomicChunkEnd(oneTable, 98, key -> key, 1));

    List<String> twoTables =
        List.of(
            Keys.snapshotPointerById("account", "a", 1L),
            Keys.snapshotPointerById("account", "b", 1L));
    assertEquals(
        1,
        TableWriteFence.atomicChunkEnd(
            twoTables, 0, key -> key, PointerStore.MAX_ATOMIC_BATCH_OPERATIONS - 3));

    List<String> untracked = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      untracked.add("/accounts/account/other/" + i);
    }
    assertEquals(99, TableWriteFence.atomicChunkEnd(untracked, 0, key -> key, 1));
  }

  @Test
  void tableRowDeleteRecordsItsPurgeAndRetiredMarkers() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.tablePointerById("account", "table");
    String marker = Keys.tablePurgePointer("account", "table");
    String retired = Keys.tableRetiredPointer("account", "table");
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));

    var fenced = TableWriteFence.fence(pointers, List.of(new PointerStore.CasDelete(row, 1L)));

    assertTrue(fenced.writable());
    assertTrue(pointers.compareAndSetBatch(fenced.operations()));
    assertTrue(pointers.get(row).isEmpty());
    assertTrue(pointers.get(marker).isPresent());
    assertTrue(pointers.get(retired).isPresent());
  }

  /**
   * A table id is never reused, even after its purge completes: a writer that captured the id
   * before the drop must never land in a later table.
   */
  @Test
  void tableIdIsNeverReusedOnceItsRowIsDeleted() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.tablePointerById("account", "table");
    String marker = Keys.tablePurgePointer("account", "table");
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));
    assertTrue(
        pointers.compareAndSetBatch(
            TableWriteFence.fence(pointers, List.of(new PointerStore.CasDelete(row, 1L)))
                .operations()));

    var recreate =
        TableWriteFence.fence(pointers, List.of(new PointerStore.CasUpsert(row, 0L, pointer(row))));

    assertTrue(recreate.writable());
    assertFalse(pointers.compareAndSetBatch(recreate.operations()));
    assertTrue(pointers.get(row).isEmpty());

    assertTrue(pointers.delete(marker), "the purge completes");
    assertFalse(pointers.compareAndSetBatch(recreate.operations()));
    assertTrue(pointers.get(row).isEmpty());
  }

  @Test
  void viewRowDeleteRecordsItsPurgeMarkerAndBlocksRecreation() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.viewPointerById("account", "view");
    String marker = Keys.viewPurgePointer("account", "view");
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));

    List<PointerStore.CasOp> delete = List.of(new PointerStore.CasDelete(row, 1L));
    assertEquals(1, TableWriteFence.addedOperationCount(delete));
    assertTrue(pointers.compareAndSetBatch(TableWriteFence.fence(pointers, delete).operations()));
    assertTrue(pointers.get(row).isEmpty());
    assertTrue(pointers.get(marker).isPresent());

    var recreate =
        TableWriteFence.fence(pointers, List.of(new PointerStore.CasUpsert(row, 0L, pointer(row))));
    assertFalse(pointers.compareAndSetBatch(recreate.operations()));
    assertTrue(pointers.delete(marker));
    assertTrue(pointers.compareAndSetBatch(recreate.operations()));
  }

  @Test
  void onlyUserViewRowsOwnAPurgeMarker() {
    assertTrue(TableWriteFence.ownsPurgeMarker(Keys.viewPointerById("account", "view")));
    assertTrue(TableWriteFence.ownsPurgeMarker(Keys.tablePointerById("account", "table")));
    assertFalse(
        TableWriteFence.ownsPurgeMarker(
            Keys.viewPointerByName("account", "catalog", "namespace", "view")));
    assertFalse(TableWriteFence.ownsPurgeMarker(Keys.viewPurgePointer("account", "view")));
    assertFalse(TableWriteFence.ownsPurgeMarker(Keys.viewPointerById("_system", "view")));
    assertFalse(
        TableWriteFence.requiresFence(
            List.of(
                new PointerStore.CasUpsert(
                    Keys.viewPointerById("account", "view"),
                    3L,
                    pointer(Keys.viewPointerById("account", "view"))))));
  }

  @Test
  void tableRowUpdateDoesNotConsultTheLifecycleMarkers() {
    String row = Keys.tablePointerById("account", "table");

    assertFalse(
        TableWriteFence.requiresFence(List.of(new PointerStore.CasUpsert(row, 3L, pointer(row)))));
  }

  @Test
  void lifecycleMarkersAreNotTableScopedState() {
    for (String marker :
        List.of(
            Keys.tablePurgePointer("account", "table"),
            Keys.tableRetiredPointer("account", "table"))) {
      assertFalse(TableWriteFence.isTableRow(marker));
      assertTrue(
          TableWriteFence.tableRowsRequiringChecks(
                  List.of(new PointerStore.CasUpsert(marker, 0L, pointer(marker))))
              .isEmpty());
      assertFalse(
          TableWriteFence.requiresFence(
              List.of(new PointerStore.CasUpsert(marker, 0L, pointer(marker)))));
    }
  }

  @Test
  void deletingTableRowCannotAlsoWriteTableScopedState() {
    String row = Keys.tablePointerById("account", "table");
    String artifact = Keys.snapshotPointerById("account", "table", 7L);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableWriteFence.tableRowsRequiringChecks(
                List.of(
                    new PointerStore.CasDelete(row, 1L),
                    new PointerStore.CasUpsert(artifact, 0L, pointer(artifact)))));
  }

  @Test
  void externalTableFamiliesResolveToTheCanonicalTableRow() {
    assertEquals(
        Set.of(Keys.tablePointerById("account", "table")),
        TableWriteFence.tableRowsRequiringChecks(
            List.of(
                new PointerStore.CasUpsert(
                    Keys.reconcileFinalizedSnapshotIdentityPointer("account", "table", 7L),
                    0L,
                    pointer("finalized")),
                new PointerStore.CasUpsert(
                    Keys.rootResyncPendingPointer("account", "table"), 0L, pointer("resync")))));
  }

  @Test
  void systemTableStateDoesNotRequireAPersistedUserTableRow() {
    String stats = Keys.snapshotTargetStatsManifestPointer("_system", "system-table", 7L);

    assertTrue(
        TableWriteFence.tableRowsRequiringChecks(
                List.of(new PointerStore.CasUpsert(stats, 0L, pointer(stats))))
            .isEmpty());
  }

  private static Pointer pointer(String key) {
    return Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build();
  }

  private static final class EventuallyConsistentGetStore extends InMemoryPointerStore {
    @Override
    public synchronized Optional<Pointer> get(String key) {
      return Optional.empty();
    }
  }
}
