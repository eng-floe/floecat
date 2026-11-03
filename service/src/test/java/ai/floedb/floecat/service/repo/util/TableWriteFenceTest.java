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
            new PointerStore.CasCheckAbsent(Keys.tablePurgePointer("account", "table"))),
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
    // Two writes below table a share one row check; deleting b adds its marker; creating c adds
    // the marker-absence check and fences its own state.
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
    assertEquals(3, TableWriteFence.addedOperationCount(operations));
    assertEquals(
        operations.size() + TableWriteFence.addedOperationCount(operations),
        fenced.operations().size());
    assertEquals(0, TableWriteFence.addedOperationCount(fenced.operations()));
  }

  @Test
  void tableRowDeleteRecordsItsPurgeMarker() {
    var pointers = new InMemoryPointerStore();
    String row = Keys.tablePointerById("account", "table");
    String marker = Keys.tablePurgePointer("account", "table");
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));

    var fenced = TableWriteFence.fence(pointers, List.of(new PointerStore.CasDelete(row, 1L)));

    assertTrue(fenced.writable());
    assertTrue(pointers.compareAndSetBatch(fenced.operations()));
    assertTrue(pointers.get(row).isEmpty());
    assertTrue(pointers.get(marker).isPresent());
  }

  @Test
  void tableRowCannotBeRecreatedWhileItsPurgeIsPending() {
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

    assertTrue(pointers.delete(marker));
    assertTrue(pointers.compareAndSetBatch(recreate.operations()));
  }

  @Test
  void tableRowUpdateDoesNotConsultThePurgeMarker() {
    String row = Keys.tablePointerById("account", "table");

    assertFalse(
        TableWriteFence.requiresFence(List.of(new PointerStore.CasUpsert(row, 3L, pointer(row)))));
  }

  @Test
  void purgeMarkerIsNotTableScopedState() {
    String marker = Keys.tablePurgePointer("account", "table");

    assertFalse(TableWriteFence.isTableRow(marker));
    assertTrue(
        TableWriteFence.tableRowsRequiringChecks(
                List.of(new PointerStore.CasUpsert(marker, 0L, pointer(marker))))
            .isEmpty());
    assertFalse(
        TableWriteFence.requiresFence(
            List.of(new PointerStore.CasUpsert(marker, 0L, pointer(marker)))));
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
