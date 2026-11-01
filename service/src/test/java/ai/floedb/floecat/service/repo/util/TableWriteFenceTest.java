/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
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
    assertEquals(operations, fenced.operations());
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

  private static Pointer pointer(String key) {
    return Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build();
  }
}
