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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.catalog.impl.RelationPurge;
import ai.floedb.floecat.service.repo.cache.DurablePointerReads;
import ai.floedb.floecat.service.repo.cache.IndexedPointerStore;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TablePurgeQueueTest {
  private static final String ACCOUNT = "account";
  private static final String TABLE = "123e4567-e89b-12d3-a456-426614174000";

  @Test
  void drainFinishesAPurgeThatFailedAfterTheRowDelete() throws Exception {
    var durable = new InMemoryPointerStore();
    PointerStore pointers = new IndexedPointerStore(durable, new PlanningPointerIndex(durable));
    var hints = mock(HintCache.class);
    var queue = queue(pointers, hints);
    String row = Keys.tablePointerById(ACCOUNT, TABLE);
    String state = Keys.snapshotPointerById(ACCOUNT, TABLE, 7L);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));
    assertTrue(pointers.compareAndSet(state, 0L, pointer(state)));

    assertTrue(pointers.compareAndDelete(row, 1L));
    assertTrue(pointers.get(marker).isPresent(), "the row delete must queue its purge");
    assertTrue(pointers.get(state).isPresent());

    doThrow(new IllegalStateException("storage unavailable")).when(hints).evictRelation(tableId());
    var failed = queue.drainAccount(ACCOUNT, System.currentTimeMillis() + 10_000L, 10);
    assertEquals(new TablePurgeQueue.Result(1, 0), failed);
    assertTrue(pointers.get(marker).isPresent(), "a failed purge must stay queued");
    assertFalse(pointers.compareAndSet(row, 0L, pointer(row)), "the table id must never be reused");

    reset(hints);
    var completed = queue.drainAccount(ACCOUNT, System.currentTimeMillis() + 10_000L, 10);

    assertEquals(new TablePurgeQueue.Result(1, 1), completed);
    assertTrue(pointers.get(state).isEmpty());
    assertTrue(pointers.get(marker).isEmpty());
    assertFalse(pointers.compareAndSet(row, 0L, pointer(row)), "nor once its purge has completed");
  }

  @Test
  void namespaceWorkCanBeAcknowledgedOnceAStrandedTablesPurgeIsQueued() throws Exception {
    var durable = new UnlistableTableStateStore(Keys.tablePrefix(ACCOUNT, TABLE));
    PointerStore pointers = new IndexedPointerStore(durable, new PlanningPointerIndex(durable));
    var hints = mock(HintCache.class);
    var tables = mock(TableRepository.class);
    var purges = queue(pointers, hints);
    inject(purges.relations, "tables", tables);
    var relations = new RelationPurgeQueue();
    relations.pointerStore = pointers;
    relations.durablePointers = new DurablePointerReads(durable);
    relations.relations = purges.relations;
    relations.accounts = mock(OwnedAccounts.class);
    when(relations.accounts.owns(ACCOUNT)).thenReturn(true);

    String catalog = "catalog";
    String namespace = "namespace";
    String work = Keys.relationPurgePointer(ACCOUNT, catalog, namespace);
    String row = Keys.tablePointerById(ACCOUNT, TABLE);
    String name = Keys.tablePointerByName(ACCOUNT, catalog, namespace, "events");
    String constraints = Keys.snapshotConstraintsPointer(ACCOUNT, TABLE, 7L);
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    assertTrue(
        pointers.compareAndSet(work, 0L, PointerReferences.opaqueMarkerPointer(work, work, 1L)));
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));
    assertTrue(
        pointers.compareAndSet(
            name, 0L, PointerReferences.blobPointer(name, "/table.pb", 1L, tableId(), "events")));
    assertTrue(pointers.compareAndSet(constraints, 0L, pointer(constraints)));
    when(tables.pointerMetaForSafeConsistent(tableId()))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1L).build());
    when(tables.getByIdConsistent(tableId()))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId())
                    .setNamespaceId(ResourceId.newBuilder().setId(namespace))
                    .build()));
    // The repository deletes the row with its name pointer; the store seam adds the marker.
    when(tables.deleteWhilePointersMatch(eq(tableId()), eq(1L), any()))
        .thenAnswer(
            invocation ->
                pointers.compareAndSetBatch(
                    List.of(
                        new PointerStore.CasDelete(row, 1L),
                        new PointerStore.CasDelete(name, 1L))));
    long deadline = System.currentTimeMillis() + 10_000L;

    relations.drain(deadline, 10);
    assertTrue(pointers.get(row).isEmpty());
    assertTrue(pointers.get(constraints).isPresent(), "the inline purge failed");
    relations.drain(deadline, 10);
    assertTrue(pointers.get(work).isEmpty(), "no name pointer is left to discover the table by");
    assertTrue(pointers.get(marker).isPresent(), "the table's own purge marker keeps the work");

    durable.failing = false;
    assertEquals(new TablePurgeQueue.Result(1, 1), purges.drainAccount(ACCOUNT, deadline, 10));
    assertTrue(pointers.get(constraints).isEmpty());
    assertTrue(pointers.get(marker).isEmpty());
  }

  @Test
  void drainFinishesAViewPurgeThatFailedAfterTheRowDelete() throws Exception {
    var durable = new InMemoryPointerStore();
    PointerStore pointers = new IndexedPointerStore(durable, new PlanningPointerIndex(durable));
    var hints = mock(HintCache.class);
    var queue = queue(pointers, hints);
    String view = "223e4567-e89b-12d3-a456-426614174000";
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId(view)
            .setKind(ResourceKind.RK_VIEW)
            .build();
    String row = Keys.viewPointerById(ACCOUNT, view);
    String hint = Keys.relationHintsPointer(ACCOUNT, view, "engine", "1");
    String marker = Keys.viewPurgePointer(ACCOUNT, view);
    assertTrue(pointers.compareAndSet(row, 0L, pointer(row)));
    assertTrue(pointers.compareAndSet(hint, 0L, pointer(hint)));

    assertTrue(pointers.compareAndDelete(row, 1L));
    assertTrue(pointers.get(marker).isPresent(), "the view row delete must queue its purge");

    doThrow(new IllegalStateException("storage unavailable")).when(hints).evictRelation(viewId);
    var failed = queue.drainAccount(ACCOUNT, System.currentTimeMillis() + 10_000L, 10);
    assertEquals(new TablePurgeQueue.Result(1, 0), failed);
    assertTrue(pointers.get(marker).isPresent(), "a failed purge must stay queued");

    reset(hints);
    var completed = queue.drainAccount(ACCOUNT, System.currentTimeMillis() + 10_000L, 10);

    assertEquals(new TablePurgeQueue.Result(1, 1), completed);
    assertTrue(pointers.get(hint).isEmpty());
    assertTrue(pointers.get(marker).isEmpty());
  }

  @Test
  void drainDropsMarkersThatNameNoTable() throws Exception {
    var pointers = new InMemoryPointerStore();
    var queue = queue(pointers, mock(HintCache.class));
    String bogus = Keys.tablePurgePointer(ACCOUNT, "not-a-uuid");
    assertTrue(pointers.compareAndSet(bogus, 0L, pointer(bogus)));

    var result = queue.drainAccount(ACCOUNT, System.currentTimeMillis() + 10_000L, 10);

    assertEquals(new TablePurgeQueue.Result(1, 0), result);
    assertTrue(pointers.get(bogus).isEmpty());
  }

  @Test
  void drainStopsBetweenBoundedPurgeBatchesAtTheDeadline() {
    var pointers = new InMemoryPointerStore();
    String marker = Keys.tablePurgePointer(ACCOUNT, TABLE);
    assertTrue(pointers.compareAndSet(marker, 0L, pointer(marker)));
    var purge = mock(RelationPurge.class);
    AtomicLong clock = new AtomicLong();
    // Each batch makes progress but leaves work behind, and runs past the deadline.
    when(purge.purgeTableState(tableId(), 1))
        .thenAnswer(
            call -> {
              clock.set(10L);
              return false;
            });
    var queue = new TablePurgeQueue();
    queue.pointerStore = pointers;
    queue.relations = purge;
    queue.nowMillis = clock::get;

    var result = queue.drainAccount(ACCOUNT, 5L, 10);

    assertEquals(new TablePurgeQueue.Result(1, 0), result);
    verify(purge).purgeTableState(tableId(), 1);
    assertTrue(pointers.get(marker).isPresent());
  }

  private static TablePurgeQueue queue(PointerStore pointers, HintCache hints) throws Exception {
    var purge = new RelationPurge();
    inject(purge, "pointerStore", pointers);
    inject(purge, "hints", hints);
    var queue = new TablePurgeQueue();
    queue.pointerStore = pointers;
    queue.relations = purge;
    return queue;
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT)
        .setId(TABLE)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static Pointer pointer(String key) {
    return Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build();
  }

  /** Fails every listing of one prefix while {@link #failing}, as an unavailable store would. */
  private static final class UnlistableTableStateStore extends InMemoryPointerStore {
    private final String prefix;
    boolean failing = true;

    UnlistableTableStateStore(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String listed, int limit, String token, StringBuilder next) {
      if (failing && listed.equals(prefix)) {
        throw new IllegalStateException("storage unavailable");
      }
      return super.listPointersByPrefix(listed, limit, token, next);
    }
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
