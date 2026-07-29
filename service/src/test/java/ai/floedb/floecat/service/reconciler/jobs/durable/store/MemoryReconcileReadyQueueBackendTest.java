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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import org.junit.jupiter.api.Test;

class MemoryReconcileReadyQueueBackendTest {

  @Test
  void globalSliceExcludesSecondaryReadyIndexes() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    String canonical = Keys.reconcileJobPointerById("acct", "job");
    put(
        pointers,
        Keys.reconcileReadyByExecutionClassPointerByDue(1L, "DEFAULT", "acct", "job"),
        canonical);
    MemoryReconcileReadyQueueBackend backend = new MemoryReconcileReadyQueueBackend(pointers);

    var page =
        backend.scanReadySlice(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(
                ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""),
            1,
            "",
            null);

    assertTrue(page.entries().isEmpty());
    assertTrue(page.nextPageToken().isBlank());
  }

  @Test
  void allReadyScanDoesNotReturnSecondaryIndexesAsGlobalEntries() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    String canonical = Keys.reconcileJobPointerById("acct", "job");
    put(pointers, Keys.reconcileReadyPointerByDue(1L, "acct", "lane", "job"), canonical);
    put(
        pointers,
        Keys.reconcileReadyByExecutionClassPointerByDue(1L, "DEFAULT", "acct", "job"),
        canonical);
    MemoryReconcileReadyQueueBackend backend = new MemoryReconcileReadyQueueBackend(pointers);

    var page = backend.scanAllReadyEntries(10, "");

    assertEquals(2, page.entries().size());
    assertEquals(
        1,
        page.entries().stream()
            .filter(entry -> entry.indexType() == ReconcileReadyQueueStore.ReadyIndexType.GLOBAL)
            .count());
  }

  @Test
  void conditionalDeleteRejectsCanonicalRefresh() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    String canonical = Keys.reconcileJobPointerById("acct", "job");
    String ready = Keys.reconcileReadyPointerByDue(1L, "acct", "lane", "job");
    put(pointers, canonical, "blob-v1");
    put(pointers, ready, canonical);
    MemoryReconcileReadyQueueBackend backend = new MemoryReconcileReadyQueueBackend(pointers);
    var entry = backend.scanAllReadyEntries(1, "").entries().getFirst();
    var snapshot = backend.loadCanonicalSnapshot(canonical, null).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonical,
            snapshot.version(),
            Pointer.newBuilder().setKey(canonical).setBlobUri("blob-v2").build()));

    assertFalse(backend.deleteReadyEntry(entry, snapshot));
    assertTrue(pointers.get(ready).isPresent());
  }

  @Test
  void conditionalDeleteRejectsCanonicalCreatedAfterMissingRead() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    String canonical = Keys.reconcileJobPointerById("acct", "job");
    String ready = Keys.reconcileReadyPointerByDue(1L, "acct", "lane", "job");
    put(pointers, ready, canonical);
    MemoryReconcileReadyQueueBackend backend = new MemoryReconcileReadyQueueBackend(pointers);
    var entry = backend.scanAllReadyEntries(1, "").entries().getFirst();
    put(pointers, canonical, "blob-v1");

    assertFalse(backend.deleteReadyEntry(entry, null));
    assertTrue(pointers.get(ready).isPresent());
  }

  @Test
  void allReadyScanDoesNotSkipTheNextRowWhenThePreviousPageIsDeleted() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    String canonicalOne = Keys.reconcileJobPointerById("acct", "job-1");
    String canonicalTwo = Keys.reconcileJobPointerById("acct", "job-2");
    String readyOne = Keys.reconcileReadyPointerByDue(1L, "acct", "lane", "job-1");
    String readyTwo = Keys.reconcileReadyPointerByDue(2L, "acct", "lane", "job-2");
    put(pointers, readyOne, canonicalOne);
    put(pointers, readyTwo, canonicalTwo);
    MemoryReconcileReadyQueueBackend backend = new MemoryReconcileReadyQueueBackend(pointers);

    var first = backend.scanAllReadyEntries(1, "");
    assertEquals(readyOne, first.entries().getFirst().readyPointerKey());
    assertTrue(backend.deleteReadyEntry(first.entries().getFirst(), null));

    var second = backend.scanAllReadyEntries(1, first.nextPageToken());
    assertEquals(readyTwo, second.entries().getFirst().readyPointerKey());
  }

  private static void put(InMemoryPointerStore pointers, String key, String canonical) {
    assertTrue(
        pointers.compareAndSet(
            key,
            0L,
            Pointer.newBuilder().setKey(key).setBlobUri(canonical).setVersion(1L).build()));
  }
}
