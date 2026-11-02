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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.RelationPurge;
import ai.floedb.floecat.service.repo.cache.DurablePointerReads;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RelationPurgeQueueTest {
  private final InMemoryPointerStore pointers = new InMemoryPointerStore();
  private RelationPurgeQueue queue;

  @BeforeEach
  void setUp() {
    queue = new RelationPurgeQueue();
    queue.pointerStore = pointers;
    queue.durablePointers = new DurablePointerReads(pointers);
    queue.relations = mock(RelationPurge.class);
    queue.accounts = mock(OwnedAccounts.class);
  }

  @Test
  void drainsTablesAndViewsBeforeAcknowledgingNamespaceWork() {
    String account = "acct/one";
    String catalog = "catalog/one";
    String namespace = "namespace/one";
    String queueKey = Keys.relationPurgePointer(account, catalog, namespace);
    String tableKey = Keys.tablePointerByName(account, catalog, namespace, "table");
    String viewKey = Keys.viewPointerByName(account, catalog, namespace, "view");
    ResourceId table = id(account, "table-id", ResourceKind.RK_TABLE);
    ResourceId view = id(account, "view-id", ResourceKind.RK_VIEW);
    put(queueKey, PointerReferences.opaqueMarkerPointer(queueKey, queueKey, 1L));
    put(tableKey, PointerReferences.blobPointer(tableKey, "/table.pb", 1L, table, "table"));
    put(viewKey, PointerReferences.blobPointer(viewKey, "/view.pb", 1L, view, "view"));
    when(queue.accounts.owns(account)).thenReturn(true);
    doAnswer(
            invocation -> {
              ResourceId relation = invocation.getArgument(0);
              String key = relation.getKind() == ResourceKind.RK_TABLE ? tableKey : viewKey;
              var pointer = pointers.get(key).orElseThrow();
              pointers.compareAndDelete(key, pointer.getVersion());
              return true;
            })
        .when(queue.relations)
        .deleteStranded(any(), eq(namespace));

    assertEquals(
        new RelationPurgeQueue.Result(1, 1, 2),
        queue.drain(System.currentTimeMillis() + 30_000L, 1));
    assertFalse(pointers.get(queueKey).isPresent());
    assertFalse(pointers.get(tableKey).isPresent());
    assertFalse(pointers.get(viewKey).isPresent());
  }

  @Test
  void parsesEncodedWorkKeySegments() {
    var work =
        RelationPurgeQueue.parse(Keys.relationPurgePointer("acct/one", "catalog two", "ns+three"));

    assertNotNull(work);
    assertEquals("acct/one", work.accountId());
    assertEquals("catalog two", work.catalogId());
    assertEquals("ns+three", work.namespaceId());
  }

  @Test
  void removesOrphanedNamePointerWithoutWaitingForPointerGc() {
    String account = "account";
    String catalog = "catalog";
    String namespace = "namespace";
    String queueKey = Keys.relationPurgePointer(account, catalog, namespace);
    String tableKey = Keys.tablePointerByName(account, catalog, namespace, "orphan");
    ResourceId table = id(account, "table-id", ResourceKind.RK_TABLE);
    put(queueKey, PointerReferences.opaqueMarkerPointer(queueKey, queueKey, 1L));
    put(tableKey, PointerReferences.blobPointer(tableKey, "/table.pb", 1L, table, "table"));
    when(queue.accounts.owns(account)).thenReturn(true);
    when(queue.relations.deleteStranded(table, namespace)).thenReturn(false);

    assertEquals(
        new RelationPurgeQueue.Result(1, 1, 0),
        queue.drain(System.currentTimeMillis() + 30_000L, 10));
    assertFalse(pointers.get(queueKey).isPresent());
    assertFalse(pointers.get(tableKey).isPresent());
  }

  private void put(String key, ai.floedb.floecat.common.rpc.Pointer pointer) {
    org.junit.jupiter.api.Assertions.assertTrue(pointers.compareAndSet(key, 0L, pointer));
  }

  private static ResourceId id(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }
}
