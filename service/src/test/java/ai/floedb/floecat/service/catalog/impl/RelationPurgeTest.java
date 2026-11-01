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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
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
    var roots = mock(TableRootRepository.class);
    var hints = mock(HintCache.class);
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.tableRoots = roots;
    purge.hints = hints;

    List<String> tableScoped =
        List.of(
            Keys.snapshotPointerById(accountId, tableId, 7L),
            Keys.snapshotTargetStatsManifestPointer(accountId, tableId, 7L),
            Keys.snapshotIndexArtifactActiveGenerationPointer(accountId, tableId, 7L),
            Keys.snapshotConstraintsPointer(accountId, tableId, 7L),
            Keys.tableOwnerReuseLeasePointer(accountId, tableId, "publication"),
            Keys.tableOwnerPublicationProgressPointer(accountId, tableId, "publication"),
            Keys.tableRootByTable(accountId, tableId));
    tableScoped.forEach(key -> put(pointers, key));
    String tableRow = Keys.tablePointerById(accountId, tableId);
    String otherTable = Keys.snapshotPointerById(accountId, "other", 7L);
    put(pointers, tableRow);
    put(pointers, otherTable);

    purge.purgeTableState(id);

    tableScoped.forEach(key -> assertTrue(pointers.get(key).isEmpty(), key));
    assertTrue(pointers.get(tableRow).isPresent());
    assertTrue(pointers.get(otherTable).isPresent());
    verify(roots).purgeRoot(id);
    verify(hints).deleteRelation(id);
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
    var roots = mock(TableRootRepository.class);
    var hints = mock(HintCache.class);
    var purge = new RelationPurge();
    purge.pointerStore = pointers;
    purge.tableRoots = roots;
    purge.hints = hints;
    String liveTableRow = Keys.tablePointerById(accountId, "live-table");
    put(pointers, liveTableRow);

    assertThrows(IllegalArgumentException.class, () -> purge.purgeTableState(crafted));

    assertTrue(pointers.get(liveTableRow).isPresent());
    verifyNoInteractions(roots, hints);
  }

  private static void put(InMemoryPointerStore pointers, String key) {
    assertTrue(
        pointers.compareAndSet(
            key, 0L, Pointer.newBuilder().setKey(key).setBlobUri("blob://value").build()));
  }
}
