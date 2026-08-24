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

package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarkerStoreTest {

  @Test
  void accountFenceBlocksMarkerWrites() {
    var pointers = new InMemoryPointerStore();
    var markers = new MarkerStore();
    markers.pointerStore = pointers;
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    String fence = Keys.accountDeletionMarker("acct");
    pointers.compareAndSet(fence, 0L, PointerReferences.opaqueMarkerPointer(fence, "acct", 1L));

    markers.bumpCatalogMarker(catalogId);

    assertTrue(pointers.get(Keys.catalogChildrenMarker("acct", "catalog")).isEmpty());
    assertThrows(
        BaseResourceRepository.AccountDeletionInProgressException.class,
        () -> markers.advanceCatalogMarker(catalogId, 0L));
  }

  @Test
  void markerContentionStillReturnsFalse() {
    var pointers = new InMemoryPointerStore();
    var markers = new MarkerStore();
    markers.pointerStore = pointers;
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("namespace")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    markers.bumpNamespaceMarker(namespaceId);

    assertFalse(markers.advanceNamespaceMarker(namespaceId, 0L));
  }

  @Test
  void fenceThatWinsTheMarkerBatchIsPropagated() {
    String fence = Keys.accountDeletionMarker("acct");
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            super.compareAndSet(
                fence, 0L, PointerReferences.opaqueMarkerPointer(fence, "acct", 1L));
            return false;
          }
        };
    var markers = new MarkerStore();
    markers.pointerStore = pointers;
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();

    assertThrows(
        BaseResourceRepository.AccountDeletionInProgressException.class,
        () -> markers.advanceCatalogMarker(catalogId, 0L));
  }
}
