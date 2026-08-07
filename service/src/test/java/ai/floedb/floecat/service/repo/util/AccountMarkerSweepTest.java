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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.RepoTestPointerStores;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The children-marker sweep that ends account teardown, and what it must not mistake for a marker.
 *
 * <p>It walks the catalog and namespace roots, and a namespace's by-path row lives under the
 * catalog root — with caller-chosen path segments after {@code by-path/}. So "the key has a markers
 * segment in it" is not a description of a marker row, it is a description of a marker row and of
 * every namespace whose path happens to run through one.
 */
class AccountMarkerSweepTest {

  private static final String ACCOUNT = "acct";
  private static final String CATALOG = "cat";
  private static final String NAMESPACE = "ns";

  @Test
  void sweepsMarkerRowsAndLeavesEveryOtherRowAlone() {
    var pointers = new InMemoryPointerStore();
    var markers = new MarkerStore();
    markers.pointerStore = pointers;

    String catalogMarker = Keys.catalogChildrenMarker(ACCOUNT, CATALOG);
    String namespaceMarker = Keys.namespaceChildrenMarker(ACCOUNT, NAMESPACE);
    put(pointers, catalogMarker);
    put(pointers, namespaceMarker);

    // A namespace whose own path runs through a segment called "markers". Its by-path key contains
    // "/markers/" and sits under the catalog root this sweep walks, so a substring test takes it —
    // and that row is the only handle every later walk has for reaching the namespace.
    String pathThroughMarkers =
        Keys.namespacePointerByPath(ACCOUNT, CATALOG, List.of("sales", "markers", "raw"));
    String ordinaryPath = Keys.namespacePointerByPath(ACCOUNT, CATALOG, List.of("sales", "orders"));
    put(pointers, pathThroughMarkers);
    put(pointers, ordinaryPath);

    assertEquals(2, markers.deleteAccountMarkers(ACCOUNT, BatchGuard.NONE), "the two markers");

    assertFalse(pointers.get(catalogMarker).isPresent(), "the catalog marker is swept");
    assertFalse(pointers.get(namespaceMarker).isPresent(), "the namespace marker is swept");
    assertTrue(
        pointers.get(pathThroughMarkers).isPresent(),
        "a namespace path running through \"markers\" is not a marker row");
    assertTrue(pointers.get(ordinaryPath).isPresent(), "an ordinary by-path row is untouched");
  }

  @Test
  void markerSweepRetriesANonCommittingTransactionConflict() {
    var backing = new InMemoryPointerStore();
    String marker = Keys.namespaceChildrenMarker(ACCOUNT, NAMESPACE);
    put(backing, marker);
    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(backing) {
          private boolean conflicted;

          @Override
          public boolean compareAndSetBatch(java.util.List<CasOp> ops) {
            if (!conflicted) {
              conflicted = true;
              return false;
            }
            return super.compareAndSetBatch(ops);
          }
        };
    var markers = new MarkerStore();
    markers.pointerStore = pointers;

    assertEquals(1, markers.deleteAccountMarkers(ACCOUNT, BatchGuard.NONE));
    assertFalse(backing.get(marker).isPresent());
  }

  private static void put(InMemoryPointerStore pointers, String key) {
    pointers.compareAndSet(key, 0L, Pointer.newBuilder().setKey(key).setVersion(1L).build());
  }
}
