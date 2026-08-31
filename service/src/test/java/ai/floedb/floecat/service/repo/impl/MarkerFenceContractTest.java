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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a marker condition means on each write path.
 *
 * <p>A {@code markerVersions} entry is the only fence available for an invariant about rows a write
 * does not itself touch -- "this namespace has no children", "this namespace holds no relations".
 * It compiles to a CAS that requires the marker at the version the caller read and advances it, so
 * the invariant and the write commit together or neither does.
 *
 * <p>Which makes a dropped entry silent and total: the write proceeds, the caller believes it was
 * fenced, and the invariant it was protecting is simply unenforced. Create, update and delete all
 * honour the entry; these assert it on all three, because the type cannot -- a {@code
 * PointerConditions} carrying a marker version is accepted by a write that ignores it.
 */
class MarkerFenceContractTest {

  private InMemoryPointerStore ptr;
  private InMemoryBlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
  }

  private static final String MARKER = Keys.namespaceChildrenMarker("acct", "ns-1");

  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("cat-1")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private Catalog seedCatalog() {
    var cat = Catalog.newBuilder().setResourceId(CATALOG_ID).setDisplayName("alpha").build();
    new CatalogRepository(ptr, blobs).create(cat);
    return cat;
  }

  private static Namespace namespace(String id, String name) {
    return Namespace.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setId(id)
                .setKind(ResourceKind.RK_NAMESPACE)
                .build())
        .setDisplayName(name)
        .setCatalogId(CATALOG_ID)
        .build();
  }

  private static PointerConditions fenceAt(long version) {
    return new PointerConditions(Map.of(), Set.of(), Map.of(MARKER, version));
  }

  /** Someone else advanced the marker after our caller read it. */
  private void bumpMarkerBehindOurBack(long from) {
    assertThat(
            ptr.compareAndSet(
                MARKER, from, PointerReferences.opaqueMarkerPointer(MARKER, MARKER, from + 1L)))
        .isTrue();
  }

  @Test
  void aFencedCreateFailsWhenTheMarkerMoved() {
    var repo = new NamespaceRepository(ptr, blobs);
    seedCatalog();
    bumpMarkerBehindOurBack(0L);

    assertThat(repo.createWhilePointersMatch(namespace("ns-a", "alpha"), fenceAt(0L))).isFalse();
    assertThat(repo.getById(namespace("ns-a", "alpha").getResourceId())).isEmpty();
  }

  @Test
  void aFencedCreateSucceedsAndAdvancesTheMarker() {
    var repo = new NamespaceRepository(ptr, blobs);
    seedCatalog();

    assertThat(repo.createWhilePointersMatch(namespace("ns-b", "beta"), fenceAt(0L))).isTrue();
    assertThat(ptr.get(MARKER).orElseThrow().getVersion()).isEqualTo(1L);
  }

  @Test
  void aFencedUpdateFailsWhenTheMarkerMoved() {
    var repo = new NamespaceRepository(ptr, blobs);
    seedCatalog();
    var created = namespace("ns-c", "gamma");
    repo.create(created);
    bumpMarkerBehindOurBack(0L);

    assertThat(
            repo.updateWhilePointersMatch(
                created.toBuilder().setDescription("changed").build(), 1L, fenceAt(0L)))
        .isEmpty();
  }

  @Test
  void aFencedDeleteFailsWhenTheMarkerMoved() {
    // Without the marker reaching the batch this delete succeeds, and a caller that passed a
    // fence to it is writing unguarded while believing otherwise.
    var repo = new NamespaceRepository(ptr, blobs);
    seedCatalog();
    var created = namespace("ns-d", "delta");
    repo.create(created);
    bumpMarkerBehindOurBack(0L);

    assertThat(repo.deleteWhilePointersMatch(created.getResourceId(), 1L, fenceAt(0L))).isFalse();
    assertThat(repo.getById(created.getResourceId())).isPresent();
  }

  @Test
  void aFencedDeleteSucceedsAndAdvancesTheMarker() {
    var repo = new NamespaceRepository(ptr, blobs);
    seedCatalog();
    var created = namespace("ns-e", "epsilon");
    repo.create(created);

    assertThat(repo.deleteWhilePointersMatch(created.getResourceId(), 1L, fenceAt(0L))).isTrue();
    assertThat(repo.getById(created.getResourceId())).isEmpty();
    assertThat(ptr.get(MARKER).orElseThrow().getVersion()).isEqualTo(1L);
  }
}
