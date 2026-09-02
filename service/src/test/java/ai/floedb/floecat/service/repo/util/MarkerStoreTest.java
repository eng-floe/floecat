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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the two kinds of condition mean, and where they are not interchangeable.
 *
 * <p>A marker is advanced, which is what excludes a writer racing the check. A row's version is
 * only checked, which is what refuses a write against a row whose delete already finished -- the
 * case a marker cannot catch, because a write arriving afterwards samples the post-delete version
 * and matches it.
 */
class MarkerStoreTest {

  private final InMemoryPointerStore pointers = new InMemoryPointerStore();
  private final MarkerStore markers = new MarkerStore();

  MarkerStoreTest() {
    markers.pointerStore = pointers;
  }

  private static ResourceId catalogId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_CATALOG)
        .build();
  }

  private void seed(String key) {
    assertTrue(
        pointers.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, key, 1L)));
  }

  @Test
  void aCatalogChildSetFenceAdvancesTheMarkerAndOnlyChecksTheRow() {
    var id = catalogId("cat-live");
    String canonical = Keys.catalogPointerById("acct", "cat-live");
    String marker = Keys.catalogChildrenMarker("acct", "cat-live");
    seed(canonical);
    seed(marker);

    var conditions = markers.catalogChildNamespacesFence(id);

    assertEquals(
        1L,
        conditions.markerVersions().get(marker),
        "the children marker is advanced, which is what excludes a racing delete");
    assertEquals(
        1L,
        conditions.requiredVersions().get(canonical),
        "the catalog's own row is only required, so concurrent creates do not contend");
    assertTrue(
        conditions.markerVersions().get(canonical) == null,
        "requiring the row must not advance it");
  }

  /**
   * Absence has to be refused rather than folded into the condition.
   *
   * <p>An absent pointer reads as version zero, and requiring version zero is a requirement that it
   * be ABSENT -- which a write against an already-deleted catalog satisfies. Encoding it that way
   * would turn the condition into a licence for the very write it exists to refuse.
   */
  @Test
  void aFenceOnADeletedCatalogIsRefusedRatherThanRequiringAbsence() {
    var id = catalogId("cat-gone");

    var thrown =
        assertThrows(
            BaseResourceRepository.NotFoundException.class,
            () -> markers.catalogChildNamespacesFence(id));
    assertTrue(thrown.getMessage().contains("cat-gone"), "names the catalog that is gone");
  }

  @Test
  void bothOfACatalogsChildSetsAreSampledTogether() {
    var id = catalogId("cat-two");
    String children = Keys.catalogChildrenMarker("acct", "cat-two");
    String overlays = Keys.catalogOverlaysMarker("acct", "cat-two");
    seed(children);

    var sampled = markers.catalogChildSetMarkers(id);

    assertEquals(
        1L,
        sampled.toDelete().get(children),
        "an existing marker is required at its version and goes with the catalog");
    assertTrue(
        sampled.conditions().requiredAbsent().contains(overlays),
        "a marker never written is required absent, so the writer that creates the first child of"
            + " that kind loses this batch");
  }

  @Test
  void aRelationThatIsNotMovingTakesNoFence() {
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-1")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var conditions = markers.relationMoveFence(namespaceId, namespaceId, false);

    assertTrue(
        conditions.markerVersions().isEmpty() && conditions.requiredVersions().isEmpty(),
        "an update that leaves a relation where it is changes no namespace's relation set, and"
            + " asserting anyway would serialize every ordinary update");
  }

  /**
   * A relation that changes catalog without changing namespace is still moving.
   *
   * <p>The marker is keyed by namespace, but the emptiness check it guards counts by catalog AND
   * namespace, so this update moves the relation from one count to another. Reading it as a no-op
   * asserted nothing on a write that re-keys the relation.
   */
  @Test
  void aRelationChangingOnlyItsCatalogStillTakesTheFence() {
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-1")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    seed(Keys.namespacePointerById("acct", "ns-1"));

    var conditions = markers.relationMoveFence(namespaceId, namespaceId, true);

    assertEquals(
        java.util.Set.of(Keys.namespaceRelationsMarker("acct", "ns-1")),
        conditions.markerVersions().keySet(),
        "one namespace on both sides, so one marker -- the same one a create would assert");
    assertTrue(
        conditions.requiredVersions().containsKey(Keys.namespacePointerById("acct", "ns-1")),
        "and its row, refusing a delete that already finished");
  }

  /** A move fences only the destination: removing a relation cannot orphan it in the source. */
  @Test
  void aMoveBetweenAccountsThatShareALocalIdStillTakesTheFence() {
    var from =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-same")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var to = from.toBuilder().setAccountId("other-acct").build();
    seed(Keys.namespacePointerById("other-acct", "ns-same"));

    var conditions = markers.relationMoveFence(from, to, false);

    assertEquals(
        java.util.Set.of(Keys.namespaceRelationsMarker("other-acct", "ns-same")),
        conditions.markerVersions().keySet(),
        "only joining the destination can race its deletion; leaving the source makes it emptier");
    assertEquals(
        2,
        conditions.toCasOps().size(),
        "a destination join is exactly one marker advance plus one namespace existence check");
    assertTrue(
        conditions
            .requiredVersions()
            .containsKey(Keys.namespacePointerById("other-acct", "ns-same")),
        "and the destination's own row, refusing a delete that already finished");
  }

  /**
   * The create fence joins BOTH child sets: the catalog's, and the parent namespace's.
   *
   * <p>Both, because a namespace has two containers whose shape it changes. This is the composition
   * every namespace writer now shares -- requests, the overlay reconciler, and bootstrap seeding --
   * so it is asserted directly rather than only through those callers.
   */
  @Test
  void aNestedCreateJoinsTheCatalogsChildSetAndItsParents() {
    var catalog = catalogId("cat-nested");
    var pointers2 = pointers;
    var namespaces = new NamespaceRepository(pointers2, new InMemoryBlobStore());
    seed(Keys.catalogPointerById("acct", "cat-nested"));

    var parentRid =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-parent")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    namespaces.create(
        Namespace.newBuilder()
            .setResourceId(parentRid)
            .setDisplayName("a")
            .setCatalogId(catalog)
            .build());

    var fence = namespaces.createFence(markers, catalog, List.of("a"));

    assertTrue(
        fence.markerVersions().containsKey(Keys.catalogChildrenMarker("acct", "cat-nested")),
        "the catalog's child set is joined, which is what a concurrent DeleteCatalog loses to");
    assertTrue(
        fence.markerVersions().containsKey(Keys.namespaceChildrenMarker("acct", "ns-parent")),
        "and the parent's, which is what a concurrent delete or rename of the parent loses to");
    assertTrue(
        fence.requiredVersions().containsKey(Keys.namespacePointerById("acct", "ns-parent")),
        "the parent's own row is a read dependency, refusing a delete that already finished");
  }

  /** A top-level create has no parent to join, so the catalog's child set is all there is. */
  @Test
  void aTopLevelCreateJoinsOnlyTheCatalogsChildSet() {
    var catalog = catalogId("cat-root");
    var namespaces = new NamespaceRepository(pointers, new InMemoryBlobStore());
    seed(Keys.catalogPointerById("acct", "cat-root"));

    var fence = namespaces.createFence(markers, catalog, List.of());

    assertEquals(
        java.util.Set.of(Keys.catalogChildrenMarker("acct", "cat-root")),
        fence.markerVersions().keySet(),
        "no parent marker, because there is no parent row to have one");
  }

  /** A parent path with no row cannot be joined, and says so rather than fencing on nothing. */
  @Test
  void aCreateUnderAnUnmaterialisedParentIsRefused() {
    var catalog = catalogId("cat-sparse");
    var namespaces = new NamespaceRepository(pointers, new InMemoryBlobStore());
    seed(Keys.catalogPointerById("acct", "cat-sparse"));

    assertThrows(
        BaseResourceRepository.NotFoundException.class,
        () -> namespaces.createFence(markers, catalog, List.of("missing")),
        "fencing on an absent parent would assert nothing while claiming to join its child set");
  }
}
