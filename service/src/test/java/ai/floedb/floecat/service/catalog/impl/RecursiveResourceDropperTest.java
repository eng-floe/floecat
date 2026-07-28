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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Recursive-delete safety around resources that change under a stale subtree scan.
 *
 * <p>The scans that drive the dropper produce ids, and deleting by id resolves whatever that id
 * points at <em>now</em>. The guarded path therefore re-reads each scanned resource, confirms it
 * still belongs to the namespace/subtree being dropped, and deletes pinned to the pointer version
 * that proved it — so a reparent that moved a resource out cannot have it destroyed in its new
 * home. The unguarded account-teardown path keeps deleting unconditionally and never raises a
 * retryable abort: the whole account is going away, and a lost CAS there must not leave the table
 * pointer and its root-resync marker as durable orphans.
 */
class RecursiveResourceDropperTest {

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("cat")
          .setKind(ResourceKind.RK_CATALOG)
          .build();
  private static final ResourceId ROOT_NS =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("ns")
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();
  private static final ResourceId OTHER_NS =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("ns-keep")
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final String TABLE_BLOB = "blob://acct/tables/tbl/v1";
  private static final long TABLE_POINTER_VERSION = 7L;
  private static final long ROOT_POINTER_VERSION = 3L;

  /** The by-name index row the drop follows to reach the table; carries id and name, no blob. */
  private static final Pointer TABLE_NAME_POINTER =
      Pointer.newBuilder()
          .setKey(Keys.tablePointerByName("acct", "cat", "ns", "orders"))
          .setVersion(1L)
          .setBlobUri(TABLE_BLOB)
          .setResourceId(TABLE_ID)
          .setDisplayName("orders")
          .build();

  private NamespaceRepository namespaceRepo;
  private TableRepository tableRepo;
  private TableRootRepository tableRoots;
  private ViewRepository viewRepo;
  private StatsRepository statsRepo;
  private PointerStore pointerStore;
  private MarkerStore markerStore;
  private RecursiveResourceDropper dropper;

  private final Namespace root =
      Namespace.newBuilder()
          .setResourceId(ROOT_NS)
          .setCatalogId(CATALOG)
          .setDisplayName("ns")
          .build();
  private final Table table =
      Table.newBuilder()
          .setResourceId(TABLE_ID)
          .setCatalogId(CATALOG)
          .setNamespaceId(ROOT_NS)
          .setDisplayName("orders")
          .build();

  private static MutationMeta meta(String blobUri, long version) {
    return MutationMeta.newBuilder().setBlobUri(blobUri).setPointerVersion(version).build();
  }

  @BeforeEach
  void setUp() {
    namespaceRepo = mock(NamespaceRepository.class);
    tableRepo = mock(TableRepository.class);
    tableRoots = mock(TableRootRepository.class);
    viewRepo = mock(ViewRepository.class);
    statsRepo = mock(StatsRepository.class);
    pointerStore = mock(PointerStore.class);
    markerStore = mock(MarkerStore.class);

    dropper = new RecursiveResourceDropper();
    dropper.namespaceRepo = namespaceRepo;
    dropper.tableRepo = tableRepo;
    dropper.tableRoots = tableRoots;
    dropper.viewRepo = viewRepo;
    dropper.statsRepo = statsRepo;
    dropper.metadataGraph = mock(UserGraph.class);
    dropper.topology = mock(TopologyGraph.class);
    dropper.markerStore = markerStore;
    dropper.pointerStore = pointerStore;

    // No descendants: dropNamespaceContents processes only the root's own relations. Descendants
    // are enumerated from by-path pointer rows, so an unparseable namespace blob cannot break the
    // walk either.
    when(namespaceRepo.listRefsUnder(anyString(), anyString(), any())).thenReturn(List.of());
    // One table under the root. Relations are enumerated through their by-name pointer rows, not
    // their blobs, so a corrupt relation cannot break the listing itself.
    when(tableRepo.listNamePointers(eq("acct"), eq("cat"), eq("ns")))
        .thenReturn(List.of(TABLE_NAME_POINTER));
    when(viewRepo.listNamePointers(anyString(), anyString(), anyString())).thenReturn(List.of());

    // The root pointer the guarded path pins its relation removals to.
    when(namespaceRepo.metaForSafe(any()))
        .thenReturn(meta("blob://acct/namespaces/ns/v1", ROOT_POINTER_VERSION));
    when(markerStore.namespacePinnedGuard(any(), anyLong())).thenReturn(BatchGuard.NONE);

    // The scanned table, still where the scan found it.
    when(tableRepo.metaForSafe(eq(TABLE_ID))).thenReturn(meta(TABLE_BLOB, TABLE_POINTER_VERSION));
    when(tableRepo.getByBlobUri(eq(TABLE_BLOB))).thenReturn(Optional.of(table));
  }

  @Test
  void guardedDropDeletesPinnedToTheVersionThatProvedMembership() {
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.tablesDeleted);
    assertEquals(1, summary.snapshotPrefixesDeleted);
    verify(tableRoots).purgeRoot(eq(TABLE_ID));
    verify(statsRepo).deleteAllStatsForTable(eq(TABLE_ID));
    // Never the unconditional by-id delete, which would resolve the table's CURRENT pointer.
    verify(tableRepo, never()).delete(any());
  }

  @Test
  void guardedDropRefusesToDeleteATableThatMovedToAnotherNamespace() {
    // The scan saw this table under the root, but by the time it is re-read at its own pointer it
    // has been reparented into a namespace outside the subtree. Deleting by id here would destroy
    // it in its new home.
    when(tableRepo.getByBlobUri(eq(TABLE_BLOB)))
        .thenReturn(Optional.of(table.toBuilder().setNamespaceId(OTHER_NS).build()));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    verify(tableRepo, never()).delete(any());
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(pointerStore, never()).deleteByPrefix(anyString());
    verify(tableRoots, never()).purgeRoot(any());
    verify(statsRepo, never()).deleteAllStatsForTable(any());
  }

  @Test
  void guardedDropSkipsATableAlreadyRemovedConcurrently() {
    // A concurrent DeleteTable won and already ran this same cleanup: nothing to delete or purge.
    when(tableRepo.metaForSafe(eq(TABLE_ID))).thenReturn(meta("", 0L));

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(0, summary.tablesDeleted);
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(tableRoots, never()).purgeRoot(any());
  }

  @Test
  void guardedDropAbortsWhenThePinnedTableDeleteDoesNotCommit() {
    // The table changed between the membership check and the delete, so the pinned CAS loses. Its
    // owned state must not be purged — it may still be alive under a namespace that survives.
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(false);

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    verify(pointerStore, never()).deleteByPrefix(anyString());
    verify(tableRoots, never()).purgeRoot(any());
    verify(statsRepo, never()).deleteAllStatsForTable(any());
  }

  @Test
  void guardedDropRefusesToDeleteADescendantNamespaceThatMovedOutOfTheSubtree() {
    var movedId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-child")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    // Scanned as a child of the root ("ns"), so it is in the drop set...
    var scanned =
        Namespace.newBuilder()
            .setResourceId(movedId)
            .setCatalogId(CATALOG)
            .addParents("ns")
            .setDisplayName("child")
            .build();
    when(namespaceRepo.listRefsUnder(eq("acct"), eq("cat"), eq(List.of("ns"))))
        .thenReturn(
            List.of(
                new TopologyGraph.NamespaceRef(movedId, "child", CATALOG, List.of("ns", "child"))));
    when(namespaceRepo.metaForSafe(eq(movedId)))
        .thenReturn(meta("blob://acct/namespaces/ns-child/v2", 5L));
    // ...but re-reading it at its own pointer shows it now hangs off a different root entirely.
    when(namespaceRepo.getByBlobUri(eq("blob://acct/namespaces/ns-child/v2")))
        .thenReturn(Optional.of(scanned.toBuilder().clearParents().addParents("other").build()));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    // Neither the escaped namespace nor anything inside it is touched.
    verify(namespaceRepo, never()).delete(any(), any());
    verify(namespaceRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(tableRoots, never()).purgeRoot(any());
  }

  /**
   * The emptiness gate counts by-name pointers while every removal resolves its relation through
   * the canonical by-id pointer. A by-name pointer whose relation is already gone — what the
   * corrupt-blob delete path leaves behind — is therefore unreachable by the drop and fatal to the
   * gate forever: without reclaiming it, the namespace can never be deleted by any means.
   */
  @Test
  void guardedDropReclaimsAByNamePointerWhoseRelationIsAlreadyGone() {
    // The canonical pointer is gone, so nothing resolves the relation any more...
    when(tableRepo.metaForSafe(eq(TABLE_ID))).thenReturn(meta("", 0L));
    // ...but the by-name row the gate counts is still there.
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PointerStore.CasOp>> batch = ArgumentCaptor.forClass(List.class);
    verify(pointerStore).compareAndSetBatch(batch.capture());
    // The removal asserts the relation is still gone, so a create racing under this name keeps its
    // index intact.
    assertEquals(
        List.<PointerStore.CasOp>of(
            new PointerStore.CasCheckAbsent(Keys.tablePointerById("acct", "tbl")),
            new PointerStore.CasDelete(TABLE_NAME_POINTER.getKey(), 1L)),
        batch.getValue());
    // Nothing was "deleted": there was no relation left, only its leftover index entry.
    assertEquals(0, summary.tablesDeleted);
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(tableRoots, never()).purgeRoot(any());
  }

  @Test
  void guardedDropLeavesAByNamePointerWhoseRelationStillExists() {
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.tablesDeleted);
    verify(pointerStore, never()).compareAndSetBatch(any());
  }

  /**
   * The namespace analogue, and the stakes are higher: descendants are dropped deepest-first, so
   * refusing on an unparseable namespace blob aborts an operation that has already destroyed
   * everything below it, leaving the tree half torn down. Placement came from the by-path row the
   * walk followed, and the removal stays pinned to the canonical version read here.
   */
  @Test
  void guardedDropDeletesADescendantNamespaceWhoseBlobCannotBeParsed() {
    var childId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-child")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    when(namespaceRepo.listRefsUnder(eq("acct"), eq("cat"), eq(List.of("ns"))))
        .thenReturn(
            List.of(
                new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("ns", "child"))));
    when(namespaceRepo.metaForSafe(eq(childId)))
        .thenReturn(meta("blob://acct/namespaces/ns-child/v9", 5L));
    when(namespaceRepo.getByBlobUri(eq("blob://acct/namespaces/ns-child/v9")))
        .thenThrow(new BaseResourceRepository.CorruptionException("parse failed", null));
    // Empty and stable, so the marker protocol lets it go.
    when(markerStore.advanceNamespaceMarker(eq(childId), anyLong())).thenReturn(true);
    when(markerStore.namespaceMarkerVersion(eq(childId))).thenReturn(0L, 1L, 1L);
    when(namespaceRepo.deleteWithPrecondition(eq(childId), eq(5L), any())).thenReturn(true);
    // The root's own table is incidental here; let it drop cleanly.
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.namespacesDeleted);
    // Pinned to the version the by-path walk resolved, so a reparent still loses the CAS.
    verify(namespaceRepo).deleteWithPrecondition(eq(childId), eq(5L), any());
    verify(namespaceRepo, never()).delete(any(), any());
  }

  /**
   * A corrupt relation blob must not wedge the subtree. Clearing damaged state is what a recursive
   * delete is for, so an unparseable table is deleted on the evidence that survives — the by-name
   * row that led here, and the canonical pointer version it is pinned to — rather than aborting the
   * whole operation because the content cannot confirm the namespace it claims.
   */
  @Test
  void guardedDropDeletesATableWhoseBlobCannotBeParsed() {
    when(tableRepo.getByBlobUri(eq(TABLE_BLOB)))
        .thenThrow(new BaseResourceRepository.CorruptionException("parse failed", null));
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);
    // The repository can only drop the canonical pointer for an unparseable resource, so the drop
    // has to release the index rows itself.
    when(tableRepo.metaForSafe(eq(TABLE_ID)))
        .thenReturn(meta(TABLE_BLOB, TABLE_POINTER_VERSION), meta("", 0L));
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.tablesDeleted);
    // Still pinned to the observed version, so a table reparented out of the subtree loses the CAS.
    verify(tableRepo).deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any());
    verify(tableRoots).purgeRoot(eq(TABLE_ID));
    // ...and the by-name row the emptiness gate counts is released.
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PointerStore.CasOp>> batch = ArgumentCaptor.forClass(List.class);
    verify(pointerStore).compareAndSetBatch(batch.capture());
    assertEquals(
        List.<PointerStore.CasOp>of(
            new PointerStore.CasCheckAbsent(Keys.tablePointerById("acct", "tbl")),
            new PointerStore.CasDelete(TABLE_NAME_POINTER.getKey(), 1L)),
        batch.getValue());
  }

  @Test
  void guardedDropStillRefusesATableWhoseBlobIsReadableAndSaysAnotherNamespace() {
    // The corrupt-blob tolerance above must not weaken the moved-out check when the blob CAN be
    // read: a readable table that names another namespace is still refused.
    when(tableRepo.getByBlobUri(eq(TABLE_BLOB)))
        .thenReturn(Optional.of(table.toBuilder().setNamespaceId(OTHER_NS).build()));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(pointerStore, never()).compareAndSetBatch(any());
  }

  @Test
  void unguardedDropPurgesOwnedStateEvenWhenTableDeleteDoesNotCommit() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(false);

    // Account teardown: no retryable abort, and owned state is purged unconditionally so a table
    // that lost the CAS is not left orphaned along with its root-resync marker and stats.
    var summary = dropper.dropNamespaceContents(root, false);

    assertEquals(1, summary.tablesDeleted);
    assertEquals(1, summary.snapshotPrefixesDeleted);
    verify(tableRoots).purgeRoot(eq(TABLE_ID));
    verify(statsRepo).deleteAllStatsForTable(eq(TABLE_ID));
  }

  @Test
  void unguardedDropNeverPinsOrVerifiesMembership() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(true);

    dropper.dropNamespaceContents(root, false);

    // Teardown must not depend on membership: the tree is going away wholesale, and a pinned CAS
    // that lost would strand owned state.
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
  }
}
