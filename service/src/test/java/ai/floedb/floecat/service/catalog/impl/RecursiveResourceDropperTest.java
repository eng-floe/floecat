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
import static org.mockito.ArgumentMatchers.anyInt;
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

    // No descendants: dropNamespaceContents processes only the root's own relations.
    when(namespaceRepo.list(anyString(), anyString(), any(), anyInt(), anyString(), any()))
        .thenReturn(List.of());
    // One table under the root, single page.
    when(tableRepo.list(anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of(table));
    when(viewRepo.list(anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of());

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
    when(namespaceRepo.list(anyString(), anyString(), any(), anyInt(), anyString(), any()))
        .thenReturn(List.of(scanned));
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
    String strandedKey = Keys.tablePointerByName("acct", "cat", "ns", "orders");
    // Nothing resolvable is left to drop: the blob-loading listing sees no table...
    when(tableRepo.list(anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of());
    // ...but the by-name pointer the gate counts is still there.
    when(tableRepo.listNamePointers(eq("acct"), eq("cat"), eq("ns")))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(strandedKey)
                    .setVersion(1L)
                    .setBlobUri(TABLE_BLOB)
                    .setResourceId(TABLE_ID)
                    .setDisplayName("orders")
                    .build()));
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
            new PointerStore.CasDelete(strandedKey, 1L)),
        batch.getValue());
    // Nothing was "deleted": there was no relation left, only its leftover index entry.
    assertEquals(0, summary.tablesDeleted);
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(tableRoots, never()).purgeRoot(any());
  }

  @Test
  void guardedDropLeavesAByNamePointerWhoseRelationStillExists() {
    when(tableRepo.listNamePointers(eq("acct"), eq("cat"), eq("ns")))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(Keys.tablePointerByName("acct", "cat", "ns", "orders"))
                    .setVersion(1L)
                    .setBlobUri(TABLE_BLOB)
                    .setResourceId(TABLE_ID)
                    .setDisplayName("orders")
                    .build()));
    // The relation is alive, so its index entry is not leftover state and must not be touched.
    when(pointerStore.get(eq(Keys.tablePointerById("acct", "tbl"))))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder().setKey("x").setVersion(TABLE_POINTER_VERSION).build()));
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.tablesDeleted);
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
