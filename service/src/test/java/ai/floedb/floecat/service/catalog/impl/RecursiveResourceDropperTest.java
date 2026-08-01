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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

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

    // No descendants unless a test stubs some: the subtree walk hands rows to a consumer, so an
    // unstubbed void call hands over nothing. Descendants are enumerated from by-path pointer rows,
    // so an unparseable namespace blob cannot break the walk either.
    //
    // One table under the root. Relations are enumerated through their by-name pointer rows, not
    // their blobs, so a corrupt relation cannot break the listing itself.
    stubTableNamePointers("ns", List.of(TABLE_NAME_POINTER));

    // The root pointer the guarded path pins its relation removals to.
    when(namespaceRepo.metaForSafe(any()))
        .thenReturn(meta("blob://acct/namespaces/ns/v1", ROOT_POINTER_VERSION));
    when(markerStore.namespacePinnedGuard(any(), anyLong())).thenReturn(BatchGuard.NONE);

    // The scanned table, still where the scan found it.
    when(tableRepo.metaForSafe(eq(TABLE_ID))).thenReturn(meta(TABLE_BLOB, TABLE_POINTER_VERSION));
    when(tableRepo.getByBlobUri(eq(TABLE_BLOB))).thenReturn(Optional.of(table));
  }

  /**
   * Feeds {@code rows} to whatever consumer the dropper passes to the streaming enumeration, which
   * is how it reads a namespace's relations: a page at a time, never as a list.
   */
  private void stubTableNamePointers(String namespaceId, List<Pointer> rows) {
    doAnswer(feed(rows))
        .when(tableRepo)
        .forEachNamePointer(eq("acct"), eq("cat"), eq(namespaceId), any());
  }

  /**
   * Feeds {@code refs} to the subtree walk's consumer, in the key order a by-path scan produces:
   * the walk turns that order into deepest-first itself, so a test that hands over parents before
   * children is describing exactly what the store returns.
   */
  private void stubNamespaceRefsUnder(
      List<String> parentPath, List<TopologyGraph.NamespaceRef> refs) {
    doAnswer(
            invocation -> {
              Consumer<TopologyGraph.NamespaceRef> consumer = invocation.getArgument(3);
              refs.forEach(consumer);
              return null;
            })
        .when(namespaceRepo)
        .forEachRefUnder(eq("acct"), eq("cat"), eq(parentPath), any());
  }

  private void stubViewNamePointers(String namespaceId, List<Pointer> rows) {
    doAnswer(feed(rows))
        .when(viewRepo)
        .forEachNamePointer(eq("acct"), eq("cat"), eq(namespaceId), any());
  }

  private static Answer<Void> feed(List<Pointer> rows) {
    return invocation -> {
      Consumer<Pointer> consumer = invocation.getArgument(3);
      rows.forEach(consumer);
      return null;
    };
  }

  /**
   * The emptiness gate asks this before reconciling anything, so the ordinary "namespace holds
   * tables" rejection costs one row scan and one pointer read instead of a read per relation.
   */
  @Test
  void hasResolvableRelationAnswersAtTheFirstLiveRelation() {
    String canonical = Keys.tablePointerById("acct", "tbl");
    when(pointerStore.get(eq(canonical)))
        .thenReturn(Optional.of(Pointer.newBuilder().setKey(canonical).setVersion(1L).build()));

    assertTrue(dropper.hasResolvableRelation(root));
    // Stopped at the first row: the views were never listed, and no second canonical read happened.
    verify(pointerStore).get(eq(canonical));
    verify(viewRepo, never()).forEachNamePointer(anyString(), anyString(), anyString(), any());
  }

  /**
   * A create asks this after a name collision, and it must answer without scanning the namespace: a
   * name held by a live relation collides on every attempt, so a sweep there is pure cost.
   */
  @Test
  void relationNameHeldAnswersFromTheClaimWithoutScanningTheNamespace() {
    String claimKey = Keys.relationPointerByName("acct", "cat", "ns", "orders");
    when(pointerStore.get(eq(claimKey)))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(claimKey)
                    .setVersion(3L)
                    .setResourceId(TABLE_ID)
                    .build()));
    String canonical = Keys.tablePointerById("acct", "tbl");
    when(pointerStore.get(eq(canonical)))
        .thenReturn(Optional.of(Pointer.newBuilder().setKey(canonical).setVersion(1L).build()));

    assertTrue(dropper.relationNameHeld(root, "orders"));

    // The claim named a live table, so neither by-name index was consulted and nothing was listed.
    verify(pointerStore, never()).get(eq(Keys.tablePointerByName("acct", "cat", "ns", "orders")));
    verify(tableRepo, never()).forEachNamePointer(anyString(), anyString(), anyString(), any());
    verify(viewRepo, never()).forEachNamePointer(anyString(), anyString(), anyString(), any());
  }

  /** A claim whose relation is gone is exactly what the sweep exists to release. */
  @Test
  void relationNameHeldIsFalseWhenTheClaimsRelationIsGone() {
    String claimKey = Keys.relationPointerByName("acct", "cat", "ns", "orders");
    when(pointerStore.get(eq(claimKey)))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(claimKey)
                    .setVersion(3L)
                    .setResourceId(TABLE_ID)
                    .build()));
    when(pointerStore.get(eq(Keys.tablePointerById("acct", "tbl")))).thenReturn(Optional.empty());

    assertFalse(dropper.relationNameHeld(root, "orders"));
  }

  /**
   * Releasing a row is a relation-scoped write, so a caller that holds only one kind's grant sweeps
   * only that kind — a CreateTable authorized by table.write alone must not clear view rows.
   */
  @Test
  void reclaimHonoursTheKindsTheCallerMayWrite() {
    when(pointerStore.get(eq(Keys.tablePointerById("acct", "tbl")))).thenReturn(Optional.empty());
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);

    assertEquals(1, dropper.reclaimStrandedRelationNames(root, Set.of(ResourceKind.RK_TABLE)));

    verify(viewRepo, never()).forEachNamePointer(anyString(), anyString(), anyString(), any());
  }

  /** Rows whose relation is gone, or that name nothing at all, are what the sweep is for. */
  @Test
  void hasResolvableRelationIsFalseWhenNothingResolves() {
    when(pointerStore.get(eq(Keys.tablePointerById("acct", "tbl")))).thenReturn(Optional.empty());
    stubViewNamePointers(
        "ns",
        List.of(
            Pointer.newBuilder()
                .setKey(Keys.viewPointerByName("acct", "cat", "ns", "ghost"))
                .setVersion(2L)
                .setDisplayName("ghost")
                .build()));

    assertFalse(dropper.hasResolvableRelation(root));
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

  /**
   * A by-name row that names no relation — no ref, no parseable blob URI — is unusable index state:
   * every removal resolves its relation through the owner this row does not carry, so nothing can
   * ever delete it, while the emptiness gate counts it like any other relation. Skipping it made a
   * recursive delete permanently fatal — destroy the rest of the subtree, report the namespace
   * non-empty, retry, repeat — so the row is released instead, pinned to the version read.
   */
  @Test
  void guardedDropReleasesAByNameRowThatNamesNoRelation() {
    String unresolvableKey = Keys.tablePointerByName("acct", "cat", "ns", "ghost");
    stubTableNamePointers(
        "ns",
        List.of(
            Pointer.newBuilder()
                .setKey(unresolvableKey)
                .setVersion(9L)
                .setDisplayName("ghost")
                .build()));
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    // No table was deleted, because there was none to resolve...
    assertEquals(0, summary.tablesDeleted);
    verify(tableRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    // ...but the row the gate would have counted forever is gone, at the version observed.
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PointerStore.CasOp>> batch = ArgumentCaptor.forClass(List.class);
    verify(pointerStore).compareAndSetBatch(batch.capture());
    assertEquals(
        List.<PointerStore.CasOp>of(new PointerStore.CasDelete(unresolvableKey, 9L)),
        batch.getValue());
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
    stubNamespaceRefsUnder(
        List.of("ns"),
        List.of(new TopologyGraph.NamespaceRef(movedId, "child", CATALOG, List.of("ns", "child"))));
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
   * A drop touches no ancestor at all: not their children markers, which are delete fences only a
   * child publish may move, and therefore not their blobs either.
   *
   * <p>Both halves are pinned here. Resolving an ancestor through its content throws
   * CorruptionException for an unparseable blob — neither retryable nor a precondition failure, so
   * it surfaces as an internal error over a half-finished teardown — and this stubs that throw, so
   * reaching for an ancestor by content fails the test. Bumping one by pointer row instead, which
   * is how that hazard was first fixed, fails the marker assertion.
   */
  @Test
  void guardedDropTouchesNoAncestor() {
    var nested =
        Namespace.newBuilder()
            .setResourceId(ROOT_NS)
            .setCatalogId(CATALOG)
            .addParents("db")
            .setDisplayName("ns")
            .build();
    var ancestorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-db")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    // Content-bearing resolution of the ancestor is what used to run here, and it throws.
    when(namespaceRepo.getByPath(anyString(), anyString(), any()))
        .thenThrow(new BaseResourceRepository.CorruptionException("parse failed", null));
    when(namespaceRepo.refByPath(eq("acct"), eq("cat"), eq(List.of("db"))))
        .thenReturn(
            Optional.of(new TopologyGraph.NamespaceRef(ancestorId, "db", CATALOG, List.of("db"))));
    stubTableNamePointers("ns", List.of());
    // The teardown delete reports whether it actually removed the pointer, and only a real removal
    // is counted.
    when(namespaceRepo.delete(eq(ROOT_NS), any())).thenReturn(true);

    // dropNamespaceTree is the teardown entry point: it deletes the root itself.
    var summary = dropper.dropNamespaceTree(nested);

    assertEquals(1, summary.namespacesDeleted);
    verify(markerStore, never()).bumpNamespaceMarker(eq(ancestorId));
    verify(markerStore, never()).bumpCatalogMarker(any());
  }

  /**
   * A descendant whose canonical pointer is already gone still leaves the by-path row the walk
   * followed, and every immediate-child probe counts that row. Skipping without reclaiming it
   * wedges the drop permanently: the parent reports a child nothing can resolve, let alone delete.
   */
  /**
   * A descendant whose canonical pointer is gone still owns its relations: they are keyed by
   * namespace id, not by path, so they outlive that pointer. Releasing the by-path row without
   * dropping them first strands them where nothing can reach them again — no emptiness gate counts
   * them, and account teardown enumerates namespaces by that very row.
   */
  @Test
  void guardedDropRemovesTheRelationsOfADescendantThatIsAlreadyGoneBeforeReleasingItsRow() {
    var childId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-child")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var childTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-child")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String childTableBlob = "blob://acct/tables/tbl-child/v1";
    stubNamespaceRefsUnder(
        List.of("ns"),
        List.of(new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("ns", "child"))));
    // The namespace is gone...
    when(namespaceRepo.metaForSafe(eq(childId))).thenReturn(meta("", 0L));
    // ...but it still owns a table, reachable only through its namespace id.
    stubTableNamePointers(
        "ns-child",
        List.of(
            Pointer.newBuilder()
                .setKey(Keys.tablePointerByName("acct", "cat", "ns-child", "orders"))
                .setVersion(1L)
                .setBlobUri(childTableBlob)
                .setResourceId(childTableId)
                .setDisplayName("orders")
                .build()));
    when(tableRepo.metaForSafe(eq(childTableId))).thenReturn(meta(childTableBlob, 4L));
    when(tableRepo.getByBlobUri(eq(childTableBlob)))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(childTableId)
                    .setCatalogId(CATALOG)
                    .setNamespaceId(childId)
                    .setDisplayName("orders")
                    .build()));
    when(tableRepo.deleteWithPrecondition(eq(childTableId), eq(4L), any())).thenReturn(true);
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);
    // The by-path row that is the namespace's only remaining handle, and gets released last.
    String childByPath = Keys.namespacePointerByPath("acct", "cat", List.of("ns", "child"));
    when(pointerStore.get(eq(childByPath)))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(childByPath)
                    .setVersion(2L)
                    .setResourceId(childId)
                    .build()));
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    // The orphan-to-be is destroyed, and its owned state with it.
    assertEquals(2, summary.tablesDeleted);
    verify(tableRepo).deleteWithPrecondition(eq(childTableId), eq(4L), any());
    verify(tableRoots).purgeRoot(eq(childTableId));
    // Order matters: the by-path row is the only handle left for finding this namespace, so it must
    // not be released until its relations are gone.
    var inOrder = org.mockito.Mockito.inOrder(tableRepo, pointerStore);
    inOrder.verify(tableRepo).deleteWithPrecondition(eq(childTableId), eq(4L), any());
    inOrder.verify(pointerStore).compareAndSetBatch(any());
  }

  @Test
  void guardedDropReclaimsTheByPathRowOfADescendantThatIsAlreadyGone() {
    var childId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-child")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    stubNamespaceRefsUnder(
        List.of("ns"),
        List.of(new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("ns", "child"))));
    // Canonical pointer gone: a concurrent delete that could only remove that one.
    when(namespaceRepo.metaForSafe(eq(childId))).thenReturn(meta("", 0L));
    String byPathKey = Keys.namespacePointerByPath("acct", "cat", List.of("ns", "child"));
    when(pointerStore.get(eq(byPathKey)))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(byPathKey)
                    .setVersion(2L)
                    .setResourceId(childId)
                    .build()));
    when(pointerStore.compareAndSetBatch(any())).thenReturn(true);
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    // Nothing was deleted — there was no namespace left — but the row it left behind is released.
    assertEquals(0, summary.namespacesDeleted);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PointerStore.CasOp>> batch = ArgumentCaptor.forClass(List.class);
    verify(pointerStore).compareAndSetBatch(batch.capture());
    assertEquals(
        List.<PointerStore.CasOp>of(
            new PointerStore.CasCheckAbsent(Keys.namespacePointerById("acct", "ns-child")),
            new PointerStore.CasDelete(byPathKey, 2L)),
        batch.getValue());
    verify(namespaceRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    // Its children marker goes too. Nothing else ever names this namespace again — the row sits
    // outside every prefix the GC and teardown sweep — so skipping it here leaks it for good.
    verify(markerStore).deleteNamespaceMarker(eq(childId));
  }

  /**
   * The walk finds a descendant by its path, but an in-place rename makes that path stale while
   * leaving the namespace inside the subtree. Probing for children under the scanned path would
   * miss one created under the new name and delete this namespace out from over a live child.
   */
  @Test
  void guardedDropProbesForChildrenUnderTheNamespacesCurrentPathNotTheScannedOne() {
    var childId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns-child")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    // Scanned as "ns.child"...
    stubNamespaceRefsUnder(
        List.of("ns"),
        List.of(new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("ns", "child"))));
    when(namespaceRepo.metaForSafe(eq(childId)))
        .thenReturn(meta("blob://acct/namespaces/ns-child/v3", 5L));
    // ...but it has since been renamed in place to "ns.renamed", still inside the subtree.
    when(namespaceRepo.getByBlobUri(eq("blob://acct/namespaces/ns-child/v3")))
        .thenReturn(
            Optional.of(
                Namespace.newBuilder()
                    .setResourceId(childId)
                    .setCatalogId(CATALOG)
                    .addParents("ns")
                    .setDisplayName("renamed")
                    .build()));
    // A child was created under the new name. Only a probe on the current path can see it.
    when(namespaceRepo.hasChildUnder(eq("acct"), eq("cat"), eq(List.of("ns", "renamed"))))
        .thenReturn(true);
    // Everything else lets the delete through, so the abort below can only come from seeing that
    // child: probing the stale path would find nothing and this would commit.
    when(markerStore.advanceNamespaceMarker(eq(childId), anyLong())).thenReturn(true);
    when(markerStore.namespaceMarkerVersion(eq(childId))).thenReturn(0L, 1L, 1L);
    when(namespaceRepo.deleteWithPrecondition(eq(childId), eq(5L), any())).thenReturn(true);
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(TABLE_POINTER_VERSION), any()))
        .thenReturn(true);

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    verify(namespaceRepo, never()).deleteWithPrecondition(any(), anyLong(), any());
    verify(namespaceRepo, never()).delete(any(), any());
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
    stubNamespaceRefsUnder(
        List.of("ns"),
        List.of(new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("ns", "child"))));
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

  /**
   * Account teardown sweeps a whole catalog in one streamed pass, and the walk must turn the
   * store's parents-first key order into deepest-first: a namespace is deleted only after
   * everything beneath it, and exactly once. Visiting one twice would re-run its scans and cache
   * evictions and would count it twice in the audit record for an irreversible operation.
   */
  @Test
  void teardownDropsEveryNamespaceOnceDeepestFirst() {
    var parentId = namespaceId("ns-parent");
    var childId = namespaceId("ns-child");
    var siblingId = namespaceId("ns-sibling");
    // Key order: a namespace, then what is under it, then the next namespace along.
    stubNamespaceRefsUnder(
        List.of(),
        List.of(
            new TopologyGraph.NamespaceRef(parentId, "parent", CATALOG, List.of("parent")),
            new TopologyGraph.NamespaceRef(childId, "child", CATALOG, List.of("parent", "child")),
            new TopologyGraph.NamespaceRef(siblingId, "sibling", CATALOG, List.of("sibling"))));
    when(namespaceRepo.delete(any(), any())).thenReturn(true);

    var summary = dropper.dropCatalogNamespaces("acct", CATALOG);

    assertEquals(3, summary.namespacesDeleted);
    // The child goes before its parent, and each namespace is deleted once.
    var inOrder = org.mockito.Mockito.inOrder(namespaceRepo);
    inOrder.verify(namespaceRepo).delete(eq(childId), any());
    inOrder.verify(namespaceRepo).delete(eq(parentId), any());
    verify(namespaceRepo).delete(eq(siblingId), any());
  }

  /**
   * A by-path key ends at its last segment with no trailing delimiter, so a sibling whose name
   * extends another's lands <em>between</em> that namespace and its own children: {@code orders} <
   * {@code orders-2024} < {@code orders/archive}, because {@code /} (0x2F) sorts after the {@code
   * -} that {@code Keys.encode} leaves literal. A walk that treats "not a descendant of the open
   * one" as "that one is finished" drops {@code orders} while {@code orders/archive} is still to
   * come.
   *
   * <p>The feed order here is the store's own: the refs are sorted by the very key the scan sorts
   * by, so this cannot drift from what a real prefix scan returns.
   */
  @Test
  void teardownDropsAChildBeforeItsParentEvenWithASiblingSortingBetweenThem() {
    var ordersId = namespaceId("ns-orders");
    var archiveId = namespaceId("ns-archive");
    var siblingId = namespaceId("ns-orders-2024");
    var refs =
        new java.util.ArrayList<>(
            List.of(
                new TopologyGraph.NamespaceRef(ordersId, "orders", CATALOG, List.of("orders")),
                new TopologyGraph.NamespaceRef(
                    archiveId, "archive", CATALOG, List.of("orders", "archive")),
                new TopologyGraph.NamespaceRef(
                    siblingId, "orders-2024", CATALOG, List.of("orders-2024"))));
    refs.sort(
        java.util.Comparator.comparing(
            r -> Keys.namespacePointerByPath("acct", "cat", r.pathSegments())));
    assertEquals(
        List.of(ordersId, siblingId, archiveId),
        refs.stream().map(TopologyGraph.NamespaceRef::id).toList(),
        "the sibling really does sort between the namespace and its child");

    stubNamespaceRefsUnder(List.of(), refs);
    when(namespaceRepo.delete(any(), any())).thenReturn(true);

    var summary = dropper.dropCatalogNamespaces("acct", CATALOG);

    assertEquals(3, summary.namespacesDeleted);
    var inOrder = org.mockito.Mockito.inOrder(namespaceRepo);
    inOrder.verify(namespaceRepo).delete(eq(archiveId), any());
    inOrder.verify(namespaceRepo).delete(eq(ordersId), any());
  }

  private static ResourceId namespaceId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_NAMESPACE)
        .build();
  }

  @Test
  void unguardedDropPurgesOwnedStateEvenWhenTableDeleteDoesNotCommit() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(false);

    // Account teardown: no retryable abort, and owned state is purged unconditionally so a table
    // that lost the CAS is not left orphaned along with its root-resync marker and stats. The
    // counters, unlike the purge, only move for a table this call removed — a retried DeleteAccount
    // re-runs cleanup over tables an earlier pass already took.
    var summary = dropper.dropNamespaceContents(root, false);

    assertEquals(0, summary.tablesDeleted);
    assertEquals(0, summary.snapshotPrefixesDeleted);
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
