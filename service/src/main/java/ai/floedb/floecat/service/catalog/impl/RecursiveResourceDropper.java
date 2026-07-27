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

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

/** Removes the dependent state owned by tables and namespace trees. */
@ApplicationScoped
public class RecursiveResourceDropper {

  // Per-resource audit trail for irreversible teardown (account delete and recursive namespace
  // delete). Each purged namespace, table, view, and snapshot prefix is named so the destruction
  // can be reconstructed after the fact.
  private static final Logger CLEANUP_LOG = Logger.getLogger(RecursiveResourceDropper.class);

  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject TableRootRepository tableRoots;
  @Inject ViewRepository viewRepo;
  @Inject StatsRepository statsRepo;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;
  @Inject PointerStore pointerStore;

  /** Drops all descendants of {@code root}, leaving the root namespace for its caller to delete. */
  public DropSummary dropNamespaceContents(Namespace root) {
    return dropNamespaceContents(root, true);
  }

  /**
   * Drops all descendants of {@code root}, leaving the root namespace for its caller to delete.
   *
   * <p>When {@code guarded} is true, each descendant is verified empty and stable under the
   * namespace-marker protocol before removal, so a concurrently created table or immediate child
   * cannot be orphaned; the caller is expected to have advanced the root marker first. When false —
   * account teardown, where the whole tree (and its account pointer) is going away — descendants
   * are dropped unconditionally so cleanup never raises {@link
   * BaseResourceRepository.AbortRetryableException}, which would otherwise retry the account delete
   * after its pointer is gone and skip cleanup entirely.
   *
   * <p>In both modes, descendant removals never advance the root's own namespace marker (see {@link
   * #deleteNamespace(Namespace, ResourceId)}), so a guarded caller can assert the root marker moved
   * exactly once — its own advance — and still detect a genuine concurrent write to the root's
   * immediate children.
   */
  public DropSummary dropNamespaceContents(Namespace root, boolean guarded) {
    var summary = new DropSummary();
    var rootId = root.getResourceId();
    var rootPath = new ArrayList<>(root.getParentsList());
    rootPath.add(root.getDisplayName());
    // Scan only the root's subtree by its path prefix rather than the whole catalog; the by-path
    // prefix over-returns the root itself, which isDescendant filters out.
    var descendants = new ArrayList<Namespace>();
    drainPages(
        (token, next) ->
            namespaceRepo.list(
                root.getResourceId().getAccountId(),
                root.getCatalogId().getId(),
                rootPath,
                200,
                token,
                next),
        namespace -> {
          if (isDescendant(namespace, rootPath)) {
            descendants.add(namespace);
          }
        });
    descendants.sort(Comparator.comparingInt(Namespace::getParentsCount).reversed());
    for (var descendant : descendants) {
      dropNamespace(descendant, summary, rootId, rootPath, guarded);
    }
    // The root's own relations are pinned to the root the caller resolved, so a concurrent reparent
    // of the root cannot have its contents emptied out from under it either.
    dropNamespaceRelations(root, summary, guarded, subtreePin(rootId, guarded));
    return summary;
  }

  /** Drops a namespace and every object it owns, including descendant namespaces. */
  public DropSummary dropNamespaceTree(Namespace root) {
    // Account teardown: the whole tree is going away, so drop unconditionally rather than under the
    // marker guard. A guarded drop can raise AbortRetryableException, which would retry the account
    // delete after its pointer is already gone and skip cleanup entirely, orphaning resources.
    var summary = dropNamespaceContents(root, false);
    deleteNamespace(root);
    summary.namespacesDeleted++;
    return summary;
  }

  public Optional<DropSummary> dropNamespaceTree(ResourceId namespaceId) {
    return namespaceRepo.getById(namespaceId).map(this::dropNamespaceTree);
  }

  public List<ResourceId> namespaceIds(String accountId, String catalogId) {
    return namespaceRepo.listIds(accountId, catalogId);
  }

  /** Removes a table after its pointer has already been deleted through a public mutation. */
  public void cleanupDeletedTable(ResourceId tableId, ResourceId namespaceId) {
    cleanupDeletedTable(tableId, namespaceId, true);
  }

  private void cleanupDeletedTable(
      ResourceId tableId, ResourceId namespaceId, boolean bumpNamespaceMarker) {
    topology.evict(tableId);
    metadataGraph.invalidate(tableId);
    if (bumpNamespaceMarker && namespaceId != null) {
      markerStore.bumpNamespaceMarker(namespaceId);
    }
    pointerStore.deleteByPrefix(Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId()));
    // Per-snapshot stats live under /snapshots/ (removed by the prefix above); the table-level and
    // per-target "latest committed" stats pointers/blobs live outside it and must be purged too, or
    // they outlive the deleted table as durable orphans.
    statsRepo.deleteAllStatsForTable(tableId);
    tableRoots.purgeRoot(tableId);
    pointerStore.delete(Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId()));
  }

  private void dropNamespace(
      Namespace namespace,
      DropSummary summary,
      ResourceId rootId,
      List<String> rootPath,
      boolean guarded) {
    var namespaceId = namespace.getResourceId();

    if (!guarded) {
      dropNamespaceRelations(namespace, summary, false, BatchGuard.NONE);
      deleteNamespace(namespace, rootId, BatchGuard.NONE, UNPINNED);
      summary.namespacesDeleted++;
      return;
    }

    // Confirm this scanned descendant is STILL inside the subtree before destroying anything in it,
    // and capture the pointer version that proved it. A reparent out of the subtree advances that
    // pointer, so pinning every removal below to this version means a namespace that escaped
    // mid-drop
    // is neither emptied nor deleted.
    long pinnedVersion = pinDescendantToSubtree(namespace, rootPath);
    if (pinnedVersion == UNPINNED) {
      CLEANUP_LOG.infof(
          "recursive_drop_namespace_skipped_absent account_id=%s namespace_id=%s",
          namespaceId.getAccountId(), namespaceId.getId());
      return;
    }
    var subtreePin = markerStore.namespacePinnedGuard(namespaceId, pinnedVersion);

    dropNamespaceRelations(namespace, summary, true, subtreePin);
    var childrenGuard = requireNamespaceEmptyAndStable(namespace);
    deleteNamespace(namespace, rootId, childrenGuard, pinnedVersion);
    summary.namespacesDeleted++;
  }

  /** Sentinel for "no pointer version pinned" — account teardown, or a resource already gone. */
  private static final long UNPINNED = -1L;

  /**
   * Pins the root's own relations to the root pointer the caller resolved, so a concurrent reparent
   * of the root cannot have its contents emptied. Unguarded teardown pins nothing.
   */
  private BatchGuard subtreePin(ResourceId namespaceId, boolean guarded) {
    if (!guarded) {
      return BatchGuard.NONE;
    }
    long version = namespaceRepo.metaForSafe(namespaceId).getPointerVersion();
    if (version == 0L) {
      throw namespaceChanged(namespaceId);
    }
    return markerStore.namespacePinnedGuard(namespaceId, version);
  }

  /**
   * Re-reads a scanned descendant and returns the canonical pointer version that proves it is still
   * under {@code rootPath}, or {@link #UNPINNED} when it no longer exists at all.
   *
   * <p>The version and the content come from a single pointer read, so the pair is coherent: the
   * namespace path checked here is exactly the one that version resolved to. Verifying membership
   * against a separately-read version could otherwise pin a resource that had already moved.
   */
  private long pinDescendantToSubtree(Namespace scanned, List<String> rootPath) {
    var namespaceId = scanned.getResourceId();
    var meta = namespaceRepo.metaForSafe(namespaceId);
    if (meta.getPointerVersion() == 0L) {
      return UNPINNED;
    }
    var live = namespaceRepo.getByBlobUri(meta.getBlobUri());
    if (live.isEmpty()) {
      throw namespaceChanged(namespaceId);
    }
    if (!isDescendant(live.get(), rootPath)) {
      throw movedOutOfSubtree(namespaceId, String.join(".", rootPath));
    }
    return meta.getPointerVersion();
  }

  /**
   * Uses the same marker protocol as ordinary namespace deletion. This prevents a concurrently
   * created table or immediate child namespace from being orphaned by a recursive parent drop.
   *
   * @return the fence to carry into this descendant's delete batch, asserting the marker is still
   *     where these scans left it. The scans alone cannot close the window — they are reads, and a
   *     read cannot join a CAS batch — so the delete itself has to contend on the marker that every
   *     child-publishing write advances (see {@link BatchGuard}).
   */
  private BatchGuard requireNamespaceEmptyAndStable(Namespace namespace) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    var parentPath = new ArrayList<>(namespace.getParentsList());
    parentPath.add(namespace.getDisplayName());
    long markerVersion = markerStore.namespaceMarkerVersion(namespaceId);

    if (hasRelations(namespaceId, catalogId) || hasImmediateChildren(catalogId, parentPath)) {
      throw namespaceChanged(namespaceId);
    }
    if (!markerStore.advanceNamespaceMarker(namespaceId, markerVersion)) {
      throw namespaceChanged(namespaceId);
    }
    if (markerStore.namespaceMarkerVersion(namespaceId) != markerVersion + 1
        || hasRelations(namespaceId, catalogId)
        || hasImmediateChildren(catalogId, parentPath)) {
      throw namespaceChanged(namespaceId);
    }
    return markerStore.namespaceDeleteGuard(namespaceId, markerVersion + 1);
  }

  private boolean hasRelations(ResourceId namespaceId, ResourceId catalogId) {
    return tableRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0
        || viewRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0;
  }

  private boolean hasImmediateChildren(ResourceId catalogId, List<String> parentPath) {
    return anyPage(
        (token, next) ->
            namespaceRepo.list(
                catalogId.getAccountId(), catalogId.getId(), parentPath, 200, token, next),
        child -> isImmediateChildOf(child, parentPath));
  }

  private static boolean isImmediateChildOf(Namespace namespace, List<String> parentPath) {
    return namespace.getParentsCount() == parentPath.size()
        && namespace.getParentsList().equals(parentPath);
  }

  private static BaseResourceRepository.AbortRetryableException namespaceChanged(
      ResourceId namespaceId) {
    return new BaseResourceRepository.AbortRetryableException(
        "namespace children changed during recursive delete: " + namespaceId.getId());
  }

  private static BaseResourceRepository.AbortRetryableException relationChanged(
      ResourceId relationId) {
    return new BaseResourceRepository.AbortRetryableException(
        "relation changed during recursive delete: " + relationId.getId());
  }

  private static BaseResourceRepository.AbortRetryableException movedOutOfSubtree(
      ResourceId resourceId, String from) {
    return new BaseResourceRepository.AbortRetryableException(
        "resource moved out of the subtree during recursive delete: "
            + resourceId.getId()
            + " no longer under "
            + from);
  }

  /**
   * Resolves the pointer version to delete a scanned resource at, but only while it still belongs
   * to the part of the tree being dropped.
   *
   * <p>The subtree scans that feed this dropper produce ids, and deleting by id resolves whatever
   * that id points at <em>now</em>. A concurrent reparent that moved the resource out between the
   * scan and the delete would therefore have it destroyed in its new home, along with its owned
   * state — a silent data loss that no marker can prevent, because a marker can only be checked
   * after the fact.
   *
   * <p>The fix is to make membership and the delete precondition come from the same observation.
   * {@code metaReader} performs a single canonical-pointer read, yielding a coherent {@code
   * (blobUri, pointerVersion)} pair; {@code ownerOfBlob} then reads the content <em>that exact
   * version</em> pointed at and reports which namespace it declared. Deleting pinned to that
   * version means any later mutation — every reparent advances the canonical pointer — loses the
   * CAS instead of destroying a resource that had legitimately moved away.
   *
   * @return the version to delete at, or empty when the resource is already gone (a concurrent
   *     delete won and ran the same cleanup, so there is nothing left to do)
   */
  private <T> Optional<Long> pinToNamespace(
      ResourceId resourceId,
      ResourceId expectedOwner,
      Function<ResourceId, MutationMeta> metaReader,
      Function<String, Optional<ResourceId>> ownerOfBlob) {
    var meta = metaReader.apply(resourceId);
    if (meta.getPointerVersion() == 0L) {
      return Optional.empty();
    }
    var owner = ownerOfBlob.apply(meta.getBlobUri());
    if (owner.isEmpty()) {
      // The pointer moved between the version read and the content read, or it dangles. Either way
      // this scan is stale and must not be acted on.
      throw relationChanged(resourceId);
    }
    if (!owner.get().getId().equals(expectedOwner.getId())) {
      throw movedOutOfSubtree(resourceId, expectedOwner.getId());
    }
    return Optional.of(meta.getPointerVersion());
  }

  @FunctionalInterface
  private interface PageFetcher<T> {
    List<T> fetch(String pageToken, StringBuilder nextOut);
  }

  /**
   * Drains a key-paginated listing, invoking {@code consumer} for every item across all pages.
   *
   * <p>Guards against a {@link PointerStore} that returns a non-advancing page token: a repeated
   * cursor is treated as a hard error rather than spinning the worker thread forever with no
   * observable failure. This preserves the stagnant-token protection the replaced {@code
   * AccountServiceImpl.listAllPages} provided.
   */
  private static <T> void drainPages(PageFetcher<T> fetcher, Consumer<T> consumer) {
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var item : fetcher.fetch(token, next)) {
        consumer.accept(item);
      }
      token = advanceToken(next, seenTokens);
      if (token.isBlank()) {
        return;
      }
    }
  }

  /** Like {@link #drainPages} but short-circuits, returning true on the first matching item. */
  private static <T> boolean anyPage(PageFetcher<T> fetcher, Predicate<T> match) {
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var item : fetcher.fetch(token, next)) {
        if (match.test(item)) {
          return true;
        }
      }
      token = advanceToken(next, seenTokens);
      if (token.isBlank()) {
        return false;
      }
    }
  }

  private static String advanceToken(StringBuilder next, HashSet<String> seenTokens) {
    String token = next.toString();
    if (!token.isBlank() && !seenTokens.add(token)) {
      throw new IllegalStateException(
          "recursive delete pagination did not advance; repeated page token: " + token);
    }
    return token;
  }

  private void dropNamespaceRelations(
      Namespace namespace, DropSummary summary, boolean guarded, BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();

    drainPages(
        (token, next) ->
            tableRepo.list(
                namespaceId.getAccountId(),
                catalogId.getId(),
                namespaceId.getId(),
                200,
                token,
                next),
        table ->
            dropTable(
                namespace,
                table.getResourceId(),
                table.getNamespaceId(),
                summary,
                guarded,
                subtreePin));

    drainPages(
        (token, next) ->
            viewRepo.list(
                namespaceId.getAccountId(),
                catalogId.getId(),
                namespaceId.getId(),
                200,
                token,
                next),
        view -> dropView(namespace, view.getResourceId(), summary, guarded, subtreePin));
  }

  private void dropTable(
      Namespace namespace,
      ResourceId tableId,
      ResourceId tableNamespaceId,
      DropSummary summary,
      boolean guarded,
      BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();

    if (guarded) {
      var pinned =
          pinToNamespace(
              tableId,
              namespaceId,
              tableRepo::metaForSafe,
              blobUri -> tableRepo.getByBlobUri(blobUri).map(Table::getNamespaceId));
      if (pinned.isEmpty()) {
        // Already gone — a concurrent DeleteTable won and ran this same cleanup. Nothing to purge,
        // and the namespace emptiness check still has to pass before the namespace itself goes.
        CLEANUP_LOG.infof(
            "recursive_drop_table_skipped_absent account_id=%s namespace_id=%s table_id=%s",
            namespaceId.getAccountId(), namespaceId.getId(), tableId.getId());
        return;
      }
      // Delete pinned to the exact pointer version the membership check observed. Any concurrent
      // mutation — crucially a reparent that moved this table out of the subtree — advances the
      // canonical pointer, so the CAS fails and no owned state is purged. Deleting by id alone
      // would resolve the table's CURRENT pointer and destroy it in whatever namespace it had just
      // moved to.
      if (!tableRepo.deleteWithPrecondition(tableId, pinned.get(), subtreePin)) {
        throw relationChanged(tableId);
      }
      cleanupDeletedTable(tableId, tableNamespaceId, false);
      summary.tablesDeleted++;
      summary.snapshotPrefixesDeleted++;
      CLEANUP_LOG.infof(
          "recursive_drop_table account_id=%s catalog_id=%s namespace_id=%s table_id=%s committed=true",
          namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId(), tableId.getId());
      return;
    }

    boolean committed = tableRepo.delete(tableId);
    // Committed delete, or unguarded account teardown. In teardown there is no survivor to protect
    // —
    // the account, its catalogs, and namespaces are all going away — so purge owned state
    // unconditionally even when delete lost the CAS, or the table pointer and its root-resync
    // marker
    // outlive account deletion as durable orphans. The enclosing namespace is about to be deleted,
    // so cleanup must not bump its marker (false) and look like a concurrent child mutation.
    cleanupDeletedTable(tableId, tableNamespaceId, false);
    summary.tablesDeleted++;
    summary.snapshotPrefixesDeleted++;
    CLEANUP_LOG.infof(
        "recursive_drop_table account_id=%s catalog_id=%s namespace_id=%s table_id=%s committed=%s",
        namespaceId.getAccountId(),
        catalogId.getId(),
        namespaceId.getId(),
        tableId.getId(),
        committed);
  }

  private void dropView(
      Namespace namespace,
      ResourceId viewId,
      DropSummary summary,
      boolean guarded,
      BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();

    if (guarded) {
      // Same stale-scan hazard as tables: delete pinned to the version whose content was verified
      // to still live here, so a view that moved out cannot be destroyed in its new namespace.
      var pinned =
          pinToNamespace(
              viewId,
              namespaceId,
              viewRepo::metaForSafe,
              blobUri -> viewRepo.getByBlobUri(blobUri).map(View::getNamespaceId));
      if (pinned.isEmpty()) {
        CLEANUP_LOG.infof(
            "recursive_drop_view_skipped_absent account_id=%s namespace_id=%s view_id=%s",
            namespaceId.getAccountId(), namespaceId.getId(), viewId.getId());
        return;
      }
      if (!viewRepo.deleteWithPrecondition(viewId, pinned.get(), subtreePin)) {
        throw relationChanged(viewId);
      }
      topology.evict(viewId);
      metadataGraph.invalidate(viewId);
      summary.viewsDeleted++;
      CLEANUP_LOG.infof(
          "recursive_drop_view account_id=%s catalog_id=%s namespace_id=%s view_id=%s committed=true",
          namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId(), viewId.getId());
      return;
    }

    boolean committed = viewRepo.delete(viewId);
    topology.evict(viewId);
    metadataGraph.invalidate(viewId);
    summary.viewsDeleted++;
    CLEANUP_LOG.infof(
        "recursive_drop_view account_id=%s catalog_id=%s namespace_id=%s view_id=%s committed=%s",
        namespaceId.getAccountId(),
        catalogId.getId(),
        namespaceId.getId(),
        viewId.getId(),
        committed);
  }

  private void deleteNamespace(Namespace namespace) {
    // Account teardown: the whole tree and its account pointer are going away, so there is nothing
    // for a fence to protect and nothing a late child could be orphaned under.
    deleteNamespace(namespace, null, BatchGuard.NONE, UNPINNED);
  }

  /**
   * Deletes {@code namespace} and advances its ancestors' child markers, except {@code
   * skipMarkerId} — the root of a recursive drop. Skipping the root keeps the dropper's own
   * descendant removals from advancing the root marker, so the recursive-delete caller can
   * distinguish its single intentional advance from a concurrent write to the root's children.
   *
   * <p>{@code childrenGuard} makes the pointer removal atomic with the emptiness the caller
   * established: a child published since then raises {@link
   * BaseResourceRepository.BatchGuardFailedException}, which is retryable and re-runs the drop.
   *
   * <p>{@code expectedVersion} pins the removal to the pointer that was proven to be inside the
   * subtree ({@link #UNPINNED} in account teardown, where there is no subtree to leave). Without
   * it, deleting by id would resolve the namespace's current pointer and remove one that had been
   * reparented out of the subtree since the scan.
   */
  private void deleteNamespace(
      Namespace namespace,
      ResourceId skipMarkerId,
      BatchGuard childrenGuard,
      long expectedVersion) {
    var namespaceId = namespace.getResourceId();
    if (expectedVersion == UNPINNED) {
      namespaceRepo.delete(namespaceId, childrenGuard);
    } else if (!namespaceRepo.deleteWithPrecondition(namespaceId, expectedVersion, childrenGuard)) {
      throw namespaceChanged(namespaceId);
    }
    topology.evictRelationRefs(namespaceId);
    topology.evictNamespaceRefs(namespace.getCatalogId());
    metadataGraph.invalidate(namespaceId);
    markerStore.bumpCatalogMarker(namespace.getCatalogId());
    bumpParentNamespaceMarkers(namespace, skipMarkerId);
    CLEANUP_LOG.infof(
        "recursive_drop_namespace account_id=%s catalog_id=%s namespace_id=%s display_name=%s",
        namespaceId.getAccountId(),
        namespace.getCatalogId().getId(),
        namespaceId.getId(),
        namespace.getDisplayName());
  }

  private void bumpParentNamespaceMarkers(Namespace namespace, ResourceId skipMarkerId) {
    var catalogId = namespace.getCatalogId();
    var parents = namespace.getParentsList();
    for (int i = 0; i < parents.size(); i++) {
      namespaceRepo
          .getByPath(catalogId.getAccountId(), catalogId.getId(), parents.subList(0, i + 1))
          .map(Namespace::getResourceId)
          .filter(id -> skipMarkerId == null || !id.equals(skipMarkerId))
          .ifPresent(markerStore::bumpNamespaceMarker);
    }
  }

  private static boolean isDescendant(Namespace namespace, java.util.List<String> rootPath) {
    var parents = namespace.getParentsList();
    return parents.size() >= rootPath.size()
        && parents.subList(0, rootPath.size()).equals(rootPath);
  }

  public static final class DropSummary {
    public int namespacesDeleted;
    public int tablesDeleted;
    public int viewsDeleted;
    public int snapshotPrefixesDeleted;
  }
}
