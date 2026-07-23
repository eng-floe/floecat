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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
    String cursor = "";
    do {
      var next = new StringBuilder();
      for (var namespace :
          namespaceRepo.list(
              root.getResourceId().getAccountId(),
              root.getCatalogId().getId(),
              rootPath,
              200,
              cursor,
              next)) {
        if (isDescendant(namespace, rootPath)) {
          descendants.add(namespace);
        }
      }
      cursor = next.toString();
    } while (!cursor.isBlank());
    descendants.sort(Comparator.comparingInt(Namespace::getParentsCount).reversed());
    for (var descendant : descendants) {
      dropNamespace(descendant, summary, rootId, guarded);
    }
    dropNamespaceRelations(root, summary, guarded);
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
    tableRoots.purgeRoot(tableId);
    pointerStore.delete(Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId()));
  }

  private void dropNamespace(
      Namespace namespace, DropSummary summary, ResourceId rootId, boolean guarded) {
    dropNamespaceRelations(namespace, summary, guarded);
    if (guarded) {
      requireNamespaceEmptyAndStable(namespace);
    }
    deleteNamespace(namespace, rootId);
    summary.namespacesDeleted++;
  }

  /**
   * Uses the same marker protocol as ordinary namespace deletion. This prevents a concurrently
   * created table or immediate child namespace from being orphaned by a recursive parent drop.
   */
  private void requireNamespaceEmptyAndStable(Namespace namespace) {
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
  }

  private boolean hasRelations(ResourceId namespaceId, ResourceId catalogId) {
    return tableRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0
        || viewRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0;
  }

  private boolean hasImmediateChildren(ResourceId catalogId, List<String> parentPath) {
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      for (var child :
          namespaceRepo.list(
              catalogId.getAccountId(), catalogId.getId(), parentPath, 200, cursor, next)) {
        if (isImmediateChildOf(child, parentPath)) {
          return true;
        }
      }
      cursor = next.toString();
      if (cursor.isBlank()) {
        return false;
      }
    }
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

  private void dropNamespaceRelations(Namespace namespace, DropSummary summary, boolean guarded) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    String tableToken = "";
    do {
      var next = new StringBuilder();
      for (var table :
          tableRepo.list(
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              200,
              tableToken,
              next)) {
        // Only purge a table's dependent state (snapshot prefix, table root, root-resync marker)
        // after its own delete actually commits. tableRepo.delete returns false when a concurrent
        // update/rename won the canonical-pointer CAS or the pointer already vanished — the table
        // may still be alive, so cleaning up its owned state would silently corrupt it.
        if (tableRepo.delete(table.getResourceId())) {
          // The enclosing namespace is about to be guarded and deleted. Do not make its own
          // cleanup look like a concurrent child mutation to that marker protocol.
          cleanupDeletedTable(table.getResourceId(), table.getNamespaceId(), false);
          summary.tablesDeleted++;
          summary.snapshotPrefixesDeleted++;
          CLEANUP_LOG.infof(
              "recursive_drop_table account_id=%s catalog_id=%s namespace_id=%s table_id=%s snapshot_prefix_purged=true",
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              table.getResourceId().getId());
        } else if (guarded) {
          // Recursive namespace delete: a concurrent writer holds the table alive. Abort and let
          // runWithRetry re-read; retrying is safe because prior deletes in this pass are
          // idempotent.
          throw relationChanged(table.getResourceId());
        } else {
          // Account teardown: the whole account is going away and cleanup must never throw a
          // retryable abort (that would re-run deleteAccount after the pointer is gone and skip
          // cleanup). The table pointer already moved or vanished, so skip its owned-state purge
          // rather than corrupt a survivor.
          CLEANUP_LOG.warnf(
              "recursive_drop_table_skipped_uncommitted account_id=%s catalog_id=%s namespace_id=%s table_id=%s",
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              table.getResourceId().getId());
        }
      }
      tableToken = next.toString();
    } while (!tableToken.isBlank());

    String viewToken = "";
    do {
      var next = new StringBuilder();
      for (var view :
          viewRepo.list(
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              200,
              viewToken,
              next)) {
        if (viewRepo.delete(view.getResourceId())) {
          topology.evict(view.getResourceId());
          metadataGraph.invalidate(view.getResourceId());
          summary.viewsDeleted++;
          CLEANUP_LOG.infof(
              "recursive_drop_view account_id=%s catalog_id=%s namespace_id=%s view_id=%s",
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              view.getResourceId().getId());
        } else if (guarded) {
          throw relationChanged(view.getResourceId());
        } else {
          CLEANUP_LOG.warnf(
              "recursive_drop_view_skipped_uncommitted account_id=%s catalog_id=%s namespace_id=%s view_id=%s",
              namespaceId.getAccountId(),
              catalogId.getId(),
              namespaceId.getId(),
              view.getResourceId().getId());
        }
      }
      viewToken = next.toString();
    } while (!viewToken.isBlank());
  }

  private void deleteNamespace(Namespace namespace) {
    deleteNamespace(namespace, null);
  }

  /**
   * Deletes {@code namespace} and advances its ancestors' child markers, except {@code
   * skipMarkerId} — the root of a recursive drop. Skipping the root keeps the dropper's own
   * descendant removals from advancing the root marker, so the recursive-delete caller can
   * distinguish its single intentional advance from a concurrent write to the root's children.
   */
  private void deleteNamespace(Namespace namespace, ResourceId skipMarkerId) {
    var namespaceId = namespace.getResourceId();
    namespaceRepo.delete(namespaceId);
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
