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

/** Removes the dependent state owned by tables and namespace trees. */
@ApplicationScoped
public class RecursiveResourceDropper {

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
    var summary = new DropSummary();
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
      dropNamespace(descendant, summary);
    }
    dropNamespaceRelations(root, summary);
    return summary;
  }

  /** Drops a namespace and every object it owns, including descendant namespaces. */
  public DropSummary dropNamespaceTree(Namespace root) {
    var summary = dropNamespaceContents(root);
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

  private void dropNamespace(Namespace namespace, DropSummary summary) {
    dropNamespaceRelations(namespace, summary);
    requireNamespaceEmptyAndStable(namespace);
    deleteNamespace(namespace);
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

  private void dropNamespaceRelations(Namespace namespace, DropSummary summary) {
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
        tableRepo.delete(table.getResourceId());
        // The enclosing namespace is about to be guarded and deleted. Do not make its own
        // cleanup look like a concurrent child mutation to that marker protocol.
        cleanupDeletedTable(table.getResourceId(), table.getNamespaceId(), false);
        summary.tablesDeleted++;
        summary.snapshotPrefixesDeleted++;
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
        viewRepo.delete(view.getResourceId());
        topology.evict(view.getResourceId());
        metadataGraph.invalidate(view.getResourceId());
        summary.viewsDeleted++;
      }
      viewToken = next.toString();
    } while (!viewToken.isBlank());
  }

  private void deleteNamespace(Namespace namespace) {
    var namespaceId = namespace.getResourceId();
    namespaceRepo.delete(namespaceId);
    topology.evictRelationRefs(namespaceId);
    topology.evictNamespaceRefs(namespace.getCatalogId());
    metadataGraph.invalidate(namespaceId);
    markerStore.bumpCatalogMarker(namespace.getCatalogId());
    bumpParentNamespaceMarkers(namespace);
  }

  private void bumpParentNamespaceMarkers(Namespace namespace) {
    var catalogId = namespace.getCatalogId();
    var parents = namespace.getParentsList();
    for (int i = 0; i < parents.size(); i++) {
      namespaceRepo
          .getByPath(catalogId.getAccountId(), catalogId.getId(), parents.subList(0, i + 1))
          .map(Namespace::getResourceId)
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
