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
import ai.floedb.floecat.service.repo.impl.StatsRepository;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
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
    // Per-snapshot stats live under /snapshots/ (removed by the prefix above); the table-level and
    // per-target "latest committed" stats pointers/blobs live outside it and must be purged too, or
    // they outlive the deleted table as durable orphans.
    statsRepo.deleteAllStatsForTable(tableId);
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

  private void dropNamespaceRelations(Namespace namespace, DropSummary summary, boolean guarded) {
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
            dropTable(namespace, table.getResourceId(), table.getNamespaceId(), summary, guarded));

    drainPages(
        (token, next) ->
            viewRepo.list(
                namespaceId.getAccountId(),
                catalogId.getId(),
                namespaceId.getId(),
                200,
                token,
                next),
        view -> dropView(namespace, view.getResourceId(), summary, guarded));
  }

  private void dropTable(
      Namespace namespace,
      ResourceId tableId,
      ResourceId tableNamespaceId,
      DropSummary summary,
      boolean guarded) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    boolean committed = tableRepo.delete(tableId);
    if (!committed && guarded) {
      // Recursive namespace delete: tableRepo.delete returned false, so a concurrent update/rename
      // won the canonical-pointer CAS and the table may still be alive. Do not purge its owned
      // state; abort and let runWithRetry re-read (prior deletes this pass are idempotent).
      throw relationChanged(tableId);
    }
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
      Namespace namespace, ResourceId viewId, DropSummary summary, boolean guarded) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    boolean committed = viewRepo.delete(viewId);
    if (!committed && guarded) {
      throw relationChanged(viewId);
    }
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
