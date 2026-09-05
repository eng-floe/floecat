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

package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.common.BaseServiceImpl.normalizeName;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogTraversalFailures;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.IcebergSchemaMapper;
import ai.floedb.floecat.connector.spi.LogSafeText;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/** Materializes one Catalog Overlay's selected upstream metadata into its target catalog. */
@ApplicationScoped
public class CatalogOverlayReconciler {

  private static final Logger LOG = Logger.getLogger(CatalogOverlayReconciler.class);

  /**
   * Bound on any upstream-supplied text reaching a log line.
   *
   * <p>Catalog, schema, table and view names are upstream text, and so is a provider's failure
   * message -- {@code UnityCatalogAccessClient} builds one from a column name and its declared
   * type. A newline in any of them forges a second log entry, and none carry a length of their own.
   * The neighbouring vend and discovery paths bound theirs the same way.
   */
  private static final int MAX_LOGGED_CHARS = 256;

  static final String OVERLAY_ID_PROPERTY = "floecat.catalog-overlay.id";
  static final String INTEGRATION_ID_PROPERTY = "floecat.catalog-integration.id";
  static final String EXTERNAL_ID_PROPERTY = "floecat.external-object.id";
  static final String EXTERNAL_ID_STABLE_PROPERTY = "floecat.external-object.identity-stable";
  static final String METADATA_LOCATION_PROPERTY = "metadata_location";
  static final String STORAGE_LOCATION_PROPERTY = "storage_location";
  private static final int MAX_NAMESPACES = 100_000;

  @Inject CatalogIntegrationAccess access;
  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogOverlayRepository overlays;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject ViewRepository views;
  @Inject PointerStore pointerStore;
  @Inject TableRootRepository tableRoots;
  @Inject TableRootWriter rootWriter;
  @Inject MarkerStore markerStore;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;

  Clock clock = Clock.systemUTC();

  public Result reconcile(
      CatalogOverlay overlay,
      MutationMeta overlayMeta,
      CatalogIntegration integration,
      MutationMeta integrationMeta) {
    requireBinding(overlay, integration);
    PointerConditions fence = fence(overlay, overlayMeta, integration, integrationMeta);
    Discovery discovery;
    try (CatalogClient client = access.open(integration)) {
      client.validate();
      discovery = discover(client, overlay);
    }

    var result = new MutableResult();
    Map<NamespacePath, Namespace> localNamespaces =
        reconcileNamespaces(
            overlay, integration, fence, discovery.materializedNamespaces(), result);
    // Every path that made the pass partial, not just the branch ones. A cycle in which each
    // loadTable failed skippably populates only unobserved, and reporting zero there would be
    // indistinguishable from a clean no-op pass -- which is the confusion these counters exist to
    // remove.
    // Unioned, because all three sets are keyed by namespace: summing them counted a namespace
    // whose table and view listings both failed twice, which made the number neither a branch count
    // nor a failure count.
    Set<NamespacePath> skippedBranches = new HashSet<>(discovery.skippedPrefixes());
    skippedBranches.addAll(discovery.skippedTablePrefixes());
    skippedBranches.addAll(discovery.skippedViewPrefixes());
    result.branchesSkipped = skippedBranches.size();
    result.objectsSkipped =
        discovery.unobservedTables().size() + discovery.unobservedViews().size();
    retireStaleRelations(overlay, fence, discovery, localNamespaces, result);
    reconcileTables(
        overlay,
        overlayMeta,
        integration,
        integrationMeta,
        fence,
        discovery.tables(),
        localNamespaces,
        result);
    reconcileViews(overlay, integration, fence, discovery.views(), localNamespaces, result);
    retireNamespaces(overlay, fence, discovery, localNamespaces, result);
    assertFence(overlay, overlayMeta, integration, integrationMeta);
    return result.freeze();
  }

  /** Removes every materialized descendant after the overlay deletion fence is installed. */
  public void retireMaterializedResources(CatalogOverlay overlay) {
    ResourceId catalogId = overlay.getCatalogId();
    List<Namespace> catalogNamespaces = new ArrayList<>(listNamespaces(catalogId));
    catalogNamespaces.sort(
        Comparator.comparingInt((Namespace namespace) -> path(namespace).segments().size())
            .reversed());
    for (Namespace namespace : catalogNamespaces) {
      for (Table table : listTables(namespace)) {
        if (!ownedBy(table.getPropertiesMap(), overlay)) continue;
        if (!tables.delete(table.getResourceId())
            && tables.getById(table.getResourceId()).isPresent()) {
          throw new BaseResourceRepository.AbortRetryableException(
              "Table changed during Catalog Overlay retirement");
        }
        purgeTableState(table.getResourceId());
        relationChanged(table.getResourceId(), namespace.getResourceId());
      }
      for (View view : listViews(namespace)) {
        if (!ownedBy(view.getPropertiesMap(), overlay)) continue;
        if (!views.delete(view.getResourceId())
            && views.getById(view.getResourceId()).isPresent()) {
          throw new BaseResourceRepository.AbortRetryableException(
              "View changed during Catalog Overlay retirement");
        }
        relationChanged(view.getResourceId(), namespace.getResourceId());
      }
      if (!ownedBy(namespace.getPropertiesMap(), overlay)) continue;
      deleteNamespaceIfEmpty(namespace, PointerConditions.none());
    }
  }

  private Discovery discover(CatalogClient client, CatalogOverlay overlay) {
    var capabilities = client.capabilities();
    for (CatalogCapability required :
        List.of(
            CatalogCapability.LIST_NAMESPACES,
            CatalogCapability.LIST_TABLES,
            CatalogCapability.LOAD_TABLE)) {
      if (!capabilities.supports(required)) {
        throw new UnsupportedOperationException(
            "Catalog provider does not support required capability=" + required);
      }
    }
    boolean listViews = capabilities.supports(CatalogCapability.LIST_VIEWS);
    boolean loadViews = capabilities.supports(CatalogCapability.LOAD_VIEW);
    if (listViews != loadViews) {
      throw new IllegalStateException(
          "Catalog provider must advertise LIST_VIEWS and LOAD_VIEW together");
    }
    Set<NamespacePath> materialized = new LinkedHashSet<>();
    Map<CatalogObjectName, CatalogTable> discoveredTables = new LinkedHashMap<>();
    Map<CatalogObjectName, CatalogView> discoveredViews = new LinkedHashMap<>();
    // What the walk could not see, as opposed to what it saw was gone. Retirement reads a relation
    // missing from the maps above as deleted upstream and hard-deletes the local copy, so a branch
    // that was skipped or an object that would not load has to be recorded, or tolerating a
    // transient denial would destroy every overlay-owned relation beneath it on the next cycle.
    // Split by relation kind. A namespace listing that fails hides both kinds below it, but a
    // table listing that fails says nothing about views and vice versa -- one shared set meant a
    // recurring view-only denial kept deleted tables alive indefinitely.
    Set<NamespacePath> skippedPrefixes = new HashSet<>();
    Set<NamespacePath> skippedTablePrefixes = new HashSet<>();
    Set<NamespacePath> skippedViewPrefixes = new HashSet<>();
    // Failure classes already reported with a stack trace this pass. The two per-object skips
    // below are unbounded by nature -- a failure class that is per-object in code but catalog-wide
    // in practice, an output column type no view can represent being the case that motivated the
    // guard, raises once per object, every cycle. At MAX_NAMESPACES that is tens of thousands of
    // cause chains a pass, burying the branch-level warnings that need attention.
    //
    // Keyed on the class, not the object: the flood is many objects sharing one failure, so keying
    // on identity would suppress nothing. Per pass and unbounded, unlike the bounded map {@code
    // CatalogIntegrationAccess} keeps -- its key is tenant-supplied and has to survive across
    // calls, while this set dies with the walk and has one entry per code. The count of suppressed
    // repeats is already reported: it is objectsSkipped.
    Set<CatalogAccessException.Code> tracedSkipTableClasses = new HashSet<>();
    Set<CatalogAccessException.Code> tracedSkipViewClasses = new HashSet<>();
    // Split by kind like every other skip set here. One set meant a table whose load was skipped
    // also made a same-named view unobserved, so a view genuinely deleted upstream was never
    // retired -- the single place the kind split had not been carried through.
    Set<CatalogObjectName> unobservedTables = new HashSet<>();
    Set<CatalogObjectName> unobservedViews = new HashSet<>();
    // Namespaces where a detail load was skipped. Such a load never reveals the object's stable
    // identity, and retirement matches a stable-ID relation by identity rather than by name -- so a
    // table renamed upstream is stored under its old path, is not the name recorded in unobserved,
    // and would be retired and purged as though the rename were a deletion.
    //
    // Recorded per namespace rather than as one flag for the walk. A single object that fails to
    // load on every pass -- an unrepresentable view column does exactly that -- otherwise disabled
    // stable-ID retirement across the entire overlay indefinitely, so upstream deletions stopped
    // propagating anywhere and accumulated with no bound.
    Set<NamespacePath> tableIdentityUnknownIn = new HashSet<>();
    Set<NamespacePath> viewIdentityUnknownIn = new HashSet<>();
    Set<NamespacePath> seen = new HashSet<>();
    var pending = new ArrayDeque<NamespacePath>();
    pending.add(NamespacePath.root());
    while (!pending.isEmpty()) {
      NamespacePath parent = pending.removeFirst();
      // Normalized before it reaches the include-filter comparison, as the sibling call below is.
      // The filters are written in normalized form, so an upstream namespace whose raw segments
      // differ -- surrounding or collapsible whitespace, a non-NFKC form -- would fail to match and
      // be judged irrelevant when the operator had in fact selected it.
      NamespacePath normalizedParent = normalizePath(parent);
      List<NamespacePath> children;
      try {
        children = client.listNamespaces(parent);
      } catch (CatalogAccessException failure) {
        // Tolerated only where the operator did not ask for this branch. An overlay with no
        // include filters selects the whole upstream tree -- the documented default -- and a Unity
        // workspace almost always has a system catalog whose schemas the integration principal
        // cannot enumerate, so propagating that aborted reconcile for every table in the overlay.
        // Where include filters name something under this branch, the denial is the answer to a
        // question the operator actually asked, and it surfaces.
        if (!tolerateBranchFailure(overlay, normalizedParent, failure, true)) {
          throw failure;
        }
        // Nothing below this parent was enumerated, so nothing below it is known to be gone.
        // Logged and counted: the skip excludes the branch from materialization as well as from
        // retirement, so new upstream objects there never appear and the overlay drifts. Silence
        // would leave an operator no way to tell that from a complete reconcile.
        LOG.warnf(
            failure,
            "overlay %s skipping namespaces under %s: %s",
            overlay.getResourceId().getId(),
            LogSafeText.bounded(String.valueOf(parent), MAX_LOGGED_CHARS),
            LogSafeText.bounded(failure.getMessage(), MAX_LOGGED_CHARS));
        skippedPrefixes.add(normalizedParent);
        continue;
      }
      for (NamespacePath path : children.stream().sorted().toList()) {
        if (!seen.add(path)) continue;
        if (seen.size() > MAX_NAMESPACES) {
          throw new IllegalStateException("Catalog namespace inventory exceeds " + MAX_NAMESPACES);
        }
        NamespacePath normalizedPath = normalizePath(path);
        if (excluded(overlay, normalizedPath)) continue;
        if (selected(overlay, normalizedPath)) {
          addAncestors(normalizedPath, materialized);
          List<CatalogObjectName> tableNames;
          try {
            tableNames = client.listTables(path);
          } catch (CatalogAccessException failure) {
            if (!tolerateBranchFailure(overlay, normalizedPath, failure, false)) {
              throw failure;
            }
            LOG.warnf(
                failure,
                "overlay %s skipping tables in %s: %s",
                overlay.getResourceId().getId(),
                LogSafeText.bounded(String.valueOf(normalizedPath), MAX_LOGGED_CHARS),
                LogSafeText.bounded(failure.getMessage(), MAX_LOGGED_CHARS));
            skippedTablePrefixes.add(normalizedPath);
            // Empty, and fall through rather than skip the namespace. Views are listed separately
            // below and a table denial says nothing about them, but leaving by this path meant they
            // were never enumerated while only the table skip was recorded -- so retirement read
            // the gap as a deletion and destroyed local copies of views that were alive upstream.
            // Falling through also leaves the descent to the enqueue at the foot of the loop, which
            // is the one place that decides it.
            tableNames = List.of();
          }
          for (CatalogObjectName name : tableNames.stream().sorted().toList()) {
            CatalogObjectName localTableName =
                new CatalogObjectName(normalizedPath, normalizeName(name.name()));
            CatalogTable table;
            try {
              table = client.loadTable(name);
            } catch (CatalogAccessException failure) {
              // Same treatment as a view that will not load: a table dropped upstream between the
              // listing and this read answers NOT_FOUND, which used to abort the cycle for every
              // table and view in the overlay.
              //
              // Note what this does not cover. A columns array the strict decoder rejects arrives
              // as INVALID_RESPONSE -> INTERNAL, which describesOneBranch deliberately excludes, so
              // it still fails the reconcile. That is the intended reading: a proxy or version
              // difference that reshapes columns reshapes them for every table, so it is a fault of
              // the catalog rather than of one object, and walking the inventory to collect the
              // same answer would bury it.
              if (!CatalogTraversalFailures.describesOneBranch(failure)) {
                throw failure;
              }
              if (tracedSkipTableClasses.add(failure.code())) {
                LOG.warnf(
                    failure,
                    "overlay %s skipping table %s: %s",
                    overlay.getResourceId().getId(),
                    LogSafeText.bounded(String.valueOf(name), MAX_LOGGED_CHARS),
                    LogSafeText.bounded(failure.getMessage(), MAX_LOGGED_CHARS));
              } else {
                LOG.debugf(
                    "overlay %s skipping table %s (%s again)",
                    overlay.getResourceId().getId(),
                    LogSafeText.bounded(String.valueOf(name), MAX_LOGGED_CHARS),
                    failure.code());
              }
              unobservedTables.add(localTableName);
              tableIdentityUnknownIn.add(normalizedPath);
              continue;
            }
            if (!name.equals(table.name())) {
              throw new IllegalStateException(
                  "Catalog provider returned table metadata for the wrong object: expected="
                      + name
                      + " actual="
                      + table.name());
            }
            // The same value localTableName already holds. Two names for one expression made the
            // invariant that matters -- the unobserved entry and the discoveredTables key must be
            // the same key, or retirement misjudges the relation -- something a reader had to
            // verify character by character.
            if (discoveredTables.putIfAbsent(localTableName, table) != null) {
              throw new IllegalStateException(
                  "Upstream table names collide after normalization: " + localTableName);
            }
          }
          if (listViews) {
            List<CatalogObjectName> viewNames;
            try {
              viewNames = client.listViews(path);
            } catch (CatalogAccessException failure) {
              // The same guard the table listing beside it carries. For Unity both listings share
              // one RPC so they rarely disagree, but this reconciler is provider-neutral and a
              // denial on one namespace's views was aborting the whole overlay.
              if (!tolerateBranchFailure(overlay, normalizedPath, failure, false)) {
                throw failure;
              }
              LOG.warnf(
                  failure,
                  "overlay %s skipping views in %s: %s",
                  overlay.getResourceId().getId(),
                  LogSafeText.bounded(String.valueOf(normalizedPath), MAX_LOGGED_CHARS),
                  LogSafeText.bounded(failure.getMessage(), MAX_LOGGED_CHARS));
              skippedViewPrefixes.add(normalizedPath);
              // Falls through like the table listing above it, for the same reason: the enqueue at
              // the foot of the loop is the one place that decides descent, and a second copy here
              // is a rule that can drift from it.
              viewNames = List.of();
            }
            for (CatalogObjectName name : viewNames.stream().sorted().toList()) {
              CatalogObjectName localName =
                  new CatalogObjectName(normalizedPath, normalizeName(name.name()));
              if (discoveredTables.containsKey(localName)) {
                throw new IllegalStateException(
                    "Upstream relation name is both table and view: " + localName);
              }
              CatalogView view;
              try {
                view = client.loadView(name);
              } catch (CatalogAccessException failure) {
                // One view cannot cost the overlay every table in it. A listing and a detail read
                // can disagree -- the Unity /tables listing is parsed leniently, so a deployment
                // that omits columns there passes a view whose output schema loadView then refuses
                // by name -- and this call had no guard, so that view aborted reconcile and kept
                // aborting it until someone dropped it upstream. Recorded as unobserved so
                // retirement does not read the gap as "deleted upstream" and remove the local copy.
                if (!CatalogTraversalFailures.describesOneBranch(failure)) {
                  throw failure;
                }
                if (tracedSkipViewClasses.add(failure.code())) {
                  LOG.warnf(
                      failure,
                      "overlay %s skipping view %s: %s",
                      overlay.getResourceId().getId(),
                      LogSafeText.bounded(String.valueOf(name), MAX_LOGGED_CHARS),
                      LogSafeText.bounded(failure.getMessage(), MAX_LOGGED_CHARS));
                } else {
                  LOG.debugf(
                      "overlay %s skipping view %s (%s again)",
                      overlay.getResourceId().getId(),
                      LogSafeText.bounded(String.valueOf(name), MAX_LOGGED_CHARS),
                      failure.code());
                }
                unobservedViews.add(localName);
                viewIdentityUnknownIn.add(normalizedPath);
                continue;
              }
              if (!name.equals(view.name())) {
                throw new IllegalStateException(
                    "Catalog provider returned view metadata for the wrong object: expected="
                        + name
                        + " actual="
                        + view.name());
              }
              if (discoveredViews.putIfAbsent(localName, view) != null) {
                throw new IllegalStateException(
                    "Upstream view names collide after normalization: " + localName);
              }
            }
          }
        }
        if (mayContainSelection(overlay, normalizedPath)) pending.addLast(path);
      }
    }
    requireUniqueStableIdentities(
        discoveredTables.values().stream().map(CatalogTable::identity).toList(), "table");
    requireUniqueStableIdentities(
        discoveredViews.values().stream().map(CatalogView::identity).toList(), "view");
    return new Discovery(
        Set.copyOf(materialized),
        Map.copyOf(discoveredTables),
        Map.copyOf(discoveredViews),
        Set.copyOf(skippedPrefixes),
        Set.copyOf(skippedTablePrefixes),
        Set.copyOf(skippedViewPrefixes),
        Set.copyOf(unobservedTables),
        Set.copyOf(unobservedViews),
        Set.copyOf(tableIdentityUnknownIn),
        Set.copyOf(viewIdentityUnknownIn));
  }

  private Map<NamespacePath, Namespace> reconcileNamespaces(
      CatalogOverlay overlay,
      CatalogIntegration integration,
      PointerConditions fence,
      Set<NamespacePath> targetPaths,
      MutableResult result) {
    ResourceId catalogId = overlay.getCatalogId();
    Map<NamespacePath, Namespace> current = new HashMap<>();
    for (Namespace namespace : listNamespaces(catalogId)) {
      current.put(path(namespace), namespace);
    }
    for (NamespacePath path : targetPaths.stream().sorted().toList()) {
      if (current.containsKey(path)) continue;
      List<String> segments = path.segments();
      Namespace created =
          Namespace.newBuilder()
              .setResourceId(randomId(catalogId.getAccountId(), ResourceKind.RK_NAMESPACE))
              .setDisplayName(segments.get(segments.size() - 1))
              .addAllParents(segments.subList(0, segments.size() - 1))
              .setCatalogId(catalogId)
              .setCreatedAt(now())
              .putAllProperties(ownershipProperties(overlay, integration))
              .build();
      // Joins the parent's child set exactly as CreateNamespace does, or a concurrent rename of
      // that parent passes its own fence and strands what this creates.
      var parentSegments = segments.subList(0, segments.size() - 1);
      var namespaceFence =
          fence.and(orRetry(() -> namespaces.createFence(markerStore, catalogId, parentSegments)));
      if (!namespaces.createWhilePointersMatch(created, namespaceFence)) throw lostFence();
      metadataGraph.invalidate(created.getResourceId());
      topology.evictNamespaceRefs(catalogId);
      current.put(path, created);
      result.namespacesCreated++;
    }
    return current;
  }

  /**
   * Deletes overlay-owned relations the upstream no longer has.
   *
   * <p>Only where the walk was in a position to see that. A relation missing from the discovery
   * maps is either gone upstream or was never looked at, and those call for opposite actions: the
   * first is the deletion this method exists for, the second is a branch a transient denial hid.
   * {@link Discovery#observed} tells them apart, and getting it wrong destroys metadata a later
   * cycle cannot rebuild -- the local copy and its purge state are both gone.
   */
  private void retireStaleRelations(
      CatalogOverlay overlay,
      PointerConditions fence,
      Discovery discovery,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
    Map<CatalogObjectName, CatalogTable> discoveredTables = discovery.tables();
    Map<CatalogObjectName, CatalogView> discoveredViews = discovery.views();
    Map<String, Table> tablesByIdentity = new HashMap<>();
    Map<CatalogObjectName, Table> tablesByName = new HashMap<>();
    Map<String, View> viewsByIdentity = new HashMap<>();
    Map<CatalogObjectName, View> viewsByName = new HashMap<>();
    for (Namespace namespace : localNamespaces.values()) {
      NamespacePath namespacePath = path(namespace);
      for (Table table : listTables(namespace)) {
        if (!ownedBy(table.getPropertiesMap(), overlay)) continue;
        tablesByName.put(new CatalogObjectName(namespacePath, table.getDisplayName()), table);
        stableIdentity(table.getPropertiesMap()).ifPresent(id -> tablesByIdentity.put(id, table));
      }
      for (View view : listViews(namespace)) {
        if (!ownedBy(view.getPropertiesMap(), overlay)) continue;
        viewsByName.put(new CatalogObjectName(namespacePath, view.getDisplayName()), view);
        stableIdentity(view.getPropertiesMap()).ifPresent(id -> viewsByIdentity.put(id, view));
      }
    }

    Set<String> retainedTableIds = new HashSet<>();
    for (var entry : discoveredTables.entrySet()) {
      CatalogTable source = entry.getValue();
      Table current =
          source.identity().stable()
              ? tablesByIdentity.get(source.identity().value())
              : tablesByName.get(entry.getKey());
      if (current != null) retainedTableIds.add(current.getResourceId().getId());
    }
    Set<String> retainedViewIds = new HashSet<>();
    for (var entry : discoveredViews.entrySet()) {
      CatalogView source = entry.getValue();
      View current =
          source.identity().stable()
              ? viewsByIdentity.get(source.identity().value())
              : viewsByName.get(entry.getKey());
      if (current != null) retainedViewIds.add(current.getResourceId().getId());
    }

    for (var staleEntry : tablesByName.entrySet()) {
      Table stale = staleEntry.getValue();
      if (retainedTableIds.contains(stale.getResourceId().getId())) continue;
      if (!discovery.observedTable(staleEntry.getKey())) continue;
      if (discovery.tableIdentityMayBeHidden(staleEntry.getKey().namespace())
          && stableIdentity(stale.getPropertiesMap()).isPresent()) {
        // Its name says nothing: a stable-ID relation is matched by identity, and a rename moves it
        // out from under the name recorded in unobserved. Some load this pass did not reveal its
        // identity, so this could be that relation under its previous path -- and deleting it here
        // would purge a resource the next successful pass recreates from scratch, breaking the
        // stable-identity guarantee the matching above exists to keep.
        continue;
      }
      MutationMeta meta = tables.metaFor(stale.getResourceId());
      if (!tables.deleteWhilePointersMatch(
          stale.getResourceId(), meta.getPointerVersion(), fence)) {
        throw lostFence();
      }
      purgeTableState(stale.getResourceId());
      relationChanged(stale.getResourceId(), stale.getNamespaceId());
      result.tablesDeleted++;
    }
    for (var staleEntry : viewsByName.entrySet()) {
      View stale = staleEntry.getValue();
      if (retainedViewIds.contains(stale.getResourceId().getId())) continue;
      if (!discovery.observedView(staleEntry.getKey())) continue;
      if (discovery.viewIdentityMayBeHidden(staleEntry.getKey().namespace())
          && stableIdentity(stale.getPropertiesMap()).isPresent()) {
        continue;
      }
      MutationMeta meta = views.metaFor(stale.getResourceId());
      if (!views.deleteWhilePointersMatch(stale.getResourceId(), meta.getPointerVersion(), fence)) {
        throw lostFence();
      }
      relationChanged(stale.getResourceId(), stale.getNamespaceId());
      result.viewsDeleted++;
    }
  }

  private void reconcileTables(
      CatalogOverlay overlay,
      MutationMeta overlayMeta,
      CatalogIntegration integration,
      MutationMeta integrationMeta,
      PointerConditions fence,
      Map<CatalogObjectName, CatalogTable> discovered,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
    Map<String, Table> existingByIdentity = new HashMap<>();
    Map<CatalogObjectName, Table> existingByName = new HashMap<>();
    for (Namespace namespace : localNamespaces.values()) {
      for (Table table : listTables(namespace)) {
        if (!ownedBy(table.getPropertiesMap(), overlay)) continue;
        existingByName.put(new CatalogObjectName(path(namespace), table.getDisplayName()), table);
        stableIdentity(table.getPropertiesMap()).ifPresent(id -> existingByIdentity.put(id, table));
      }
    }

    for (var entry : discovered.entrySet()) {
      CatalogObjectName name = entry.getKey();
      CatalogTable source = entry.getValue();
      Namespace namespace = requireNamespace(localNamespaces, name.namespace());
      Table current =
          source.identity().stable()
              ? existingByIdentity.get(source.identity().value())
              : existingByName.get(name);
      Table desired = tableFor(overlay, integration, namespace, source, current);
      boolean changed = false;
      MutationMeta definitionMeta;
      if (current == null) {
        definitionMeta =
            tables
                .createWhilePointersMatch(
                    desired,
                    fence.and(
                        orRetry(() -> markerStore.relationCreateFence(desired.getNamespaceId()))))
                .orElseThrow(CatalogOverlayReconciler::lostFence);
        result.tablesCreated++;
        changed = true;
      } else if (!current.equals(desired)) {
        MutationMeta meta = tables.metaFor(current.getResourceId());
        // An update that moves the table adds a relation to the destination, so it has to pass the
        // destination's relation fence exactly as a create does. Leaving the source only makes it
        // emptier, so the source needs no fence.
        definitionMeta =
            tables
                .updateWhilePointersMatch(
                    desired,
                    meta.getPointerVersion(),
                    fence.and(
                        orRetry(
                            () ->
                                markerStore.relationMoveFence(
                                    current.getNamespaceId(),
                                    desired.getNamespaceId(),
                                    !current
                                        .getCatalogId()
                                        .getId()
                                        .equals(desired.getCatalogId().getId())))))
                .orElseThrow(CatalogOverlayReconciler::lostFence);
        result.tablesUpdated++;
        changed = true;
      } else {
        definitionMeta = tables.metaFor(current.getResourceId());
      }
      commitDefinitionWhileFenced(
          desired.getResourceId(),
          definitionMeta,
          overlay,
          overlayMeta,
          integration,
          integrationMeta);
      if (changed) {
        relationChanged(desired.getResourceId(), desired.getNamespaceId());
        if (current != null && !current.getNamespaceId().equals(desired.getNamespaceId())) {
          relationChanged(desired.getResourceId(), current.getNamespaceId());
        }
      }
    }
  }

  private void reconcileViews(
      CatalogOverlay overlay,
      CatalogIntegration integration,
      PointerConditions fence,
      Map<CatalogObjectName, CatalogView> discovered,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
    Map<String, View> existingByIdentity = new HashMap<>();
    Map<CatalogObjectName, View> existingByName = new HashMap<>();
    for (Namespace namespace : localNamespaces.values()) {
      for (View view : listViews(namespace)) {
        if (!ownedBy(view.getPropertiesMap(), overlay)) continue;
        existingByName.put(new CatalogObjectName(path(namespace), view.getDisplayName()), view);
        stableIdentity(view.getPropertiesMap()).ifPresent(id -> existingByIdentity.put(id, view));
      }
    }

    for (var entry : discovered.entrySet()) {
      CatalogObjectName name = entry.getKey();
      CatalogView source = entry.getValue();
      Namespace namespace = requireNamespace(localNamespaces, name.namespace());
      View current =
          source.identity().stable()
              ? existingByIdentity.get(source.identity().value())
              : existingByName.get(name);
      View desired = viewFor(overlay, integration, namespace, source, current);
      boolean changed = false;
      if (current == null) {
        if (!views.createWhilePointersMatch(
            desired,
            fence.and(orRetry(() -> markerStore.relationCreateFence(desired.getNamespaceId()))))) {
          throw lostFence();
        }
        result.viewsCreated++;
        changed = true;
      } else if (!current.equals(desired)) {
        MutationMeta meta = views.metaFor(current.getResourceId());
        // Moves a view between namespaces on the same terms as a table. See reconcileTables.
        if (views
            .updateWhilePointersMatch(
                desired,
                meta.getPointerVersion(),
                fence.and(
                    orRetry(
                        () ->
                            markerStore.relationMoveFence(
                                current.getNamespaceId(),
                                desired.getNamespaceId(),
                                !current
                                    .getCatalogId()
                                    .getId()
                                    .equals(desired.getCatalogId().getId())))))
            .isEmpty()) throw lostFence();
        result.viewsUpdated++;
        changed = true;
      }
      if (changed) {
        relationChanged(desired.getResourceId(), desired.getNamespaceId());
        if (current != null && !current.getNamespaceId().equals(desired.getNamespaceId())) {
          relationChanged(desired.getResourceId(), current.getNamespaceId());
        }
      }
    }
  }

  /**
   * Deletes overlay-owned namespaces the upstream no longer has.
   *
   * <p>Guarded the same way as the relations above, and for the same reason: {@code
   * materializedNamespaces} is truncated by a branch the walk could not enumerate, so without the
   * check a tolerated denial would delete the namespaces under it as well as their contents.
   */
  private void retireNamespaces(
      CatalogOverlay overlay,
      PointerConditions fence,
      Discovery discovery,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
    Set<NamespacePath> targetPaths = discovery.materializedNamespaces();
    List<Map.Entry<NamespacePath, Namespace>> stale =
        localNamespaces.entrySet().stream()
            .filter(entry -> !targetPaths.contains(entry.getKey()))
            .filter(entry -> discovery.observedNamespace(entry.getKey()))
            .filter(entry -> ownedBy(entry.getValue().getPropertiesMap(), overlay))
            .sorted(Map.Entry.<NamespacePath, Namespace>comparingByKey().reversed())
            .toList();
    for (var entry : stale) {
      Namespace namespace = entry.getValue();
      if (deleteNamespaceIfEmpty(namespace, fence)) {
        result.namespacesDeleted++;
      }
    }
  }

  private Table tableFor(
      CatalogOverlay overlay,
      CatalogIntegration integration,
      Namespace namespace,
      CatalogTable source,
      Table current) {
    ResourceId id =
        current == null
            ? randomId(overlay.getResourceId().getAccountId(), ResourceKind.RK_TABLE)
            : current.getResourceId();
    var properties = objectProperties(overlay, integration, source.identity());
    source.metadataLocation().ifPresent(value -> properties.put(METADATA_LOCATION_PROPERTY, value));
    source.storageLocation().ifPresent(value -> properties.put(STORAGE_LOCATION_PROPERTY, value));
    var upstream =
        UpstreamRef.newBuilder()
            .setCatalogIntegrationId(integration.getResourceId())
            .setCatalogOverlayId(overlay.getResourceId())
            .setUri(source.metadataLocation().orElse(integration.getCatalogUri()))
            .addAllNamespacePath(source.name().namespace().segments())
            .setTableDisplayName(source.name().name())
            .setFormat(tableFormat(source.format()))
            .addAllPartitionKeys(source.partitionKeys())
            .setColumnIdAlgorithm(columnIdAlgorithm(source.format()))
            .build();
    return Table.newBuilder()
        .setResourceId(id)
        .setDisplayName(normalizeName(source.name().name()))
        .setCatalogId(overlay.getCatalogId())
        .setNamespaceId(namespace.getResourceId())
        .setCreatedAt(current == null ? now() : current.getCreatedAt())
        .setSchemaJson(source.schemaJson())
        .setUpstream(upstream)
        .putAllProperties(properties)
        .build();
  }

  private View viewFor(
      CatalogOverlay overlay,
      CatalogIntegration integration,
      Namespace namespace,
      CatalogView source,
      View current) {
    ResourceId id =
        current == null
            ? randomId(overlay.getResourceId().getAccountId(), ResourceKind.RK_VIEW)
            : current.getResourceId();
    var schema =
        IcebergSchemaMapper.map(
            ColumnIdAlgorithm.CID_FIELD_ID, source.outputSchemaJson(), Set.of());
    var builder =
        View.newBuilder()
            .setResourceId(id)
            .setCatalogId(overlay.getCatalogId())
            .setNamespaceId(namespace.getResourceId())
            .setDisplayName(normalizeName(source.name().name()))
            .setCreatedAt(current == null ? now() : current.getCreatedAt())
            .addAllCreationSearchPath(normalizePath(source.defaultNamespace()).segments())
            .addAllOutputColumns(schema.getColumnsList())
            .putAllProperties(objectProperties(overlay, integration, source.identity()));
    for (var definition : source.definitions()) {
      builder.addSqlDefinitions(
          ViewSqlDefinition.newBuilder()
              .setSql(definition.sql())
              .setDialect(definition.dialect())
              .build());
    }
    return builder.build();
  }

  private PointerConditions fence(
      CatalogOverlay overlay,
      MutationMeta overlayMeta,
      CatalogIntegration integration,
      MutationMeta integrationMeta) {
    String accountId = overlay.getResourceId().getAccountId();
    return new PointerConditions(
        Map.of(
            Keys.catalogOverlayPointerById(accountId, overlay.getResourceId().getId()),
            overlayMeta.getPointerVersion(),
            Keys.catalogIntegrationPointerById(accountId, integration.getResourceId().getId()),
            integrationMeta.getPointerVersion()),
        Set.of(
            Keys.catalogOverlayDeletionMarker(accountId, overlay.getResourceId().getId()),
            Keys.catalogIntegrationDeletionMarker(accountId, integration.getResourceId().getId())),
        Map.of());
  }

  private void assertFence(
      CatalogOverlay overlay,
      MutationMeta overlayMeta,
      CatalogIntegration integration,
      MutationMeta integrationMeta) {
    if (overlays.metaForSafe(overlay.getResourceId()).getPointerVersion()
            != overlayMeta.getPointerVersion()
        || integrations.metaForSafe(integration.getResourceId()).getPointerVersion()
            != integrationMeta.getPointerVersion()
        || overlays.deletionFenceVersion(overlay.getResourceId()) != 0L
        || integrations.cascadeDeletionFenceVersion(integration.getResourceId()) != 0L) {
      throw lostFence();
    }
  }

  private void commitDefinitionWhileFenced(
      ResourceId tableId,
      MutationMeta definitionMeta,
      CatalogOverlay overlay,
      MutationMeta overlayMeta,
      CatalogIntegration integration,
      MutationMeta integrationMeta) {
    rootWriter.commitDefinition(tableId, definitionMeta);
    try {
      assertFence(overlay, overlayMeta, integration, integrationMeta);
    } catch (BaseResourceRepository.AbortRetryableException lost) {
      removeStaleRootPublication(tableId, definitionMeta);
      throw lost;
    }
  }

  private void removeStaleRootPublication(ResourceId tableId, MutationMeta publishedDefinition) {
    MutationMeta currentDefinition = tables.metaForSafe(tableId);
    if (sameRevision(publishedDefinition, currentDefinition)) return;
    rootWriter.replaceDefinitionIfMatches(tableId, publishedDefinition, currentDefinition);
  }

  private static boolean sameRevision(MutationMeta first, MutationMeta second) {
    return first.getPointerVersion() == second.getPointerVersion()
        && first.getBlobUri().equals(second.getBlobUri())
        && first.getEtag().equals(second.getEtag());
  }

  private static void requireBinding(CatalogOverlay overlay, CatalogIntegration integration) {
    if (!overlay.getIntegrationId().equals(integration.getResourceId())) {
      throw new IllegalArgumentException(
          "Overlay is not bound to the supplied Catalog Integration");
    }
    if (!overlay.hasCatalogId()) {
      throw new IllegalStateException("Catalog Overlay is missing its target catalog");
    }
  }

  private List<Table> listTables(Namespace namespace) {
    return listAll(
        (token, next) ->
            tables.listConsistent(
                namespace.getResourceId().getAccountId(),
                namespace.getCatalogId().getId(),
                namespace.getResourceId().getId(),
                200,
                token,
                next));
  }

  private List<Namespace> listNamespaces(ResourceId catalogId) {
    return listAll(
        (token, next) ->
            namespaces.listConsistent(
                catalogId.getAccountId(), catalogId.getId(), List.of(), 200, token, next));
  }

  private List<View> listViews(Namespace namespace) {
    return listAll(
        (token, next) ->
            views.listConsistent(
                namespace.getResourceId().getAccountId(),
                namespace.getCatalogId().getId(),
                namespace.getResourceId().getId(),
                200,
                token,
                next));
  }

  private static <T> List<T> listAll(Page<T> page) {
    List<T> out = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      out.addAll(page.load(token, next));
      token = next.toString();
      if (token.isBlank()) return List.copyOf(out);
      if (!seen.add(token)) throw new IllegalStateException("Stagnant repository page token");
    }
  }

  /**
   * Deletes a namespace only while it still holds nothing, reporting whether it went.
   *
   * <p>Both shape markers are sampled before the emptiness checks and ride the delete's batch, so a
   * relation or child namespace that lands after those checks costs this delete its CAS rather than
   * being orphaned by it. Without that this reaches exactly the outcome DeleteNamespace is fenced
   * against, by a different door.
   *
   * <p>A child namespace this overlay does not own is invisible to the ownership filters that
   * select what to retire, so it has to be checked for here or the parent goes away underneath it.
   */
  private boolean deleteNamespaceIfEmpty(Namespace namespace, PointerConditions fence) {
    ResourceId namespaceId = namespace.getResourceId();
    // Sampled before the emptiness checks below, and removed with the row rather than advanced --
    // the same idiom the service's delete uses. Advancing here would not merely leave the markers
    // behind: one never written samples as absent, so the advance CREATES a row for a namespace
    // this is in the act of deleting, which nothing can ever read again.
    // The overlay fence is composed in here, so the removal carries both.
    MarkerStore.MarkerRemoval shapeMarkers =
        markerStore.namespaceShapeMarkers(namespaceId).and(fence);
    if (relationCount(namespace) > 0
        || namespaces.hasDescendants(
            namespace.getCatalogId().getAccountId(),
            namespace.getCatalogId().getId(),
            path(namespace).segments())) {
      return false;
    }
    MutationMeta meta = namespaces.metaForSafe(namespaceId);
    if (meta.getPointerVersion() == 0L) {
      // Already gone -- another pass or another writer retired it. Nothing to do, and nothing lost:
      // failing the fence here would retry the whole reconcile for a namespace that is not there.
      return false;
    }
    if (!namespaces.deleteWhileShapeUnchanged(
        namespaceId, meta.getPointerVersion(), shapeMarkers)) {
      throw lostFence();
    }
    metadataGraph.invalidate(namespaceId);
    topology.evictNamespaceRefs(namespace.getCatalogId());
    return true;
  }

  /**
   * A fence this reconcile pass lost. Retryable: the next pass reads what it has to contend with.
   */
  private static BaseResourceRepository.AbortRetryableException lostFence() {
    return BaseResourceRepository.AbortRetryableException.lostFence(
        "catalog overlay reconciliation");
  }

  /**
   * A fence whose namespace has already been retired, reported as contention.
   *
   * <p>Any of these fences refuses a namespace that no longer exists. For a request that is a
   * caller error; for a reconcile pass it is a race -- the overlay named it while it was there and
   * another writer retired it since -- so a pass retries rather than failing.
   */
  private PointerConditions orRetry(Supplier<PointerConditions> fence) {
    try {
      return fence.get();
    } catch (BaseResourceRepository.NotFoundException retired) {
      throw lostFence();
    }
  }

  private int relationCount(Namespace namespace) {
    return NamespaceRepository.relationCount(
        tables, views, namespace.getCatalogId(), namespace.getResourceId());
  }

  /**
   * Invalidates the caches that name a relation, after its write has committed.
   *
   * <p>It does not touch the relation marker. A write that adds a relation asserts the marker in
   * its own batch, so advancing it again here would cost an unrelated concurrent writer its fence
   * for no gain; and a write that only removes one never needs to, because nothing is orphaned by a
   * namespace that became emptier than the caller checked.
   */
  private void relationChanged(ResourceId relationId, ResourceId namespaceId) {
    metadataGraph.invalidate(relationId);
    topology.evictRelationRefs(namespaceId);
  }

  private void purgeTableState(ResourceId tableId) {
    pointerStore.deleteByPrefix(Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId()));
    tableRoots.purgeRoot(tableId);
  }

  private static Namespace requireNamespace(
      Map<NamespacePath, Namespace> namespaces, NamespacePath path) {
    return Optional.ofNullable(namespaces.get(path))
        .orElseThrow(() -> new IllegalStateException("Missing materialized namespace=" + path));
  }

  private static Map<String, String> ownershipProperties(
      CatalogOverlay overlay, CatalogIntegration integration) {
    return Map.of(
        OVERLAY_ID_PROPERTY,
        overlay.getResourceId().getId(),
        INTEGRATION_ID_PROPERTY,
        integration.getResourceId().getId());
  }

  private static Map<String, String> objectProperties(
      CatalogOverlay overlay, CatalogIntegration integration, ExternalObjectIdentity identity) {
    Map<String, String> properties = new LinkedHashMap<>(ownershipProperties(overlay, integration));
    properties.put(EXTERNAL_ID_PROPERTY, identity.value());
    properties.put(EXTERNAL_ID_STABLE_PROPERTY, Boolean.toString(identity.stable()));
    return properties;
  }

  private static Optional<String> stableIdentity(Map<String, String> properties) {
    if (!Boolean.parseBoolean(properties.get(EXTERNAL_ID_STABLE_PROPERTY))) return Optional.empty();
    return Optional.ofNullable(properties.get(EXTERNAL_ID_PROPERTY))
        .filter(value -> !value.isBlank());
  }

  private static boolean ownedBy(Map<String, String> properties, CatalogOverlay overlay) {
    return overlay.getResourceId().getId().equals(properties.get(OVERLAY_ID_PROPERTY));
  }

  private static void requireUniqueStableIdentities(
      List<ExternalObjectIdentity> identities, String objectKind) {
    Set<String> seen = new HashSet<>();
    for (ExternalObjectIdentity identity : identities) {
      if (identity.stable() && !seen.add(identity.value())) {
        throw new IllegalStateException(
            "Catalog provider returned duplicate stable "
                + objectKind
                + " identity="
                + identity.value());
      }
    }
  }

  private static TableFormat tableFormat(String format) {
    return switch (format.trim().toUpperCase(Locale.ROOT)) {
      case "ICEBERG" -> TableFormat.TF_ICEBERG;
      case "DELTA" -> TableFormat.TF_DELTA;
      default -> throw new IllegalArgumentException("Unsupported upstream table format=" + format);
    };
  }

  private static ColumnIdAlgorithm columnIdAlgorithm(String format) {
    return switch (tableFormat(format)) {
      case TF_ICEBERG -> ColumnIdAlgorithm.CID_FIELD_ID;
      case TF_DELTA -> ColumnIdAlgorithm.CID_PATH_ORDINAL;
      default -> throw new IllegalArgumentException("Unsupported upstream table format=" + format);
    };
  }

  private static NamespacePath path(Namespace namespace) {
    List<String> path = new ArrayList<>(namespace.getParentsList());
    path.add(namespace.getDisplayName());
    return new NamespacePath(path);
  }

  private static void addAncestors(NamespacePath path, Set<NamespacePath> materialized) {
    for (int length = 1; length <= path.segments().size(); length++) {
      materialized.add(new NamespacePath(path.segments().subList(0, length)));
    }
  }

  /**
   * Whether a listing failure on one branch should be stepped over rather than fail the reconcile.
   *
   * <p>Two conditions. The failure has to describe the branch and not the catalog, which {@link
   * CatalogTraversalFailures#describesOneBranch} decides and {@code CatalogIntegrationDiscovery}
   * consults for the same question -- one rule, so a catalog that validates cannot then fail to
   * reconcile on a branch validation skipped.
   *
   * <p>And the operator has to not have asked for this branch. With no include filters the overlay
   * selects the whole tree by default, and an inaccessible corner of a workspace is not a reason to
   * refuse the rest of it. With include filters, a branch that could hold a selection is one the
   * operator named, so a denial there is reported rather than silently dropping what they asked
   * for. {@code descending} distinguishes the two questions: enumerating children of a branch that
   * may contain a selection, versus listing tables in a branch already selected.
   */
  private static boolean tolerateBranchFailure(
      CatalogOverlay overlay,
      NamespacePath path,
      CatalogAccessException failure,
      boolean descending) {
    if (!CatalogTraversalFailures.describesOneBranch(failure)) {
      return false;
    }
    // The root listing is not a branch, it is the whole tree. Tolerating it learns nothing about
    // anything: with no include filters the rule below would skip it, every path would then be
    // unobserved, and reconcile would return an all-zero result with no exception -- an integration
    // whose principal has lost listCatalogs would report healthy indefinitely while the overlay
    // drifts. A caller that cannot enumerate the catalog at all has to hear about it.
    if (descending && path.segments().isEmpty()) {
      return false;
    }
    if (overlay.getIncludeNamespacesCount() == 0) {
      return true;
    }
    // Named, not merely covered. Reusing selected() here conflated "the operator named an ancestor
    // of this branch" with "the operator named this branch", and since both non-descending call
    // sites already sit inside selected(), the test could never tolerate anything once filters
    // existed: adding --include main turned a stepped-over denial on main.system_schema into an
    // aborted reconcile for the whole overlay, every cycle, which is the shape the smoke uses.
    return !(descending
        ? mustDescendToReachASelection(overlay, path)
        : namedExactly(overlay, path));
  }

  /** Whether an include entry names this namespace itself. */
  private static boolean namedExactly(CatalogOverlay overlay, NamespacePath path) {
    return overlay.getIncludeNamespacesList().stream()
        .anyMatch(include -> include.getSegmentsList().equals(path.segments()));
  }

  /**
   * Whether something the operator named is at or below this namespace.
   *
   * <p>The question a failed {@code listNamespaces} asks: if an include sits underneath this
   * branch, the walk had to enumerate it to reach what was asked for, and a denial there means the
   * operator does not get what they selected. A branch merely descended from an include is not that
   * -- it is one corner of a subtree they asked for wholesale.
   */
  private static boolean mustDescendToReachASelection(CatalogOverlay overlay, NamespacePath path) {
    return overlay.getIncludeNamespacesList().stream()
        .anyMatch(include -> startsWith(include.getSegmentsList(), path.segments()));
  }

  private static boolean selected(CatalogOverlay overlay, NamespacePath path) {
    if (overlay.getIncludeNamespacesCount() == 0) return true;
    return overlay.getIncludeNamespacesList().stream()
        .anyMatch(include -> startsWith(path.segments(), include.getSegmentsList()));
  }

  private static boolean mayContainSelection(CatalogOverlay overlay, NamespacePath path) {
    if (overlay.getIncludeNamespacesCount() == 0 || selected(overlay, path)) return true;
    return overlay.getIncludeNamespacesList().stream()
        .anyMatch(include -> startsWith(include.getSegmentsList(), path.segments()));
  }

  private static boolean excluded(CatalogOverlay overlay, NamespacePath path) {
    return overlay.getExcludeNamespacesList().stream()
        .anyMatch(exclude -> startsWith(path.segments(), exclude.getSegmentsList()));
  }

  private static boolean startsWith(List<String> path, List<String> prefix) {
    return path.size() >= prefix.size() && path.subList(0, prefix.size()).equals(prefix);
  }

  private static NamespacePath normalizePath(NamespacePath path) {
    return new NamespacePath(normalizeSegments(path.segments()));
  }

  private static List<String> normalizeSegments(List<String> segments) {
    return segments.stream().map(BaseServiceImpl::normalizeName).toList();
  }

  private com.google.protobuf.Timestamp now() {
    return Timestamps.fromMillis(clock.millis());
  }

  private static ResourceId randomId(String accountId, ResourceKind kind) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(UUID.randomUUID().toString())
        .setKind(kind)
        .build();
  }

  @FunctionalInterface
  private interface Page<T> {
    List<T> load(String token, StringBuilder next);
  }

  /**
   * One pass over the upstream catalog.
   *
   * <p>The skipped and unobserved sets keep the snapshot honest. A relation absent from {@code
   * tables} or {@code views} is retired, which is correct only where the walk looked and found it
   * gone. Where a branch could not be enumerated or an object could not be loaded, absence means
   * nothing and retirement has to leave the local copy alone.
   *
   * <p>Per relation kind, because a failure in one says nothing about the other. A namespace
   * listing that fails hides both kinds beneath it; a table listing that fails hides only tables.
   */
  private record Discovery(
      Set<NamespacePath> materializedNamespaces,
      Map<CatalogObjectName, CatalogTable> tables,
      Map<CatalogObjectName, CatalogView> views,
      Set<NamespacePath> skippedPrefixes,
      Set<NamespacePath> skippedTablePrefixes,
      Set<NamespacePath> skippedViewPrefixes,
      Set<CatalogObjectName> unobservedTables,
      Set<CatalogObjectName> unobservedViews,
      Set<NamespacePath> tableIdentityUnknownIn,
      Set<NamespacePath> viewIdentityUnknownIn) {

    /** Whether the walk is in a position to say this table is gone upstream. */
    boolean observedTable(CatalogObjectName name) {
      return observedRelation(name, unobservedTables, skippedTablePrefixes);
    }

    /** Whether the walk is in a position to say this view is gone upstream. */
    boolean observedView(CatalogObjectName name) {
      return observedRelation(name, unobservedViews, skippedViewPrefixes);
    }

    private boolean observedRelation(
        CatalogObjectName name,
        Set<CatalogObjectName> kindUnobserved,
        Set<NamespacePath> kindNamespaces) {
      if (kindUnobserved.contains(name)) {
        return false;
      }
      return strictlyUnderNone(name.namespace(), skippedPrefixes)
          && notIn(name.namespace(), kindNamespaces);
    }

    /**
     * Whether the walk is in a position to say this namespace is gone upstream.
     *
     * <p>Both kinds, unlike the relation checks: a namespace is retired only once it holds neither
     * tables nor views, so either listing being blind makes that judgement unsafe.
     */
    boolean observedNamespace(NamespacePath path) {
      return strictlyUnderNone(path, skippedPrefixes)
          && notIn(path, skippedTablePrefixes)
          && notIn(path, skippedViewPrefixes);
    }

    /**
     * Whether a table in this namespace might be one whose identity a skipped load hid.
     *
     * <p>Scoped to the namespace the skip happened in and to its own relation kind. It covers a
     * rename within a namespace -- the ordinary case, and the one retirement would otherwise read
     * as a deletion. A relation moved to a different namespace in the same pass as a skipped load
     * at its destination is not covered: nothing in a listing correlates the two, and suppressing
     * overlay-wide to catch it stopped retirement everywhere, permanently, for one object that
     * never loads. Carrying stable identities in listing results is what would close it properly.
     */
    boolean tableIdentityMayBeHidden(NamespacePath namespace) {
      return tableIdentityUnknownIn.contains(namespace);
    }

    /** The same, for views. */
    boolean viewIdentityMayBeHidden(NamespacePath namespace) {
      return viewIdentityUnknownIn.contains(namespace);
    }

    /**
     * Exact: a kind set records that *this* namespace's own relations were not listed.
     *
     * <p>Not a prefix match, which is what the wording always meant and the code did not do. A
     * failed {@code listTables(N)} no longer stops the walk descending -- views in N are still
     * listed and N is still enqueued -- so N's descendants are visited and their own relations
     * genuinely observed. Matching by prefix suppressed all of them, which froze retirement for the
     * whole subtree for as long as N's table listing kept failing: the unbounded suppression that
     * scoping {@code tableIdentityUnknownIn} per namespace was meant to end.
     */
    private static boolean notIn(NamespacePath path, Set<NamespacePath> namespaces) {
      return !namespaces.contains(path);
    }

    /**
     * Strict: {@code skippedPrefixes} records a namespace whose {@code listNamespaces} was denied.
     *
     * <p>What that hides is the namespace's children, not itself. Its own relations were listed in
     * the earlier iteration where it appeared as a child and was selected, so treating the match as
     * inclusive excluded relations the walk had actually seen -- freezing their retirement for as
     * long as the denial recurred. The two kind sets say "this namespace's own relations were not
     * listed" and stay inclusive; this one does not.
     */
    private static boolean strictlyUnderNone(NamespacePath path, Set<NamespacePath> prefixes) {
      return prefixes.stream()
          .noneMatch(
              prefix ->
                  path.segments().size() > prefix.segments().size()
                      && startsWith(path.segments(), prefix.segments()));
    }
  }

  public record Result(
      int namespacesCreated,
      int namespacesDeleted,
      int tablesCreated,
      int tablesUpdated,
      int tablesDeleted,
      int viewsCreated,
      int viewsUpdated,
      int viewsDeleted,
      /**
       * Branches the walk could not enumerate.
       *
       * <p>Non-zero means the reconcile was partial: everything under those branches was excluded
       * from both materialization and retirement, so the overlay may be missing new upstream
       * objects. The other counters cannot express this -- a partial pass and a complete pass with
       * nothing to do both report zeros.
       */
      int branchesSkipped,
      /** Individual objects that listed but would not load, excluded for the same reason. */
      int objectsSkipped) {

    /** A complete pass, for callers that construct an expected result rather than a partial one. */
    public Result(
        int namespacesCreated,
        int namespacesDeleted,
        int tablesCreated,
        int tablesUpdated,
        int tablesDeleted,
        int viewsCreated,
        int viewsUpdated,
        int viewsDeleted) {
      this(
          namespacesCreated,
          namespacesDeleted,
          tablesCreated,
          tablesUpdated,
          tablesDeleted,
          viewsCreated,
          viewsUpdated,
          viewsDeleted,
          0,
          0);
    }
  }

  private static final class MutableResult {
    private int namespacesCreated;
    private int namespacesDeleted;
    private int tablesCreated;
    private int tablesUpdated;
    private int tablesDeleted;
    private int viewsCreated;
    private int viewsUpdated;
    private int viewsDeleted;
    private int branchesSkipped;
    private int objectsSkipped;

    private Result freeze() {
      return new Result(
          namespacesCreated,
          namespacesDeleted,
          tablesCreated,
          tablesUpdated,
          tablesDeleted,
          viewsCreated,
          viewsUpdated,
          viewsDeleted,
          branchesSkipped,
          objectsSkipped);
    }
  }
}
