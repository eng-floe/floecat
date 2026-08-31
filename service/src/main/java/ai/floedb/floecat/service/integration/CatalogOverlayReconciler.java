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

import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
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

/** Materializes one Catalog Overlay's selected upstream metadata into its target catalog. */
@ApplicationScoped
public class CatalogOverlayReconciler {
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
    retireStaleRelations(
        overlay, fence, discovery.tables(), discovery.views(), localNamespaces, result);
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
    retireNamespaces(overlay, fence, discovery.materializedNamespaces(), localNamespaces, result);
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
    Set<NamespacePath> seen = new HashSet<>();
    var pending = new ArrayDeque<NamespacePath>();
    pending.add(NamespacePath.root());
    while (!pending.isEmpty()) {
      NamespacePath parent = pending.removeFirst();
      for (NamespacePath path : client.listNamespaces(parent).stream().sorted().toList()) {
        if (!seen.add(path)) continue;
        if (seen.size() > MAX_NAMESPACES) {
          throw new IllegalStateException("Catalog namespace inventory exceeds " + MAX_NAMESPACES);
        }
        NamespacePath normalizedPath = normalizePath(path);
        if (excluded(overlay, normalizedPath)) continue;
        if (selected(overlay, normalizedPath)) {
          addAncestors(normalizedPath, materialized);
          for (CatalogObjectName name : client.listTables(path).stream().sorted().toList()) {
            CatalogTable table = client.loadTable(name);
            if (!name.equals(table.name())) {
              throw new IllegalStateException(
                  "Catalog provider returned table metadata for the wrong object: expected="
                      + name
                      + " actual="
                      + table.name());
            }
            CatalogObjectName localName =
                new CatalogObjectName(normalizedPath, normalizeName(name.name()));
            if (discoveredTables.putIfAbsent(localName, table) != null) {
              throw new IllegalStateException(
                  "Upstream table names collide after normalization: " + localName);
            }
          }
          if (listViews) {
            for (CatalogObjectName name : client.listViews(path).stream().sorted().toList()) {
              CatalogObjectName localName =
                  new CatalogObjectName(normalizedPath, normalizeName(name.name()));
              if (discoveredTables.containsKey(localName)) {
                throw new IllegalStateException(
                    "Upstream relation name is both table and view: " + localName);
              }
              CatalogView view = client.loadView(name);
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
        Set.copyOf(materialized), Map.copyOf(discoveredTables), Map.copyOf(discoveredViews));
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

  private void retireStaleRelations(
      CatalogOverlay overlay,
      PointerConditions fence,
      Map<CatalogObjectName, CatalogTable> discoveredTables,
      Map<CatalogObjectName, CatalogView> discoveredViews,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
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

    for (Table stale : tablesByName.values()) {
      if (retainedTableIds.contains(stale.getResourceId().getId())) continue;
      MutationMeta meta = tables.metaFor(stale.getResourceId());
      if (!tables.deleteWhilePointersMatch(
          stale.getResourceId(), meta.getPointerVersion(), fence)) {
        throw lostFence();
      }
      purgeTableState(stale.getResourceId());
      relationChanged(stale.getResourceId(), stale.getNamespaceId());
      result.tablesDeleted++;
    }
    for (View stale : viewsByName.values()) {
      if (retainedViewIds.contains(stale.getResourceId().getId())) continue;
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
        // destination's relation fence exactly as a create does. Both ids when they differ; the
        // fence collapses to one marker when they do not.
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

  private void retireNamespaces(
      CatalogOverlay overlay,
      PointerConditions fence,
      Set<NamespacePath> targetPaths,
      Map<NamespacePath, Namespace> localNamespaces,
      MutableResult result) {
    List<Map.Entry<NamespacePath, Namespace>> stale =
        localNamespaces.entrySet().stream()
            .filter(entry -> !targetPaths.contains(entry.getKey()))
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

  private record Discovery(
      Set<NamespacePath> materializedNamespaces,
      Map<CatalogObjectName, CatalogTable> tables,
      Map<CatalogObjectName, CatalogView> views) {}

  public record Result(
      int namespacesCreated,
      int namespacesDeleted,
      int tablesCreated,
      int tablesUpdated,
      int tablesDeleted,
      int viewsCreated,
      int viewsUpdated,
      int viewsDeleted) {}

  private static final class MutableResult {
    private int namespacesCreated;
    private int namespacesDeleted;
    private int tablesCreated;
    private int tablesUpdated;
    private int tablesDeleted;
    private int viewsCreated;
    private int viewsUpdated;
    private int viewsDeleted;

    private Result freeze() {
      return new Result(
          namespacesCreated,
          namespacesDeleted,
          tablesCreated,
          tablesUpdated,
          tablesDeleted,
          viewsCreated,
          viewsUpdated,
          viewsDeleted);
    }
  }
}
