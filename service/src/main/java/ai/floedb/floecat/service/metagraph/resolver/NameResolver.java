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

package ai.floedb.floecat.service.metagraph.resolver;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
import ai.floedb.floecat.service.context.PropagatedContext;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository.CatalogRef;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Repository-backed name resolution helpers.
 *
 * <p>This class provides the *atomic* operations for: - catalog lookup - namespace lookup - table
 * lookup - view lookup
 *
 * <p>MetadataGraph and FullyQualifiedResolver delegate to this class so the main façade stays
 * focused on caching and orchestration.
 *
 * <p>No cache of its own — repository calls inherit the pointer cache below the store seam.
 */
@ApplicationScoped
public final class NameResolver {

  // ----------------------------------------------------------------------
  // Result wrapper for resolved relations
  // ----------------------------------------------------------------------

  public record ResolvedRelation(ResourceId resourceId, NameRef canonicalName) {}

  // ----------------------------------------------------------------------
  // Dependencies
  // ----------------------------------------------------------------------

  /** Max concurrent DynamoDB namespace scans per top-level listing call (tier-1 fan-out width). */
  static final int MAX_PARALLEL_NS_SCANS = 8;

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;
  @Inject Observability observability;

  @Inject
  public NameResolver(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {
    this.catalogRepository = catalogRepository;
    this.namespaceRepository = namespaceRepository;
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
  }

  // ----------------------------------------------------------------------
  // Weak resolution (Optional)
  // ----------------------------------------------------------------------

  public Optional<ResourceId> resolveCatalogId(String cid, String accountId, String catalogName) {
    return diagnose(
        "resolve_catalog_id",
        cid,
        accountId,
        () -> catalogByName(accountId, catalogName).map(CatalogRef::id));
  }

  public Optional<ResourceId> resolveNamespaceId(String cid, String accountId, NameRef ref) {
    return diagnose(
        "resolve_namespace_id",
        cid,
        accountId,
        () -> {
          List<String> fullPath = namespacePath(ref);

          return catalogByName(accountId, ref.getCatalog())
              .flatMap(catalog -> namespaceByPath(accountId, catalog, fullPath))
              .map(NamespaceRef::id);
        });
  }

  public Optional<ResourceId> resolveTableId(String cid, String accountId, NameRef ref) {
    return diagnose(
        "resolve_table_id",
        cid,
        accountId,
        () ->
            catalogByName(accountId, ref.getCatalog())
                .flatMap(
                    catalog ->
                        namespaceByPath(accountId, catalog, ref.getPathList())
                            .flatMap(
                                ns ->
                                    tableRepository
                                        .getRefByName(
                                            accountId,
                                            catalog.id().getId(),
                                            ns.id().getId(),
                                            ref.getName())
                                        .map(RelationRef::id)
                                        .map(this::requireCanonicalTableId))));
  }

  public Optional<ResourceId> resolveViewId(String cid, String accountId, NameRef ref) {
    return diagnose(
        "resolve_view_id",
        cid,
        accountId,
        () ->
            catalogByName(accountId, ref.getCatalog())
                .flatMap(
                    catalog ->
                        namespaceByPath(accountId, catalog, ref.getPathList())
                            .flatMap(
                                ns ->
                                    viewRepository
                                        .getRefByName(
                                            accountId,
                                            catalog.id().getId(),
                                            ns.id().getId(),
                                            ref.getName())
                                        .map(RelationRef::id))));
  }

  /**
   * Kind-agnostic name resolution: resolves catalog and namespace once, then probes tables and (on
   * miss) views. Unlike resolving the table and the view separately, this does not re-resolve the
   * catalog and namespace for the view probe. A relation name is unique across kinds (enforced by
   * the shared relation-name claim), so the table-first order is deterministic.
   */
  public Optional<ResourceId> resolveRelationId(String accountId, NameRef ref) {
    return resolveRelation(accountId, ref);
  }

  /** Batch variant of {@link #resolveRelationId}; duplicate names are resolved once per call. */
  public Map<NameRef, Optional<ResourceId>> resolveRelationIds(
      String accountId, List<NameRef> refs) {
    var out = new LinkedHashMap<NameRef, Optional<ResourceId>>(refs.size());
    for (NameRef ref : refs) {
      out.computeIfAbsent(ref, r -> resolveRelation(accountId, r));
    }
    return out;
  }

  private Optional<ResourceId> resolveRelation(String accountId, NameRef ref) {
    return diagnose(
        "resolve_relation_id",
        "",
        accountId,
        () -> {
          if (!validName(ref) || !validCatalog(ref)) {
            return Optional.<ResourceId>empty();
          }
          return catalogByName(accountId, ref.getCatalog())
              .flatMap(
                  catalog ->
                      namespaceByPath(accountId, catalog, ref.getPathList())
                          .flatMap(
                              ns -> {
                                String catalogId = catalog.id().getId();
                                String namespaceId = ns.id().getId();
                                // One pointer read (no blob fetch) answers both kind and id via the
                                // shared relation-name claim written by every create and rename.
                                Optional<ResourceId> claimed =
                                    tableRepository.relationNameClaim(
                                        accountId, catalogId, namespaceId, ref.getName());
                                if (claimed.isPresent()) {
                                  ResourceId rid = claimed.get();
                                  return Optional.of(
                                      rid.getKind() == ResourceKind.RK_TABLE
                                          ? requireCanonicalTableId(rid)
                                          : rid);
                                }
                                // Claimless rows (pre-claim): kind-specific probes.
                                Optional<ResourceId> table =
                                    tableRepository
                                        .getRefByName(
                                            accountId, catalogId, namespaceId, ref.getName())
                                        .map(RelationRef::id)
                                        .map(this::requireCanonicalTableId);
                                if (table.isPresent()) {
                                  return table;
                                }
                                return viewRepository
                                    .getRefByName(accountId, catalogId, namespaceId, ref.getName())
                                    .map(RelationRef::id);
                              }));
        });
  }

  public Optional<ResolvedRelation> resolveTableRelation(String accountId, NameRef ref) {
    return diagnose(
        "resolve_table_relation",
        "",
        accountId,
        () -> {
          if (!validName(ref)) {
            return Optional.<ResolvedRelation>empty();
          }
          return resolveScope(accountId, ref)
              .flatMap(
                  scope ->
                      tableRepository
                          .getRefByName(
                              accountId,
                              scope.catalog().id().getId(),
                              scope.namespace().id().getId(),
                              ref.getName())
                          .map(
                              table -> {
                                ResourceId tableId = requireCanonicalTableId(table.id());
                                return new ResolvedRelation(
                                    tableId,
                                    canonicalName(
                                        scope.catalog(), scope.namespace(), table.name(), tableId));
                              }));
        });
  }

  // ----------------------------------------------------------------------
  // Repository helpers
  // ----------------------------------------------------------------------

  /** A validated (catalog, namespace) pair a relation name resolves within. */
  private record Scope(CatalogRef catalog, NamespaceRef namespace) {}

  /**
   * Resolves the catalog and namespace a relation name lives in. Shared by the relation resolution
   * methods so the lookup is written once.
   */
  private Optional<Scope> resolveScope(String accountId, NameRef ref) {
    if (!validCatalog(ref)) {
      return Optional.empty();
    }
    Optional<CatalogRef> catalogOpt = catalogRepository.getRefByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    CatalogRef catalog = catalogOpt.get();
    return namespaceRepository
        .getRefByPath(accountId, catalog.id().getId(), ref.getPathList())
        .map(ns -> new Scope(catalog, ns));
  }

  private Optional<CatalogRef> catalogByName(String accountId, String name) {
    return catalogRepository.getRefByName(accountId, name);
  }

  private Optional<NamespaceRef> namespaceByPath(
      String accountId, CatalogRef catalog, List<String> parents) {

    return namespaceRepository.getRefByPath(accountId, catalog.id().getId(), parents);
  }

  // ----------------------------------------------------------------------
  // Path helpers
  // ----------------------------------------------------------------------

  private List<String> namespacePath(NameRef ref) {
    List<String> out = new ArrayList<>(ref.getPathList());
    if (ref.getName() != null && !ref.getName().isBlank()) {
      if (out.isEmpty() || !out.get(out.size() - 1).equals(ref.getName())) {
        out.add(ref.getName());
      }
    }
    return out;
  }

  // canonical: namespace parents + its own display name
  private NameRef canonicalName(
      CatalogRef catalog, NamespaceRef namespace, String displayName, ResourceId id) {
    return NameRef.newBuilder()
        .setCatalog(catalog.name())
        .addAllPath(namespace.pathSegments())
        .setName(displayName)
        .setResourceId(id)
        .build();
  }

  // ----------------------------------------------------------------------
  // Validation helpers
  // ----------------------------------------------------------------------

  private boolean validCatalog(NameRef ref) {
    return ref.getCatalog() != null && !ref.getCatalog().isBlank();
  }

  private boolean validName(NameRef ref) {
    return ref.getName() != null && !ref.getName().isBlank();
  }

  // ----------------------------------------------------------------------
  // Listing helpers
  // ----------------------------------------------------------------------

  /** Lists lightweight namespace refs from pointers, without materializing namespace blobs. */
  public List<TopologyGraph.NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    return namespaceRepository.listRefs(catalogId.getAccountId(), catalogId.getId());
  }

  /** Resolves selected namespace names by exact pointer lookup. */
  public List<TopologyGraph.NamespaceRef> listNamespaceRefsByName(
      ResourceId catalogId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    return namespaceRepository.listRefsByName(catalogId.getAccountId(), catalogId.getId(), names);
  }

  /** Lists lightweight table and view refs from the complete pointer index. */
  public List<TopologyGraph.RelationRef> listRelationRefs(
      ResourceId catalogId, ResourceId namespaceId) {
    List<TopologyGraph.RelationRef> refs = new ArrayList<>();
    refs.addAll(
        tableRepository.listRefs(catalogId.getAccountId(), catalogId.getId(), namespaceId.getId()));
    refs.addAll(
        viewRepository.listRefs(catalogId.getAccountId(), catalogId.getId(), namespaceId.getId()));
    return List.copyOf(refs);
  }

  /** Resolves selected relation names by exact table and view pointer lookups. */
  public List<TopologyGraph.RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    List<TopologyGraph.RelationRef> refs = new ArrayList<>();
    refs.addAll(
        tableRepository.listRefsByName(
            catalogId.getAccountId(), catalogId.getId(), namespaceId.getId(), names));
    refs.addAll(
        viewRepository.listRefsByName(
            catalogId.getAccountId(), catalogId.getId(), namespaceId.getId(), names));
    return List.copyOf(refs);
  }

  public List<ResourceId> listNamespaces(String accountId, String catalogId) {
    return diagnose(
        "list_namespaces",
        "",
        accountId,
        () ->
            namespaceRepository.listRefs(accountId, catalogId).stream()
                .map(NamespaceRef::id)
                .toList());
  }

  public List<ResourceId> listTableIds(String accountId, String catalogId) {
    return diagnose(
        "list_table_ids",
        "",
        accountId,
        () -> {
          List<NamespaceRef> namespaces = namespaceRepository.listRefs(accountId, catalogId);
          if (namespaces.isEmpty()) return List.of();
          if (namespaces.size() == 1) {
            return listTableIdsInNamespace(
                accountId, catalogId, namespaces.getFirst().id().getId());
          }
          return parallelScan(
              namespaces, ns -> listTableIdsInNamespace(accountId, catalogId, ns.id().getId()));
        });
  }

  public List<ResourceId> listTableIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    return diagnose(
        "list_table_ids_in_namespace",
        "",
        accountId,
        () -> {
          return tableRepository.listRefs(accountId, catalogId, namespaceId).stream()
              .map(RelationRef::id)
              .map(this::requireCanonicalTableId)
              .toList();
        });
  }

  /**
   * Fans out per-namespace work across up to {@value #MAX_PARALLEL_NS_SCANS} concurrent tasks and
   * flattens the per-namespace results. Each namespace is an independent DynamoDB scan; warm
   * connections complete in bounded waves instead of one serial scan per namespace. The scans run
   * through {@link MetadataFanout} as the per-caller (tier-1) bound; each scan's repository read is
   * held under the process-wide (tier-2) ceiling automatically at the store, so aggregate load
   * stays bounded without this fan-out wiring any admission itself.
   */
  private <T, N> List<T> parallelScan(
      List<N> namespaces, java.util.function.Function<N, List<T>> task) {
    BooleanSupplier cancelled = PropagatedContext.currentCancellation();
    MetadataFanout fanout = MetadataFanout.concurrent(MAX_PARALLEL_NS_SCANS);
    List<List<T>> results =
        cancelled == null
            ? fanout.mapOrdered(namespaces, task)
            : fanout.mapOrdered(namespaces, task, cancelled);
    return results.stream().flatMap(List::stream).toList();
  }

  private ResourceId requireCanonicalTableId(ResourceId tableId) {
    if (tableId == null
        || tableId.getId().isBlank()
        || tableId.getAccountId().isBlank()
        || tableId.getKind() != ResourceKind.RK_TABLE) {
      throw new IllegalStateException("non-canonical table resource id in resolver");
    }
    return tableId;
  }

  public List<ResourceId> listViewIds(String accountId, String catalogId) {
    return diagnose(
        "list_view_ids",
        "",
        accountId,
        () -> {
          List<NamespaceRef> namespaces = namespaceRepository.listRefs(accountId, catalogId);
          if (namespaces.isEmpty()) return List.of();
          if (namespaces.size() == 1) {
            return listViewIdsInNamespace(accountId, catalogId, namespaces.getFirst().id().getId());
          }
          return parallelScan(
              namespaces, ns -> listViewIdsInNamespace(accountId, catalogId, ns.id().getId()));
        });
  }

  public List<ResourceId> listViewIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    return diagnose(
        "list_view_ids_in_namespace",
        "",
        accountId,
        () -> {
          return viewRepository.listRefs(accountId, catalogId, namespaceId).stream()
              .map(RelationRef::id)
              .toList();
        });
  }

  private <T> T diagnose(
      String operation, String correlationId, String accountId, Supplier<T> work) {
    PhaseDiagnostics diagnostics =
        observability == null
            ? PhaseDiagnostics.NOOP
            : observability.diagnostics("metagraph", operation);
    long startedNanos = System.nanoTime();
    diagnostics.put("correlation_id", correlationId == null ? "" : correlationId);
    diagnostics.put("account_id", accountId == null ? "" : accountId);
    try {
      T result = work.get();
      diagnostics.put("outcome", "completed");
      recordResult(diagnostics, result);
      return result;
    } catch (RuntimeException | Error e) {
      diagnostics.put("outcome", "failed");
      diagnostics.put("error", e.getClass().getSimpleName());
      throw e;
    } finally {
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.emit("floecat.metagraph.name_resolver.summary");
    }
  }

  private static void recordResult(PhaseDiagnostics diagnostics, Object result) {
    if (result instanceof Optional<?> optional) {
      diagnostics.put("found", optional.isPresent());
      return;
    }
    if (result instanceof List<?> list) {
      diagnostics.put("result_count", list.size());
    }
  }
}
