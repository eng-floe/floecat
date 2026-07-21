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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.eclipse.microprofile.context.ManagedExecutor;

/**
 * Repository-backed name resolution helpers.
 *
 * <p>This class provides the *atomic* operations for: - catalog lookup - namespace lookup - table
 * lookup - view lookup
 *
 * <p>MetadataGraph and FullyQualifiedResolver delegate to this class so the main façade stays
 * focused on caching and orchestration.
 *
 * <p>No caching — pure repository calls.
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

  /** Max concurrent DynamoDB namespace scans per top-level listing call. */
  static final int MAX_PARALLEL_NS_SCANS = 8;

  private final CatalogRepository catalogRepository;
  private final NamespaceRepository namespaceRepository;
  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;
  @Inject Observability observability;
  private volatile Executor blockingExecutor = ForkJoinPool.commonPool();

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

  @Inject
  void init(Instance<ManagedExecutor> managedExecutors) {
    if (managedExecutors != null) {
      managedExecutors.stream().findFirst().ifPresent(e -> blockingExecutor = e);
    }
  }

  // ----------------------------------------------------------------------
  // Weak resolution (Optional)
  // ----------------------------------------------------------------------

  public Optional<ResourceId> resolveCatalogId(String cid, String accountId, String catalogName) {
    return diagnose(
        "resolve_catalog_id",
        cid,
        accountId,
        () -> catalogByName(accountId, catalogName).map(Catalog::getResourceId));
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
              .map(Namespace::getResourceId);
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
                                        .getByName(
                                            accountId,
                                            catalog.getResourceId().getId(),
                                            ns.getResourceId().getId(),
                                            ref.getName())
                                        .map(Table::getResourceId)
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
                                        .getByName(
                                            accountId,
                                            catalog.getResourceId().getId(),
                                            ns.getResourceId().getId(),
                                            ref.getName())
                                        .map(View::getResourceId))));
  }

  /**
   * Kind-agnostic name resolution: resolves catalog and namespace once, then probes tables and (on
   * miss) views. Unlike resolving the table and the view separately, this does not re-resolve the
   * catalog and namespace for the view probe. A relation name is unique across kinds (enforced by
   * the shared relation-name claim), so the table-first order is deterministic.
   */
  public Optional<ResourceId> resolveRelationId(String accountId, NameRef ref) {
    return resolveRelationId(accountId, ref, newScopeMemo(accountId));
  }

  /**
   * Batch variant of {@link #resolveRelationId}: names sharing a catalog/namespace resolve their
   * scope once per call instead of once per name.
   */
  public Map<NameRef, Optional<ResourceId>> resolveRelationIds(
      String accountId, List<NameRef> refs) {
    ScopeMemo memo = newScopeMemo(accountId);
    var out = new LinkedHashMap<NameRef, Optional<ResourceId>>(refs.size());
    for (NameRef ref : refs) {
      out.computeIfAbsent(ref, r -> resolveRelationId(accountId, r, memo));
    }
    return out;
  }

  private ScopeMemo newScopeMemo(String accountId) {
    return new ScopeMemo(
        name -> catalogByName(accountId, name),
        (catalog, path) -> namespaceByPath(accountId, catalog, path));
  }

  private Optional<ResourceId> resolveRelationId(String accountId, NameRef ref, ScopeMemo memo) {
    return diagnose(
        "resolve_relation_id",
        "",
        accountId,
        () -> {
          // Name validity is per-ref; a blank name must not consult the shared scope memo (keyed by
          // catalog + path, name-independent) or it would starve valid siblings in this batch.
          if (!validName(ref) || !validCatalog(ref)) {
            return Optional.<ResourceId>empty();
          }
          return memo.catalog(ref.getCatalog())
              .flatMap(
                  catalog ->
                      memo.namespace(catalog, ref.getPathList())
                          .flatMap(
                              ns -> {
                                String catalogId = catalog.getResourceId().getId();
                                String namespaceId = ns.getResourceId().getId();
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
                                        .getByName(accountId, catalogId, namespaceId, ref.getName())
                                        .map(Table::getResourceId)
                                        .map(this::requireCanonicalTableId);
                                if (table.isPresent()) {
                                  return table;
                                }
                                return viewRepository
                                    .getByName(accountId, catalogId, namespaceId, ref.getName())
                                    .map(View::getResourceId);
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
                          .getByName(
                              accountId,
                              scope.catalog().getResourceId().getId(),
                              scope.namespace().getResourceId().getId(),
                              ref.getName())
                          .map(
                              t -> {
                                ResourceId tableId = requireCanonicalTableId(t.getResourceId());
                                return new ResolvedRelation(
                                    tableId,
                                    canonicalName(
                                        scope.catalog(),
                                        scope.namespace(),
                                        t.getDisplayName(),
                                        tableId));
                              }));
        });
  }

  // ----------------------------------------------------------------------
  // Repository helpers
  // ----------------------------------------------------------------------

  /** A validated (catalog, namespace) pair a relation name resolves within. */
  private record Scope(Catalog catalog, Namespace namespace) {}

  /**
   * Resolves the catalog and namespace a relation name lives in. The scope depends only on the
   * catalog and path, not the relation name — so callers that memoize by {@code scopeKey} (catalog
   * + path) must gate on {@link #validName} themselves rather than have a blank-name ref poison the
   * shared scope entry. Shared by the relation resolution methods so the lookup is written once.
   */
  private Optional<Scope> resolveScope(String accountId, NameRef ref) {
    if (!validCatalog(ref)) {
      return Optional.empty();
    }
    Optional<Catalog> catalogOpt = catalogRepository.getByName(accountId, ref.getCatalog());
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    Catalog catalog = catalogOpt.get();
    return namespaceRepository
        .getByPath(accountId, catalog.getResourceId().getId(), ref.getPathList())
        .map(ns -> new Scope(catalog, ns));
  }

  private Optional<Catalog> catalogByName(String accountId, String name) {
    return catalogRepository.getByName(accountId, name);
  }

  private Optional<Namespace> namespaceByPath(
      String accountId, Catalog catalog, List<String> parents) {

    return namespaceRepository.getByPath(accountId, catalog.getResourceId().getId(), parents);
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
  private NameRef canonicalName(Catalog catalog, Namespace ns, String displayName, ResourceId id) {

    List<String> path = new ArrayList<>(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) {
      path.add(ns.getDisplayName());
    }

    return NameRef.newBuilder()
        .setCatalog(catalog.getDisplayName())
        .addAllPath(path)
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

  public List<ResourceId> listNamespaces(String accountId, String catalogId) {
    return diagnose(
        "list_namespaces", "", accountId, () -> namespaceRepository.listIds(accountId, catalogId));
  }

  public List<ResourceId> listTableIds(String accountId, String catalogId) {
    return diagnose(
        "list_table_ids",
        "",
        accountId,
        () -> {
          List<ResourceId> nsIds = namespaceRepository.listIds(accountId, catalogId);
          if (nsIds.isEmpty()) return List.of();
          if (nsIds.size() == 1) {
            return listTableIdsInNamespace(accountId, catalogId, nsIds.get(0).getId());
          }
          return parallelScan(
              nsIds, ns -> listTableIdsInNamespace(accountId, catalogId, ns.getId()));
        });
  }

  public List<ResourceId> listTableIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    return diagnose(
        "list_table_ids_in_namespace",
        "",
        accountId,
        () -> {
          List<Table> tables =
              tableRepository.list(
                  accountId, catalogId, namespaceId, Integer.MAX_VALUE, "", new StringBuilder());
          return tables.stream()
              .map(Table::getResourceId)
              .map(this::requireCanonicalTableId)
              .toList();
        });
  }

  /**
   * Fans out per-namespace work across up to {@value #MAX_PARALLEL_NS_SCANS} concurrent tasks on
   * the injected blocking executor. Each namespace is an independent DynamoDB scan; parallel
   * execution reduces wall-clock time from O(N) to O(1) for warm DynamoDB connections.
   */
  private <T> List<T> parallelScan(
      List<ResourceId> nsIds, java.util.function.Function<ResourceId, List<T>> task) {
    Semaphore gate = new Semaphore(MAX_PARALLEL_NS_SCANS);
    io.opentelemetry.context.Context otelContext = io.opentelemetry.context.Context.current();
    List<CompletableFuture<List<T>>> futures =
        nsIds.stream()
            .<CompletableFuture<List<T>>>map(
                ns ->
                    CompletableFuture.<List<T>>supplyAsync(
                        () -> {
                          gate.acquireUninterruptibly();
                          try (io.opentelemetry.context.Scope ignored = otelContext.makeCurrent()) {
                            return task.apply(ns);
                          } finally {
                            gate.release();
                          }
                        },
                        blockingExecutor))
            .toList();
    List<T> out = new ArrayList<>();
    for (CompletableFuture<List<T>> f : futures) {
      try {
        out.addAll(f.join());
      } catch (java.util.concurrent.CompletionException ce) {
        Throwable cause = ce.getCause();
        if (cause instanceof RuntimeException re) throw re;
        if (cause instanceof Error e) throw e;
        throw new IllegalStateException("unexpected checked exception from async task", cause);
      }
    }
    return out;
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
          List<ResourceId> nsIds = namespaceRepository.listIds(accountId, catalogId);
          if (nsIds.isEmpty()) return List.of();
          if (nsIds.size() == 1) {
            return listViewIdsInNamespace(accountId, catalogId, nsIds.get(0).getId());
          }
          return parallelScan(
              nsIds, ns -> listViewIdsInNamespace(accountId, catalogId, ns.getId()));
        });
  }

  public List<ResourceId> listViewIdsInNamespace(
      String accountId, String catalogId, String namespaceId) {
    return diagnose(
        "list_view_ids_in_namespace",
        "",
        accountId,
        () -> {
          List<View> views =
              viewRepository.list(
                  accountId, catalogId, namespaceId, Integer.MAX_VALUE, "", new StringBuilder());
          return views.stream().map(View::getResourceId).toList();
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
