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

package ai.floedb.floecat.service.metagraph.overlay;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.scanner.spi.TopologyNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.common.PageTokens;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.cache.CatalogTopologyCache;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.systemcatalog.graph.SystemCatalogTranslator;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

@ApplicationScoped
public final class MetaGraph implements CatalogOverlay, TopologyGraph {

  private static final Logger LOG = Logger.getLogger(MetaGraph.class);

  private final UserGraph userGraph;
  private final LogicalSchemaMapper schemaMapper;
  private final SystemGraph systemGraph;
  private final EngineContextProvider engine;
  private final CatalogTopologyCache topologyCache;

  @Inject
  public MetaGraph(
      UserGraph userGraph,
      LogicalSchemaMapper schemaMapper,
      SystemGraph systemGraph,
      EngineContextProvider engine,
      CatalogTopologyCache topologyCache) {
    this.userGraph = userGraph;
    this.schemaMapper = schemaMapper;
    this.systemGraph = systemGraph;
    this.engine = engine;
    this.topologyCache = topologyCache;
  }

  private EngineContext engineContext() {
    return engine.isPresent() ? engine.engineContext() : EngineContext.empty();
  }

  private static EngineContext orRequestEngine(
      EngineContext ctx, Supplier<EngineContext> fallback) {
    return ctx != null ? ctx : fallback.get();
  }

  /**
   * Resolves a graph node by ID, checking system graph first, then user graph.
   *
   * <p>System objects take precedence over user objects with the same ID, ensuring built-in catalog
   * objects like pg_class are always visible.
   *
   * @param id the resource ID to resolve
   * @return the resolved graph node, or empty if not found
   */
  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return resolve(id, engineContext());
  }

  @Override
  public Optional<GraphNode> resolve(ResourceId id, EngineContext engineContext) {
    EngineContext ctx = orRequestEngine(engineContext, this::engineContext);
    Optional<GraphNode> sys = systemGraph.resolve(id, ctx);
    if (sys.isPresent()) {
      return sys;
    }
    return userGraph.resolve(id);
  }

  /**
   * Lists all relations (tables and views) in the specified catalog.
   *
   * <p>Merges system relations and user relations unconditionally, ensuring system objects are
   * globally visible alongside user objects.
   *
   * @param catalogId the catalog to list relations from
   * @return list of all relations in the catalog
   */
  @Override
  public List<RelationNode> listRelations(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listRelations(catalogId, ctx),
        () -> listUserRelationsFromTopology(catalogId));
  }

  /**
   * Lists all relations (tables and views) in the specified namespace.
   *
   * <p>Merges system relations and user relations unconditionally, ensuring system objects are
   * globally visible alongside user objects.
   *
   * @param catalogId the catalog containing the namespace
   * @param namespaceId the namespace to list relations from
   * @return list of all relations in the namespace
   */
  @Override
  public List<RelationNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listRelationsInNamespace(catalogId, namespaceId, ctx),
        () -> listUserRelationsFromTopology(catalogId, namespaceId));
  }

  /**
   * Lists all functions in the specified namespace.
   *
   * <p>Merges system functions and user functions unconditionally.
   *
   * @param catalogId the catalog containing the namespace (unused for functions)
   * @param namespaceId the namespace to list functions from
   * @return list of all functions in the namespace
   */
  @Override
  public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listFunctions(namespaceId, ctx),
        () -> userGraph.listFunctions(namespaceId));
  }

  /**
   * Lists all types in the specified catalog.
   *
   * <p>Merges system types and user types unconditionally.
   *
   * @param catalogId the catalog to list types from
   * @return list of all types in the catalog
   */
  @Override
  public List<TypeNode> listTypes(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listTypes(catalogId, ctx), () -> userGraph.listTypes(catalogId));
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    return mergeLists(
        () -> systemGraph.listNamespaces(catalogId, ctx),
        () -> listUserNamespacesFromTopology(catalogId));
  }

  @Override
  public List<RelationNode> listSystemRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId) {
    EngineContext ctx = engineContext();
    return systemGraph.listRelationsInNamespace(catalogId, namespaceId, ctx).stream()
        .map(RelationNode.class::cast)
        .toList();
  }

  @Override
  public List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    return systemGraph.listNamespaces(catalogId, ctx).stream()
        .map(NamespaceNode.class::cast)
        .toList();
  }

  /**
   * Resolves a catalog by name.
   *
   * <p>Catalog resolution is user-first, with a system fallback alias equal to the current engine
   * kind.
   *
   * @param correlationId correlation ID for error reporting
   * @param name the catalog name to resolve
   * @return the resolved catalog resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveCatalog(String correlationId, String name) {
    Optional<ResourceId> user = userGraph.resolveCatalog(correlationId, name);
    if (user.isPresent()) {
      return user;
    }

    EngineContext ctx = engineContext();
    if (isSystemCatalogAlias(name, ctx)) {
      return Optional.of(SystemNodeRegistry.systemCatalogContainerId(ctx.effectiveEngineKind()));
    }
    return Optional.empty();
  }

  private boolean isSystemCatalogAlias(String name, EngineContext ctx) {
    if (name == null) {
      return false;
    }
    String candidate = name.trim();
    if (candidate.isEmpty()) {
      return false;
    }
    return candidate.equalsIgnoreCase(ctx.effectiveEngineKind());
  }

  /**
   * Resolves a namespace by name reference.
   *
   * <p>Uses system-first resolution. Write-side catalog surface policy prevents user namespaces
   * from occupying system namespace paths, so query resolution does not need a user graph probe
   * once the system graph matches.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved namespace resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveNamespace(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    NameRef systemRef = SystemCatalogTranslator.toSystemNamespaceRef(ref, ctx);
    Optional<ResourceId> system = systemGraph.resolveNamespace(systemRef, ctx);
    return system.isPresent() ? system : userGraph.resolveNamespace(correlationId, ref);
  }

  /**
   * Resolves a table by name reference.
   *
   * <p>Uses system-first resolution. Write-side catalog surface policy prevents user relations from
   * occupying system relation names, so query resolution does not need a user graph probe once the
   * system graph matches.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved table resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveTable(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    Optional<ResourceId> system = systemGraph.resolveTable(ref, ctx);
    return system.isPresent() ? system : userGraph.resolveTable(correlationId, ref);
  }

  /**
   * Resolves a view by name reference.
   *
   * <p>Uses system-first resolution. Write-side catalog surface policy prevents user relations from
   * occupying system relation names, so query resolution does not need a user graph probe once the
   * system graph matches.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved view resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveView(String correlationId, NameRef ref) {
    EngineContext ctx = engineContext();
    Optional<ResourceId> system = systemGraph.resolveView(ref, ctx);
    return system.isPresent() ? system : userGraph.resolveView(correlationId, ref);
  }

  /**
   * Resolves a relation (table or view) by name reference.
   *
   * <p>Uses system-first resolution. Write-side catalog surface policy prevents user relations from
   * occupying system relation names, so query resolution does not need a user graph probe once the
   * system graph matches.
   *
   * @param correlationId correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved relation resource ID, if present
   */
  @Override
  public Optional<ResourceId> resolveName(String correlationId, NameRef ref) {
    return resolveName(correlationId, ref, engineContext());
  }

  @Override
  public Optional<ResourceId> resolveName(
      String correlationId, NameRef ref, EngineContext engineContext) {
    EngineContext ctx = orRequestEngine(engineContext, this::engineContext);
    Optional<ResourceId> system = systemGraph.resolveName(ref, ctx);
    Optional<ResourceId> resolved =
        system.isPresent() ? system : userGraph.resolveName(correlationId, ref);
    if (resolved.isEmpty() && !ctx.hasEngineKind()) {
      // A lookup that misses both graphs with an empty engine context while MDC proves the
      // request DID declare an engine is how a lost engine context manifests: engine-gated system
      // objects (sys.*, pg_catalog.*) silently resolve to NOT_FOUND and the engine reports a
      // wrong-answer-shaped 42P01 (eng-floe/floecat#361). The MDC gate keeps legitimately
      // engine-less callers (e.g. client-cli) from tripping this on every name typo.
      Object mdcEngineKind = MDC.get("floecat_engine_kind");
      if (mdcEngineKind instanceof String declaredEngineKind && !declaredEngineKind.isBlank()) {
        LOG.warnf(
            "resolveName miss with empty engine context: ref=%s correlation_id=%s — the request"
                + " declared engine_kind=%s but its context was lost before resolution",
            NameRefUtil.canonical(ref), correlationId, declaredEngineKind);
      }
    }
    return resolved;
  }

  /**
   * Batch kind-agnostic name resolution: system names answer from the in-memory registry; the rest
   * resolve through the user graph in one batch so names sharing a catalog/namespace resolve their
   * scope once.
   */
  @Override
  public Map<NameRef, Optional<ResourceId>> resolveNames(String correlationId, List<NameRef> refs) {
    EngineContext ctx = engineContext();
    var out = new LinkedHashMap<NameRef, Optional<ResourceId>>(refs.size());
    var userRefs = new ArrayList<NameRef>(refs.size());
    for (NameRef ref : refs) {
      if (out.containsKey(ref)) {
        continue;
      }
      Optional<ResourceId> system = systemGraph.resolveName(ref, ctx);
      out.put(ref, system);
      if (system.isEmpty()) {
        userRefs.add(ref);
      }
    }
    if (!userRefs.isEmpty()) {
      out.putAll(userGraph.resolveNames(correlationId, userRefs));
    }
    return out;
  }

  @Override
  public Optional<ResourceId> resolveSystemTable(NameRef ref) {
    EngineContext ctx = engineContext();
    return systemGraph.resolveTable(ref, ctx);
  }

  @Override
  public Optional<NameRef> resolveSystemTableName(ResourceId id) {
    EngineContext ctx = engineContext();
    return systemGraph.tableName(id, ctx);
  }

  @Override
  public Optional<TypeNode> resolveSystemType(String namespace, String typeName) {
    EngineContext ctx = engineContext();
    return systemGraph.resolveType(namespace, typeName, ctx);
  }

  /**
   * Gets the snapshot pin for a table.
   *
   * <p>System tables don't require snapshot pins as they are immutable. Returns null for system
   * tables, otherwise delegates to user graph.
   *
   * @param correlationId correlation ID for error reporting
   * @param tableId the table resource ID
   * @param override explicit snapshot override, if any
   * @param asOfDefault default timestamp for time travel queries
   * @return the snapshot pin, or null for system tables
   */
  @Override
  public ai.floedb.floecat.query.rpc.TablePin tablePinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    // System tables have no snapshots and are never pinned.
    if (systemGraph
        .resolve(tableId, engineContext())
        .filter(SystemTableNode.class::isInstance)
        .isPresent()) {
      return null;
    }
    return userGraph.tablePinFor(correlationId, tableId, override, asOfDefault);
  }

  /**
   * Resolves tables for an explicit list of fully-qualified name references.
   *
   * <p>System resolution is attempted first using the current engine context (kind/version). Names
   * that match system objects do not require a user graph lookup because write-side policy prevents
   * user relations from occupying system relation names.
   *
   * <p>The returned {@link NameRef} for system objects is aliased back to the user-facing catalog
   * name so callers observe a "symlink" effect (e.g. user catalog "examples" shows {@code
   * information_schema.schemata}).
   */
  @Override
  public ResolveResult batchResolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    return batchResolveRelations(correlationId, items, limit, token, true);
  }

  /**
   * Resolves tables under a namespace prefix.
   *
   * <p>Performs a full merge between system and user objects. If the prefix resolves to a system
   * namespace under the current engine context, system relations are enumerated and returned first
   * (aliased back to the user-facing catalog). User relations are then appended.
   *
   * <p>This enables queries like {@code tables examples.information_schema} to surface built-in
   * engine relations while preserving user catalog naming.
   */
  @Override
  public ResolveResult listTablesByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {
    return listRelationsByPrefix(correlationId, prefix, limit, token, true);
  }

  /**
   * Resolves views for an explicit list of fully-qualified name references.
   *
   * <p>System resolution is attempted first using the current engine context (kind/version). Names
   * that match system objects do not require a user graph lookup because write-side policy prevents
   * user relations from occupying system relation names.
   *
   * <p>The returned {@link NameRef} for system objects is aliased back to the user-facing catalog
   * name so callers observe a "symlink" effect.
   */
  @Override
  public ResolveResult batchResolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    return batchResolveRelations(correlationId, items, limit, token, false);
  }

  /**
   * Shared list-resolution behind {@link #batchResolveTables} and {@link #batchResolveViews}:
   * system names answer from the in-memory registry (aliased back to the user-facing catalog), the
   * rest resolve through the user graph.
   */
  private ResolveResult batchResolveRelations(
      String correlationId, List<NameRef> items, int limit, String token, boolean tables) {

    validateListToken(correlationId, token);

    if (items == null || items.isEmpty()) {
      return new ResolveResult(List.of(), 0, "");
    }

    EngineContext ctx = engineContext();
    int max = Math.min(items.size(), normalizeLimit(limit));

    List<NameRef> subset = items.subList(0, max);
    Map<String, CatalogOverlay.QualifiedRelation> merged =
        collectSystemRelationsForNames(subset, ctx, tables);

    List<NameRef> userRefs = unresolvedUserRefs(subset, merged);
    if (!userRefs.isEmpty() && merged.size() < max) {
      ResolveResult userResult =
          toResolveResult(
              tables
                  ? userGraph.resolveTables(correlationId, userRefs, max - merged.size(), token)
                  : userGraph.resolveViews(correlationId, userRefs, max - merged.size(), token));
      for (CatalogOverlay.QualifiedRelation userRelation : userResult.relations()) {
        merged.put(canonicalName(userRelation.name()), userRelation);
      }
    }

    return new ResolveResult(new ArrayList<>(merged.values()), merged.size(), "");
  }

  /**
   * Resolves views under a namespace prefix.
   *
   * <p>Performs a full merge between system and user objects. If the prefix resolves to a system
   * namespace under the current engine context, system relations are enumerated and returned first
   * (aliased back to the user-facing catalog). User relations are then appended.
   */
  @Override
  public ResolveResult listViewsByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {
    return listRelationsByPrefix(correlationId, prefix, limit, token, false);
  }

  /**
   * Phased prefix listing: system relations first (sorted by canonical name), then user relations.
   *
   * <p>Tokens are phase-scoped so neither cursor can advance past rows the page did not emit. A
   * {@code sys:} token resumes the (bounded, in-memory) system list after the carried name; the
   * user-phase token is the user graph's own cursor. The user graph is only consulted for rows once
   * the system rows are fully emitted, so a page filled by system rows never advances — and can
   * never drop — user results. Reported totals combine both sources.
   *
   * <p>NOTE: this reimplements the same "system phase, then user phase, phase-scoped resume token"
   * shape that {@code CatalogSurfaceRelationPager} generalizes via its {@code Source<P, N>}
   * abstraction. The duplication is deliberate for now (this path is the just-fixed headline of the
   * pagination-correctness work); unifying the two phased pagers onto one abstraction is tracked as
   * a follow-up so a future fix to one does not silently miss the other.
   */
  private ResolveResult listRelationsByPrefix(
      String correlationId, NameRef prefix, int limit, String token, boolean tables) {

    EngineContext ctx = engineContext();
    int max = normalizeLimit(limit);

    final boolean sysToken = token != null && token.startsWith(SYS_TOKEN_PREFIX);
    final String sysResume =
        sysToken ? PageTokens.decode(SYS_TOKEN_PREFIX, token, correlationId) : "";
    final boolean userPhase = !sysToken && token != null && !token.isBlank();
    final String userToken = userPhase ? decodeUserToken(token) : "";

    var merged = new ArrayList<CatalogOverlay.QualifiedRelation>(Math.min(max, 64));

    // System-relation total for the combined count. On a user-phase page the system rows were all
    // emitted on earlier pages, so only their count is needed here — skip the sorted collect (with
    // its per-relation name resolution) and count the bounded in-memory nodes directly. The sorted
    // list is built only while the system phase is actually emitting rows.
    int systemTotal;
    if (userPhase) {
      systemTotal = countSystemRelationsInNamespace(prefix, ctx, tables);
    } else {
      List<CatalogOverlay.QualifiedRelation> systemAll =
          new ArrayList<>(
              collectSystemRelationsInNamespace(prefix, ctx, tables, Integer.MAX_VALUE));
      systemAll.sort(Comparator.comparing(rel -> canonicalName(rel.name())));
      systemTotal = systemAll.size();

      String lastSystemName = "";
      for (CatalogOverlay.QualifiedRelation rel : systemAll) {
        String name = canonicalName(rel.name());
        if (!sysResume.isBlank() && name.compareTo(sysResume) <= 0) {
          continue;
        }
        if (merged.size() >= max) {
          // System rows remain: continue the system phase next page; the user cursor is untouched.
          return new ResolveResult(
              merged,
              systemTotal + userTotalByPrefix(correlationId, prefix, tables),
              PageTokens.encode(SYS_TOKEN_PREFIX, lastSystemName));
        }
        merged.add(rel);
        lastSystemName = name;
      }
      if (merged.size() >= max) {
        // Page filled exactly as the system rows ran out. Hand off to the user phase only when
        // there are user rows to continue into; with none, USER_TOKEN_PREFIX would advertise a
        // next page that is always empty. The next page starts the user scan from the beginning.
        int userTotal = userTotalByPrefix(correlationId, prefix, tables);
        return new ResolveResult(
            merged, systemTotal + userTotal, userTotal > 0 ? USER_TOKEN_PREFIX : "");
      }
    }

    int room = max - merged.size();
    ResolveResult user =
        toResolveResult(
            tables
                ? userGraph.resolveTables(correlationId, prefix, room, userToken)
                : userGraph.resolveViews(correlationId, prefix, room, userToken));
    for (CatalogOverlay.QualifiedRelation rel : user.relations()) {
      if (merged.size() >= max) {
        break;
      }
      merged.add(rel);
    }
    return new ResolveResult(
        merged, systemTotal + user.totalSize(), encodeUserToken(user.nextToken()));
  }

  /**
   * User-relation total for combined counts on system-phase pages. Uses a count-only path (a plain
   * repo countByPrefix) rather than fetching and discarding a one-row probe.
   */
  private int userTotalByPrefix(String correlationId, NameRef prefix, boolean tables) {
    return tables
        ? userGraph.countTablesByPrefix(correlationId, prefix)
        : userGraph.countViewsByPrefix(correlationId, prefix);
  }

  /**
   * Gets the fully qualified name of a namespace by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the namespace resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    Optional<NameRef> user = userGraph.namespaceName(id);
    if (user.isPresent()) {
      return user;
    }
    EngineContext ctx = engineContext();
    return systemGraph.namespaceName(id, ctx);
  }

  /**
   * Gets the fully qualified name of a table by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the table resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    Optional<NameRef> user = userGraph.tableName(id);
    if (user.isPresent()) {
      return user;
    }
    EngineContext ctx = engineContext();
    return systemGraph.tableName(id, ctx);
  }

  /**
   * Gets the fully qualified name of a view by its resource ID.
   *
   * <p>Tries user graph first, then system graph for reverse lookup.
   *
   * @param id the view resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    Optional<NameRef> user = userGraph.viewName(id);
    if (user.isPresent()) {
      return user;
    }
    EngineContext ctx = engineContext();
    return systemGraph.viewName(id, ctx);
  }

  /**
   * Resolves a catalog node by its resource ID.
   *
   * <p>Looks up user catalogs first, then the synthetic system catalog for the current engine.
   *
   * @param id the catalog resource ID
   * @return the catalog node, or empty if not found
   */
  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    Optional<CatalogNode> user = userGraph.catalog(id);
    if (user.isPresent()) {
      return user;
    }
    EngineContext ctx = engineContext();
    return systemGraph.catalog(id, ctx);
  }

  /**
   * Gets the schema resolution for a table at a specific snapshot.
   *
   * <p>Delegates to user graph for schema resolution as system schemas are handled differently.
   *
   * @param correlationId correlation ID for error reporting
   * @param tableId the table resource ID
   * @param snapshot the snapshot reference
   * @param tableBlobUri the pinned table blob, or empty to read the current pointer
   * @param snapshotBlobUri the pinned snapshot blob, or empty to resolve via the live pointer
   * @return the schema resolution, or null if not found
   */
  @Override
  public SchemaResolution schemaFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef snapshot,
      String tableBlobUri,
      String snapshotBlobUri) {
    UserGraph.SchemaResolution delegate =
        userGraph.schemaFor(correlationId, tableId, snapshot, tableBlobUri, snapshotBlobUri);
    if (delegate == null) {
      return null;
    }
    return new SchemaResolution(delegate.table(), delegate.schemaJson());
  }

  /**
   * Merges two lists of items, combining system items first, then user items.
   *
   * <p>This ensures system objects are globally visible alongside user objects without any
   * filtering or deduplication.
   *
   * @param systemSupplier supplier for system items
   * @param userSupplier supplier for user items
   * @return merged list with system items followed by user items
   * @param <T> the type of items being merged
   */
  private <T> List<T> mergeLists(Supplier<List<T>> systemSupplier, Supplier<List<T>> userSupplier) {
    List<T> result = new ArrayList<>();
    result.addAll(systemSupplier.get());
    result.addAll(userSupplier.get());
    return result;
  }

  private List<NamespaceNode> listUserNamespacesFromTopology(ResourceId catalogId) {
    return topologyCache.listNamespaceRefs(catalogId).stream()
        .map(TopologyGraph.NamespaceRef::id)
        .map(userGraph::namespace)
        .flatMap(Optional::stream)
        .toList();
  }

  private List<RelationNode> listUserRelationsFromTopology(ResourceId catalogId) {
    List<RelationNode> result = new ArrayList<>();
    for (TopologyGraph.NamespaceRef namespace : topologyCache.listNamespaceRefs(catalogId)) {
      result.addAll(listUserRelationsFromTopology(catalogId, namespace.id()));
    }
    return result;
  }

  private List<RelationNode> listUserRelationsFromTopology(
      ResourceId catalogId, ResourceId namespaceId) {
    return topologyCache.listRelationRefs(catalogId, namespaceId).stream()
        .map(this::resolveUserRelation)
        .flatMap(Optional::stream)
        .toList();
  }

  private Optional<RelationNode> resolveUserRelation(TopologyGraph.RelationRef ref) {
    if (ref.kind() == ResourceKind.RK_VIEW) {
      return userGraph.view(ref.id()).map(RelationNode.class::cast);
    }
    return userGraph.table(ref.id()).map(RelationNode.class::cast);
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {
    return resolve(tableId)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .map(this::schemaForTable)
        .orElse(List.of());
  }

  private List<SchemaColumn> schemaForTable(TableNode table) {
    if (table instanceof UserTableNode ut) {
      return schemaMapper.map(ut).getColumnsList();
    }
    if (table instanceof SystemTableNode st) {
      return st.columns();
    }
    return List.of();
  }

  /**
   * Converts a UserGraph resolve result to a CatalogOverlay resolve result.
   *
   * @param delegate the user graph resolve result
   * @return the catalog overlay resolve result
   */
  private ResolveResult toResolveResult(UserGraph.ResolveResult delegate) {
    List<CatalogOverlay.QualifiedRelation> relations =
        delegate.relations().stream()
            .map(rel -> new CatalogOverlay.QualifiedRelation(rel.name(), rel.resourceId()))
            .toList();
    return new ResolveResult(relations, delegate.totalSize(), delegate.nextToken());
  }

  /**
   * Normalizes list/prefix resolution limits.
   *
   * <p>Ensures a positive limit and applies a small default for callers that omit the limit.
   */
  private static final String USER_TOKEN_PREFIX = "u:";

  private static final String SYS_TOKEN_PREFIX = "sys:";

  private static int normalizeLimit(int limit) {
    return Math.max(1, limit > 0 ? limit : 50);
  }

  private static String canonicalName(NameRef ref) {
    return NameRefUtil.canonical(ref);
  }

  private static String decodeUserToken(String token) {
    if (token == null || token.isBlank()) {
      return "";
    }
    if (token.startsWith(USER_TOKEN_PREFIX)) {
      return token.substring(USER_TOKEN_PREFIX.length());
    }
    return token;
  }

  private static String encodeUserToken(String token) {
    if (token == null || token.isBlank()) {
      return "";
    }
    return USER_TOKEN_PREFIX + token;
  }

  private void validateListToken(String correlationId, String token) {
    if (token != null && !token.isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID,
          Map.of("page_token", token));
    }
  }

  private Map<String, CatalogOverlay.QualifiedRelation> collectSystemRelationsForNames(
      List<NameRef> refs, EngineContext ctx, boolean tables) {
    Map<String, CatalogOverlay.QualifiedRelation> result = new LinkedHashMap<>();
    for (NameRef ref : refs) {
      Optional<ResourceId> sysId =
          tables
              ? systemGraph.resolveTable(SystemCatalogTranslator.toSystemRelationRef(ref, ctx), ctx)
              : systemGraph.resolveView(SystemCatalogTranslator.toSystemRelationRef(ref, ctx), ctx);
      if (sysId.isEmpty()) {
        continue;
      }

      NameRef systemName =
          tables
              ? systemGraph.tableName(sysId.get(), ctx).orElse(ref)
              : systemGraph.viewName(sysId.get(), ctx).orElse(ref);
      NameRef alias = SystemCatalogTranslator.aliasToUserCatalog(ref, systemName);
      CatalogOverlay.QualifiedRelation relation =
          new CatalogOverlay.QualifiedRelation(alias, sysId.get());
      result.put(canonicalName(alias), relation);
    }
    return result;
  }

  private List<NameRef> unresolvedUserRefs(
      List<NameRef> refs, Map<String, CatalogOverlay.QualifiedRelation> systemMatches) {
    if (systemMatches.isEmpty()) {
      return refs;
    }
    List<NameRef> out = new ArrayList<>();
    for (NameRef ref : refs) {
      if (!systemMatches.containsKey(canonicalName(ref))) {
        out.add(ref);
      }
    }
    return out;
  }

  /**
   * Counts system relations of the requested kind under a prefix, without the per-relation name
   * resolution and {@link NameRef} construction {@link #collectSystemRelationsInNamespace} does.
   * Used for the combined total on user-phase pages, where the sorted system list is not needed.
   */
  private int countSystemRelationsInNamespace(NameRef prefix, EngineContext ctx, boolean tables) {
    Optional<ResourceId> sysNsId =
        systemGraph.resolveNamespace(
            SystemCatalogTranslator.toSystemNamespaceRef(prefix, ctx), ctx);
    if (sysNsId.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (RelationNode n :
        systemGraph.listRelationsInNamespace(ResourceId.getDefaultInstance(), sysNsId.get(), ctx)) {
      if ((tables && n instanceof TableNode) || (!tables && n instanceof ViewNode)) {
        count++;
      }
    }
    return count;
  }

  private List<CatalogOverlay.QualifiedRelation> collectSystemRelationsInNamespace(
      NameRef prefix, EngineContext ctx, boolean tables, int max) {
    Optional<ResourceId> sysNsId =
        systemGraph.resolveNamespace(
            SystemCatalogTranslator.toSystemNamespaceRef(prefix, ctx), ctx);
    if (sysNsId.isEmpty()) {
      return List.of();
    }

    List<RelationNode> nodes =
        systemGraph.listRelationsInNamespace(ResourceId.getDefaultInstance(), sysNsId.get(), ctx);
    List<CatalogOverlay.QualifiedRelation> out = new ArrayList<>(Math.min(nodes.size(), max));

    for (RelationNode n : nodes) {
      if (out.size() >= max) {
        break;
      }
      if (tables && n instanceof TableNode t) {
        NameRef systemName =
            systemGraph.tableName(t.id(), ctx).orElse(NameRef.getDefaultInstance());
        NameRef alias =
            SystemCatalogTranslator.aliasToUserCatalog(prefix, systemName).toBuilder()
                .setResourceId(t.id())
                .build();
        out.add(new CatalogOverlay.QualifiedRelation(alias, t.id()));
      } else if (!tables && n instanceof ViewNode v) {
        NameRef systemName = systemGraph.viewName(v.id(), ctx).orElse(NameRef.getDefaultInstance());
        NameRef alias =
            SystemCatalogTranslator.aliasToUserCatalog(prefix, systemName).toBuilder()
                .setResourceId(v.id())
                .build();
        out.add(new CatalogOverlay.QualifiedRelation(alias, v.id()));
      }
    }

    return out;
  }

  // ---- Lightweight ref listing and topology-cache invalidation ----

  @Override
  public boolean supportsLightweightRefs() {
    return true;
  }

  @Override
  public List<TopologyGraph.NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    EngineContext ctx = engineContext();
    List<NamespaceNode> sysNs = systemGraph.listNamespaces(catalogId, ctx);
    List<TopologyGraph.NamespaceRef> userNs = topologyCache.listNamespaceRefs(catalogId);
    if (sysNs.isEmpty()) {
      return userNs;
    }
    List<TopologyGraph.NamespaceRef> result = new ArrayList<>(sysNs.size() + userNs.size());
    for (NamespaceNode ns : sysNs) {
      result.add(
          new TopologyGraph.NamespaceRef(
              ns.id(), ns.displayName(), ns.catalogId(), ns.pathSegments()));
    }
    result.addAll(userNs);
    return result;
  }

  @Override
  public List<TopologyGraph.NamespaceRef> listNamespaceRefsByName(
      ResourceId catalogId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    EngineContext ctx = engineContext();
    List<NamespaceNode> sysNs =
        systemGraph.listNamespaces(catalogId, ctx).stream()
            .filter(
                ns ->
                    names.contains(
                        TopologyNames.namespaceName(ns.pathSegments(), ns.displayName())))
            .toList();
    List<TopologyGraph.NamespaceRef> userNs =
        topologyCache.listNamespaceRefsByName(catalogId, names);
    if (sysNs.isEmpty()) {
      return userNs;
    }
    List<TopologyGraph.NamespaceRef> result = new ArrayList<>(sysNs.size() + userNs.size());
    for (NamespaceNode ns : sysNs) {
      result.add(
          new TopologyGraph.NamespaceRef(
              ns.id(), ns.displayName(), ns.catalogId(), ns.pathSegments()));
    }
    result.addAll(userNs);
    return result;
  }

  @Override
  public List<TopologyGraph.RelationRef> listRelationRefs(
      ResourceId catalogId, ResourceId namespaceId) {
    EngineContext ctx = engineContext();
    List<RelationNode> sysRels = systemGraph.listRelationsInNamespace(catalogId, namespaceId, ctx);
    List<TopologyGraph.RelationRef> userRels =
        topologyCache.listRelationRefs(catalogId, namespaceId);
    if (sysRels.isEmpty()) {
      return userRels;
    }
    List<TopologyGraph.RelationRef> result = new ArrayList<>(sysRels.size() + userRels.size());
    for (RelationNode rel : sysRels) {
      ResourceKind kind = rel instanceof ViewNode ? ResourceKind.RK_VIEW : ResourceKind.RK_TABLE;
      result.add(new TopologyGraph.RelationRef(rel.id(), rel.displayName(), kind));
    }
    result.addAll(userRels);
    return result;
  }

  @Override
  public List<TopologyGraph.RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    EngineContext ctx = engineContext();
    List<RelationNode> sysRels =
        systemGraph.listRelationsInNamespace(catalogId, namespaceId, ctx).stream()
            .filter(r -> names.contains(r.displayName()))
            .toList();
    List<TopologyGraph.RelationRef> userRels =
        topologyCache.listRelationRefsByName(catalogId, namespaceId, names);
    if (sysRels.isEmpty()) {
      return userRels;
    }
    List<TopologyGraph.RelationRef> result = new ArrayList<>(sysRels.size() + userRels.size());
    for (RelationNode rel : sysRels) {
      ResourceKind kind = rel instanceof ViewNode ? ResourceKind.RK_VIEW : ResourceKind.RK_TABLE;
      result.add(new TopologyGraph.RelationRef(rel.id(), rel.displayName(), kind));
    }
    result.addAll(userRels);
    return result;
  }

  @Override
  public void evict(ResourceId resourceId) {
    topologyCache.evict(resourceId);
  }

  @Override
  public void evictRelationRefs(ResourceId namespaceId) {
    topologyCache.evictRelationRefs(namespaceId);
  }

  @Override
  public void evictNamespaceRefs(ResourceId catalogId) {
    topologyCache.evictNamespaceRefs(catalogId);
  }
}
