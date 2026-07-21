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

package ai.floedb.floecat.service.metagraph.overlay.user;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.catalog.impl.TableRootCommitter;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.cache.GraphCacheManager;
import ai.floedb.floecat.service.metagraph.hint.EngineHintManager;
import ai.floedb.floecat.service.metagraph.loader.NodeLoader;
import ai.floedb.floecat.service.metagraph.resolver.FullyQualifiedResolver;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.telemetry.Observability;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * ==== MetadataGraph (Façade) ====
 *
 * <p>Lightweight, cached, read-only façade over metadata.
 *
 * <p>Responsibilities: - Resolve nodes (table, view, namespace, catalog) - Resolve snapshots -
 * Resolve names + FQ prefix/list operations - Delegate enumeration / traversal to helpers below
 */
@ApplicationScoped
public final class UserGraph {

  // ----------------------------------------------------------------------
  // Dependencies (constructed once)
  // ----------------------------------------------------------------------

  private final GraphCacheManager cache;
  private final ImmutableBlobCache blobCache;
  private final NodeLoader nodes;
  private final NameResolver names;
  private final FullyQualifiedResolver fq;
  private final SnapshotHelper snapshots;
  private final EngineHintManager hints;
  private final PrincipalProvider principal;
  private final PinValidator pinValidator;

  // ----------------------------------------------------------------------
  // Constructor
  // ----------------------------------------------------------------------

  /**
   * Constructs a UserGraph for managing user-defined catalog objects.
   *
   * @param catalogRepo repository for catalog operations
   * @param nsRepo repository for namespace operations
   * @param snapshotRepo repository for snapshot operations
   * @param tableRepo repository for table operations
   * @param viewRepo repository for view operations
   * @param snapshotStub gRPC client for snapshot service
   * @param meter metrics registry for performance monitoring
   * @param principal provider for current principal context
   * @param cacheMaxSize maximum size of the graph cache
   * @param engineHints manager for engine-specific hints
   * @param blobCache process-wide decoded-blob cache holding derived nodes (null disables node
   *     caching; resolution then always loads)
   */
  @Inject
  public UserGraph(
      CatalogRepository catalogRepo,
      NamespaceRepository nsRepo,
      SnapshotRepository snapshotRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo,
      TableRootRepository tableRootRepo,
      TableRootCommitter rootCommitter,
      Observability observability,
      PrincipalProvider principal,
      @ConfigProperty(name = "floecat.metadata.graph.cache-max-size", defaultValue = "50000")
          long cacheMaxSize,
      @ConfigProperty(name = "floecat.metadata.graph.meta-cache-ttl-seconds", defaultValue = "2")
          long metaCacheTtlSeconds,
      EngineHintManager engineHints,
      ai.floedb.floecat.stats.spi.StatsStore statsStore,
      ImmutableBlobCache blobCache,
      PinValidator pinValidator) {
    this.cache =
        new GraphCacheManager(
            cacheMaxSize > 0, cacheMaxSize, Math.max(0L, metaCacheTtlSeconds), observability);
    // cache-max-size=0 keeps its historical meaning — node caching OFF — independent of the
    // process-wide blob cache (whose kill switch would also drop blob decodes and indexes).
    this.blobCache = cacheMaxSize > 0 ? blobCache : null;
    this.nodes = new NodeLoader(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.names = new NameResolver(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.fq = new FullyQualifiedResolver(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.pinValidator = pinValidator;
    this.snapshots =
        new SnapshotHelper(snapshotRepo, tableRootRepo, rootCommitter, statsStore, pinValidator);
    this.hints = engineHints;
    this.principal = principal;
  }

  /** TEST-ONLY constructor with explicit cache knobs; no legacy-root synthesis. */
  public UserGraph(
      CatalogRepository catalogRepo,
      NamespaceRepository nsRepo,
      SnapshotRepository snapshotRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo,
      TableRootRepository tableRootRepo,
      Observability observability,
      PrincipalProvider principal,
      long cacheMaxSize,
      EngineHintManager engineHints) {
    this(
        catalogRepo,
        nsRepo,
        snapshotRepo,
        tableRepo,
        viewRepo,
        tableRootRepo,
        new TableRootCommitter(tableRootRepo),
        observability,
        principal,
        cacheMaxSize,
        2L,
        engineHints,
        null,
        // Mirror the pre-fold node-cache knob: a positive max size enables node caching.
        cacheMaxSize > 0
            ? new ImmutableBlobCache(true, 64L * 1024 * 1024, Duration.ofMinutes(15))
            : null,
        // No pointer store in this wiring, so broken-root repair reporting is disabled.
        new PinValidator(tableRootRepo, RootRepairRequests.disabled()));
  }

  /** TEST-ONLY constructor; no legacy-root synthesis. */
  public UserGraph(
      CatalogRepository catalogRepo,
      NamespaceRepository nsRepo,
      SnapshotRepository snapshotRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo,
      TableRootRepository tableRootRepo,
      Observability observability) {
    this(
        catalogRepo,
        nsRepo,
        snapshotRepo,
        tableRepo,
        viewRepo,
        tableRootRepo,
        new TableRootCommitter(tableRootRepo),
        observability,
        new PrincipalProvider() {
          @Override
          public PrincipalContext get() {
            return PrincipalContext.newBuilder().setAccountId("account").build();
          }
        },
        1024L,
        2L,
        null,
        null,
        new ImmutableBlobCache(true, 64L * 1024 * 1024, Duration.ofMinutes(15)),
        // No pointer store in this wiring, so broken-root repair reporting is disabled.
        new PinValidator(tableRootRepo, RootRepairRequests.disabled()));
  }

  // No writer-refresh here, deliberately (unlike the root-pointer cache): resolve() always
  // re-reads a LIVE pointer before hydrating, so a stale reinserted meta can only short-circuit
  // to an already-cached node at that blob — the TTL-bounded staleness this class documents,
  // never stale content served as current. The version-guarded putMeta removes the pathological
  // arbitrarily-late overwrite; a refresh read per DDL would buy nothing on top.
  //
  // Only the meta pointer needs invalidation. Node entries are content-keyed by blob URI in the
  // process-wide ImmutableBlobCache: a blob's derived node is right forever, so DDL simply makes
  // the fresh pointer name a different blob (and thus a different node entry).
  public void invalidate(ResourceId id) {
    cache.invalidate(id);
  }

  /**
   * Gets the current account ID from the principal context.
   *
   * @return the current account ID
   * @throws IllegalStateException if no account context is available
   */
  public String currentAccountId() {
    return requireAccountId("internal");
  }

  /**
   * Lists all catalog IDs for the specified account.
   *
   * @param accountId the account ID to list catalogs for
   * @return list of catalog resource IDs
   */
  public List<ResourceId> listAllCatalogIds(String accountId) {
    return nodes.listCatalogIds(accountId);
  }

  /**
   * Gets the fully qualified name of a namespace by its resource ID.
   *
   * @param id the namespace resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> namespaceName(ResourceId id) {
    return namespace(id)
        .flatMap(
            ns -> catalog(ns.catalogId()).map(cat -> buildNamespaceNameRef(ns, cat.displayName())));
  }

  /**
   * Gets the fully qualified name of a table by its resource ID.
   *
   * @param id the table resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> tableName(ResourceId id) {
    return table(id)
        .flatMap(
            tbl ->
                namespace(tbl.namespaceId())
                    .flatMap(
                        ns ->
                            catalog(ns.catalogId())
                                .map(
                                    cat ->
                                        buildRelationNameRef(
                                            tbl.displayName(), tbl.id(), ns, cat.displayName()))));
  }

  /**
   * Gets the fully qualified name of a view by its resource ID.
   *
   * @param id the view resource ID
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> viewName(ResourceId id) {
    return view(id)
        .flatMap(
            vw ->
                namespace(vw.namespaceId())
                    .flatMap(
                        ns ->
                            catalog(ns.catalogId())
                                .map(
                                    cat ->
                                        buildRelationNameRef(
                                            vw.displayName(), vw.id(), ns, cat.displayName()))));
  }

  // ----------------------------------------------------------------------
  // Node resolution (cached)
  // ----------------------------------------------------------------------

  /**
   * Resolves a graph node by ID with caching.
   *
   * <p>Loads nodes from repositories on cache miss and stores them for future access.
   *
   * @param id the resource ID to resolve
   * @return the resolved graph node, or empty if not found
   */
  public Optional<GraphNode> resolve(ResourceId id) {

    // ----- Hot path: cached meta names the content key for an already-derived node --------------
    MutationMeta cachedMeta = cache.getMeta(id);
    if (cachedMeta != null) {
      GraphNode hit = cachedNode(cachedMeta.getBlobUri());
      if (hit != null) {
        return Optional.of(hit);
      }
    }

    // ----- Hydration path: re-read a fresh pointer before loading -------------------------------
    // The meta cache is a per-process Caffeine cache with no cross-instance invalidation, so a
    // cached meta can name a superseded blob that is still readable (CAS blobs outlive the meta
    // TTL). Hydrating from that stale blobUri would serve stale content as current. Only a freshly
    // read pointer is safe to hand to load(); the meta cache is trusted solely to short-circuit to
    // an already-cached node above.
    Optional<MutationMeta> freshOpt = nodes.mutationMeta(id);
    if (freshOpt.isEmpty()) {
      return Optional.empty();
    }
    MutationMeta fresh = freshOpt.get();
    cache.putMeta(id, fresh);

    GraphNode cached = cachedNode(fresh.getBlobUri());
    if (cached != null) return Optional.of(cached);

    long loadStart = System.nanoTime();
    try {
      Optional<GraphNode> loaded = nodes.load(id, fresh);
      // Key by the identity the node ACTUALLY carries, not the meta we passed in: load()'s
      // swept-blob fallback may have built the node from a NEWER live meta, and storing that node
      // under the stale URI would poison the never-invalidated content-keyed cache — the same
      // mismatch the loader itself was fixed for, one seam up.
      loaded.ifPresent(node -> putNode(node.cacheIdentity(), node));
      cache.recordLoad(Duration.ofNanos(System.nanoTime() - loadStart));
      return loaded;
    } catch (Throwable t) {
      Duration duration = Duration.ofNanos(System.nanoTime() - loadStart);
      cache.recordLoadFailure(duration, t);
      throw t;
    }
  }

  /**
   * The cached derived node for the blob at {@code blobUri}, or {@code null} when node caching is
   * off, the URI is blank (a meta that names no blob has no content identity), or the entry is
   * absent.
   *
   * <p>Probe-then-build-then-put, deliberately NOT a loading get: nodes.load() decodes the source
   * blob through this SAME cache (the repository getByBlobUri seam), and a nested compute inside a
   * Caffeine compute is prohibited — a same-bin hash collision between the "#node" key and the blob
   * key would throw "Recursive update" or livelock, nondeterministically. A duplicate concurrent
   * build is harmless (the node is a pure function of the blob); the blob decode itself stays
   * single-flight.
   */
  private GraphNode cachedNode(String blobUri) {
    if (blobCache == null || !blobCache.enabled() || blobUri == null || blobUri.isBlank()) {
      return null;
    }
    String key = nodeKey(blobUri);
    return blobCache.probe(key);
  }

  private void putNode(String blobUri, GraphNode node) {
    if (blobCache == null || !blobCache.enabled() || blobUri == null || blobUri.isBlank()) {
      return;
    }
    blobCache.put(nodeKey(blobUri), node);
  }

  /** Derived-form key: the node built from the blob at {@code blobUri} (cf. "#index" entries). */
  private static String nodeKey(String blobUri) {
    return blobUri + "#node";
  }

  /**
   * Resolves a catalog node by ID.
   *
   * @param id the catalog resource ID
   * @return the catalog node, or empty if not found
   */
  public Optional<CatalogNode> catalog(ResourceId id) {
    return resolve(id).map(CatalogNode.class::cast);
  }

  /**
   * Resolves a namespace node by ID.
   *
   * @param id the namespace resource ID
   * @return the namespace node, or empty if not found
   */
  public Optional<NamespaceNode> namespace(ResourceId id) {
    return resolve(id).map(NamespaceNode.class::cast);
  }

  /**
   * Resolves a table node by ID.
   *
   * @param id the table resource ID
   * @return the table node, or empty if not found
   */
  public Optional<UserTableNode> table(ResourceId id) {
    return resolve(id).filter(UserTableNode.class::isInstance).map(UserTableNode.class::cast);
  }

  /**
   * Resolves a view node by ID.
   *
   * @param id the view resource ID
   * @return the view node, or empty if not found
   */
  public Optional<ViewNode> view(ResourceId id) {
    return resolve(id).filter(ViewNode.class::isInstance).map(ViewNode.class::cast);
  }

  // ----------------------------------------------------------------------
  // Snapshot resolution
  // ----------------------------------------------------------------------

  /**
   * Gets the snapshot pin for a table.
   *
   * @param cid correlation ID for error reporting
   * @param tableId the table resource ID
   * @param override explicit snapshot override, if any
   * @param asOfDefault default timestamp for time travel queries
   * @return the snapshot pin for the table
   */
  public TablePin tablePinFor(
      String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    return snapshots.tablePinFor(cid, tableId, override, asOfDefault);
  }

  /**
   * Resolves a relation (table or view) by name reference.
   *
   * <p>Checks tables first, then views. Write-side catalog surface policy prevents tables and views
   * from sharing a relation name, so the query path does not need to probe both kinds when a table
   * exists.
   *
   * @param cid correlation ID for error reporting
   * @param ref the name reference to resolve
   * @return the resolved resource ID, if present
   */
  public Optional<ResourceId> resolveName(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    String accountId = requireAccountId(cid);
    return names.resolveRelationId(accountId, ref);
  }

  /**
   * Batch variant of {@link #resolveName}: names sharing a catalog/namespace resolve their scope
   * (catalog + namespace reads) once per call instead of once per name.
   */
  public Map<NameRef, Optional<ResourceId>> resolveNames(String cid, List<NameRef> refs) {
    refs.forEach(ref -> validateNameRef(cid, ref));
    String accountId = requireAccountId(cid);
    return names.resolveRelationIds(accountId, refs);
  }

  /**
   * Gets the schema JSON for a table at a specific snapshot.
   *
   * @param cid correlation ID for error reporting
   * @param tbl the table node
   * @param snapshot the snapshot reference
   * @return the schema JSON string
   */
  public String schemaJsonFor(String cid, UserTableNode tbl, SnapshotRef snapshot) {
    return schemaJsonFor(cid, tbl, snapshot, "");
  }

  /**
   * Gets the schema JSON for a table, preferring the pinned snapshot blob. When {@code
   * snapshotBlobUri} names a pinned snapshot blob, the schema is read from that immutable blob
   * rather than re-hydrating the snapshot through the live {@code (table, snapshot id)} pointer,
   * which an in-place {@code UpdateSnapshot} can repoint after the pin was validated. Empty uri
   * resolves the snapshot reference against the live pointer.
   */
  public String schemaJsonFor(
      String cid, UserTableNode tbl, SnapshotRef snapshot, String snapshotBlobUri) {
    return snapshots.schemaJsonFor(cid, tbl, snapshot, snapshotBlobUri, tbl::schemaJson);
  }

  /**
   * Gets the schema resolution for a table at a specific snapshot.
   *
   * @param cid correlation ID for error reporting
   * @param tblId the table resource ID
   * @param snapshot the snapshot reference
   * @return the schema resolution containing the table and schema JSON
   */
  public SchemaResolution schemaFor(String cid, ResourceId tblId, SnapshotRef snapshot) {
    return schemaFor(cid, tblId, snapshot, "", "");
  }

  /**
   * Schema resolution for a pinned query. When {@code tableBlobUri} names a pinned table blob, the
   * table node is loaded from that immutable blob so the mapper interprets the (snapshot-sourced)
   * schema with the pinned table's format/connector metadata — never the drifted current pointer —
   * and the resolution survives a drop/unpublish of the current table. Empty uri reads the current
   * pointer (a non-pinned caller).
   *
   * @param cid correlation ID for error reporting
   * @param tblId the table resource ID
   * @param snapshot the snapshot reference
   * @param tableBlobUri the pinned table blob, or empty to read the current pointer
   * @param snapshotBlobUri the pinned snapshot blob, or empty to resolve via the live pointer
   * @return the schema resolution containing the table and schema JSON
   */
  public SchemaResolution schemaFor(
      String cid,
      ResourceId tblId,
      SnapshotRef snapshot,
      String tableBlobUri,
      String snapshotBlobUri) {
    UserTableNode tbl =
        (tableBlobUri == null || tableBlobUri.isEmpty())
            ? table(tblId)
                .orElseThrow(
                    () ->
                        GrpcErrors.notFound(
                            cid,
                            GeneratedErrorMessages.MessageKey.TABLE,
                            Map.of("id", tblId.getId())))
            : pinValidator.requirePinnedTableBlob(
                nodes.tableFromBlob(tblId, tableBlobUri), cid, tblId);
    return new SchemaResolution(tbl, schemaJsonFor(cid, tbl, snapshot, snapshotBlobUri));
  }

  public record SchemaResolution(UserTableNode table, String schemaJson) {}

  // ----------------------------------------------------------------------
  // Name resolution
  // ----------------------------------------------------------------------

  public Optional<ResourceId> resolveCatalog(String cid, String name) {
    return names.resolveCatalogId(cid, requireAccountId(cid), name);
  }

  public Optional<ResourceId> resolveNamespace(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    String accountId = requireAccountId(cid);
    return names.resolveNamespaceId(cid, accountId, ref);
  }

  public Optional<ResourceId> resolveTable(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "table");
    String accountId = requireAccountId(cid);
    return names.resolveTableId(cid, accountId, ref);
  }

  public Optional<ResourceId> resolveView(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "view");
    String accountId = requireAccountId(cid);
    return names.resolveViewId(cid, accountId, ref);
  }

  // ----------------------------------------------------------------------
  // FQ resolution (prefix/list semantics)
  // ----------------------------------------------------------------------

  public ResolveResult resolveTables(String cid, List<NameRef> items, int limit, String token) {
    FullyQualifiedResolver.ResolveResult fqResult =
        fq.resolveTableList(cid, requireAccountId(cid), items, limit, token);
    return new ResolveResult(fqResult);
  }

  public ResolveResult resolveTables(String cid, NameRef prefix, int limit, String token) {
    validateNameRef(cid, prefix);
    FullyQualifiedResolver.ResolveResult fqResult =
        fq.resolveTablesByPrefix(cid, requireAccountId(cid), prefix, limit, token);
    return new ResolveResult(fqResult);
  }

  public ResolveResult resolveViews(String cid, List<NameRef> items, int limit, String token) {
    FullyQualifiedResolver.ResolveResult fqResult =
        fq.resolveViewList(cid, requireAccountId(cid), items, limit, token);
    return new ResolveResult(fqResult);
  }

  public ResolveResult resolveViews(String cid, NameRef prefix, int limit, String token) {
    validateNameRef(cid, prefix);
    FullyQualifiedResolver.ResolveResult fqResult =
        fq.resolveViewsByPrefix(cid, requireAccountId(cid), prefix, limit, token);
    return new ResolveResult(fqResult);
  }

  /** Counts user tables under a prefix without fetching any rows. */
  public int countTablesByPrefix(String cid, NameRef prefix) {
    validateNameRef(cid, prefix);
    return fq.countTablesByPrefix(cid, requireAccountId(cid), prefix);
  }

  /** Counts user views under a prefix without fetching any rows. */
  public int countViewsByPrefix(String cid, NameRef prefix) {
    validateNameRef(cid, prefix);
    return fq.countViewsByPrefix(cid, requireAccountId(cid), prefix);
  }

  // ----------------------------------------------------------------------
  // Unified relation listing (tables + views)
  // ----------------------------------------------------------------------

  /**
   * Lists all relations (tables and views) in the specified catalog.
   *
   * @param catalogId the catalog to list relations from
   * @return list of all relations in the catalog
   */
  public List<RelationNode> listRelations(ResourceId catalogId) {
    List<RelationNode> out = new ArrayList<>(128);

    final String accountId = catalogId.getAccountId();
    final String cat = catalogId.getId();

    // --- Tables ---
    var tblIds = names.listTableIds(accountId, cat);
    for (ResourceId tblId : tblIds) {
      nodes.table(tblId).ifPresent(out::add);
    }

    // --- Views ---
    var viewIds = names.listViewIds(accountId, cat);
    for (ResourceId viewId : viewIds) {
      nodes.view(viewId).ifPresent(out::add);
    }

    return out;
  }

  /**
   * Lists all relations (tables and views) in the specified namespace.
   *
   * @param catalogId the catalog containing the namespace
   * @param namespaceId the namespace to list relations from
   * @return list of all relations in the namespace
   */
  public List<RelationNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    List<RelationNode> out = new ArrayList<>(32);

    final String accountId = catalogId.getAccountId();
    final String cat = catalogId.getId();
    final String ns = namespaceId.getId();

    var tblIds = names.listTableIdsInNamespace(accountId, cat, ns);
    for (ResourceId tblId : tblIds) {
      nodes.table(tblId).ifPresent(out::add);
    }

    var viewIds = names.listViewIdsInNamespace(accountId, cat, ns);
    for (ResourceId viewId : viewIds) {
      nodes.view(viewId).ifPresent(out::add);
    }

    return out;
  }

  public List<FunctionNode> listFunctions(ResourceId namespaceId) {
    // User-defined functions are not yet implemented
    return List.of();
  }

  public List<TypeNode> listTypes(ResourceId catalogId) {
    // User-defined functions are not yet implemented
    return List.of();
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    List<ResourceId> ids = names.listNamespaces(catalogId.getAccountId(), catalogId.getId());
    return ids.stream().map(this::namespace).flatMap(Optional::stream).toList();
  }

  public List<UserTableNode> listTablesInCatalog(ResourceId catalogId) {
    List<ResourceId> ids = names.listTableIds(catalogId.getAccountId(), catalogId.getId());
    return ids.stream().map(this::table).flatMap(Optional::stream).toList();
  }

  public List<UserTableNode> listTablesInNamespace(ResourceId namespaceId) {
    List<ResourceId> ids =
        namespace(namespaceId)
            .map(
                ns ->
                    names.listTableIdsInNamespace(
                        namespaceId.getAccountId(), ns.catalogId().getId(), namespaceId.getId()))
            .orElseGet(List::of);
    return ids.stream().map(this::table).flatMap(Optional::stream).toList();
  }

  public List<ViewNode> listViewsInCatalog(ResourceId catalogId) {
    List<ResourceId> ids = names.listViewIds(catalogId.getAccountId(), catalogId.getId());
    return ids.stream().map(this::view).flatMap(Optional::stream).toList();
  }

  public List<ViewNode> listViewsInNamespace(ResourceId namespaceId) {
    List<ResourceId> ids =
        namespace(namespaceId)
            .map(
                ns ->
                    names.listViewIdsInNamespace(
                        namespaceId.getAccountId(), ns.catalogId().getId(), namespaceId.getId()))
            .orElseGet(List::of);
    return ids.stream().map(this::view).flatMap(Optional::stream).toList();
  }

  // ----------------------------------------------------------------------
  // Engine hints
  // ----------------------------------------------------------------------

  public Optional<EngineHint> engineHint(GraphNode node, EngineKey key, String type, String cid) {

    if (hints == null) {
      return Optional.empty();
    }
    EngineHintKey hintKey = new EngineHintKey(key.engineKind(), key.engineVersion(), type);
    return hints.get(node, hintKey, cid);
  }

  // ----------------------------------------------------------------------
  // Internal validation helpers
  // ----------------------------------------------------------------------

  private String requireAccountId(String cid) {
    var ctx = principal.get();
    if (ctx == null || ctx.getAccountId() == null)
      throw new IllegalStateException("MetadataGraph requires account context");

    return ctx.getAccountId();
  }

  private void validateNameRef(String cid, NameRef ref) {
    if (ref == null || ref.getCatalog().isBlank())
      throw GrpcErrors.invalidArgument(
          cid, GeneratedErrorMessages.MessageKey.CATALOG_MISSING, Map.of());
  }

  private void validateRelationName(String cid, NameRef ref, String type) {
    if (ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          cid, relationNameMissingKey(type), Map.of("name", ref.getName()));
    }
  }

  private GeneratedErrorMessages.MessageKey relationNameMissingKey(String type) {
    return switch (type) {
      case "table" -> GeneratedErrorMessages.MessageKey.TABLE_NAME_MISSING;
      case "view" -> GeneratedErrorMessages.MessageKey.VIEW_NAME_MISSING;
      default -> GeneratedErrorMessages.MessageKey.FIELD;
    };
  }

  // ----------------------------------------------------------------------
  // Helper methods for NameRef construction
  // ----------------------------------------------------------------------

  private NameRef buildNamespaceNameRef(NamespaceNode ns, String catalogName) {
    NameRef.Builder b = NameRef.newBuilder();
    b.setCatalog(catalogName);
    for (String p : ns.pathSegments()) {
      b.addPath(p);
    }
    b.setName(ns.displayName());
    b.setResourceId(ns.id());
    return b.build();
  }

  private NameRef buildRelationNameRef(
      String relName, ResourceId id, NamespaceNode ns, String catalogName) {
    NameRef.Builder b = NameRef.newBuilder();
    b.setCatalog(catalogName);
    for (String p : ns.pathSegments()) {
      b.addPath(p);
    }
    b.addPath(ns.displayName());
    b.setName(relName);
    b.setResourceId(id);
    return b.build();
  }

  // ----------------------------------------------------------------------
  // Re-expose selected types as inner types
  // ----------------------------------------------------------------------
  public static class ResolveResult {
    private final FullyQualifiedResolver.ResolveResult delegate;

    public ResolveResult(FullyQualifiedResolver.ResolveResult d) {
      this.delegate = d;
    }

    public List<QualifiedRelation> relations() {
      List<FullyQualifiedResolver.QualifiedRelation> fqList = delegate.relations();
      List<QualifiedRelation> out = new ArrayList<>(fqList.size());
      for (FullyQualifiedResolver.QualifiedRelation qr : fqList) {
        out.add(
            new QualifiedRelation(
                qr.name().toBuilder().setResourceId(qr.resourceId()).build(), qr.resourceId()));
      }
      return out;
    }

    public int totalSize() {
      return delegate.totalSize();
    }

    public String nextToken() {
      return delegate.nextToken();
    }
  }

  public static record QualifiedRelation(NameRef name, ResourceId resourceId) {}
}
