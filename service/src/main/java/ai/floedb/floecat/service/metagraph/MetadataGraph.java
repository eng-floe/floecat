package ai.floedb.floecat.service.metagraph;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry;
import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc.SnapshotServiceBlockingStub;
import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectDefinition;
import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectResolver;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.cache.GraphCacheKey;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.context.impl.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.cache.GraphCacheManager;
import ai.floedb.floecat.service.metagraph.hint.EngineHintManager;
import ai.floedb.floecat.service.metagraph.loader.NodeLoader;
import ai.floedb.floecat.service.metagraph.resolver.FullyQualifiedResolver;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver.ResolvedRelation;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.metagraph.traversal.RelationLister;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.Timestamp;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * ============================================================================ MetadataGraph
 * (Façade) ============================================================================
 *
 * <p>Lightweight, cached, read-only façade over metadata.
 *
 * <p>Responsibilities: - Resolve nodes (table, view, namespace, catalog) - Resolve system objects -
 * Resolve snapshots - Resolve names + FQ prefix/list operations - Delegate all enumeration +
 * traversal to RelationLister
 *
 * <p>This class MUST remain thin: X no repository calls X no manual traversal X no system-object
 * iteration X no list logic
 */
@ApplicationScoped
public final class MetadataGraph implements MetadataGraphClient {

  // ----------------------------------------------------------------------
  // Dependencies (constructed once)
  // ----------------------------------------------------------------------

  private final GraphCacheManager cache;
  private final NodeLoader nodes;
  private final NameResolver names;
  private final FullyQualifiedResolver fq;
  private SnapshotHelper snapshots;
  private EngineHintManager hints;
  private RelationLister lister;
  private SystemObjectResolver sysObjects;
  private EngineContextProvider engine;
  private BuiltinNodeRegistry builtin;
  private PrincipalProvider principal;

  private Timer loadTimer;

  // ----------------------------------------------------------------------
  // Constructor
  // ----------------------------------------------------------------------

  @Inject
  public MetadataGraph(
      CatalogRepository catalogRepo,
      NamespaceRepository nsRepo,
      SnapshotRepository snapshotRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo,
      @GrpcClient("floecat") SnapshotServiceBlockingStub snapshotStub,
      MeterRegistry meter,
      PrincipalProvider principal,
      @ConfigProperty(name = "floecat.metadata.graph.cache-max-size", defaultValue = "50000")
          long cacheMaxSize,
      EngineHintManager engineHints,
      BuiltinNodeRegistry builtin,
      SystemObjectResolver sysObjects,
      EngineContextProvider engineCtx,
      RelationLister lister) {

    this.cache = new GraphCacheManager(cacheMaxSize > 0, cacheMaxSize, meter);
    this.nodes = new NodeLoader(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.names = new NameResolver(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.fq = new FullyQualifiedResolver(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.snapshots = new SnapshotHelper(snapshotRepo, snapshotStub);
    this.hints = engineHints;
    this.sysObjects = sysObjects;
    this.engine = engineCtx;
    this.principal = principal;
    this.lister = lister;
    this.builtin = builtin;
  }

  /** TEST-ONLY constructor */
  public MetadataGraph(
      CatalogRepository catalogRepo,
      NamespaceRepository nsRepo,
      SnapshotRepository snapshotRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo) {

    this.cache = new GraphCacheManager(true, 1024, null);
    this.nodes = new NodeLoader(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.names = new NameResolver(catalogRepo, nsRepo, tableRepo, viewRepo);
    this.fq = new FullyQualifiedResolver(catalogRepo, nsRepo, tableRepo, viewRepo);

    // Snapshot helper without gRPC client
    this.snapshots = new SnapshotHelper(snapshotRepo, null);

    // Fake collaborators for tests
    this.engine =
        new EngineContextProvider() {
          @Override
          public String engineKind() {
            return "test";
          }

          @Override
          public String engineVersion() {
            return "0.0";
          }
        };
    this.principal =
        new PrincipalProvider() {
          @Override
          public PrincipalContext get() {
            return PrincipalContext.newBuilder().setAccountId("account").build();
          }
        };
    this.sysObjects = new SystemObjectResolver(); // Not SystemObjectRegistry!!
    this.lister = new RelationLister(names, nodes, sysObjects);

    this.hints = null;
    this.builtin = null;
    this.loadTimer = null;
  }

  public void invalidate(ResourceId id) {
    cache.invalidate(id);
  }

  public String currentAccountId() {
    return requireAccountId("internal");
  }

  public List<ResourceId> listAllCatalogIds(String accountId) {
    return nodes.listCatalogIds(accountId);
  }

  public Optional<NameRef> namespaceName(ResourceId id) {
    return namespace(id)
        .flatMap(
            ns -> catalog(ns.catalogId()).map(cat -> buildNamespaceNameRef(ns, cat.displayName())));
  }

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

  @PostConstruct
  void initMetrics() {
    if (cache.meterRegistry() != null) {
      this.loadTimer = cache.meterRegistry().timer("floecat.metadata.graph.load");
    }
  }

  // ----------------------------------------------------------------------
  // Node resolution (cached)
  // ----------------------------------------------------------------------

  public Optional<GraphNode> resolve(ResourceId id) {

    // ----- System objects bypass (no cache in graph) --------------------------
    if (id.getKind() == ResourceKind.RK_SYSTEM_OBJECT) {
      return sysObjects
          .resolve(id, engine.engineKind(), engine.engineVersion())
          .flatMap(def -> sysObjects.buildNode(def, engine.engineKind(), engine.engineVersion()));
    }

    // ----- Regular nodes (cached in graph) ------------------------------------
    Optional<MutationMeta> metaOpt = nodes.mutationMeta(id);
    if (metaOpt.isEmpty()) return Optional.empty();

    MutationMeta meta = metaOpt.get();
    GraphCacheKey key = new GraphCacheKey(id, meta.getPointerVersion());

    GraphNode cached = cache.get(id, key);
    if (cached != null) return Optional.of(cached);

    Timer.Sample sample = (loadTimer != null) ? Timer.start(cache.meterRegistry()) : null;

    Optional<GraphNode> loaded = nodes.load(id, meta);
    loaded.ifPresent(node -> cache.put(id, key, node));

    if (sample != null) sample.stop(loadTimer);

    return loaded;
  }

  // Convenience typed resolvers
  public Optional<CatalogNode> catalog(ResourceId id) {
    return resolve(id).map(CatalogNode.class::cast);
  }

  public Optional<NamespaceNode> namespace(ResourceId id) {
    return resolve(id).map(NamespaceNode.class::cast);
  }

  public Optional<TableNode> table(ResourceId id) {
    return resolve(id).filter(TableNode.class::isInstance).map(TableNode.class::cast);
  }

  public Optional<ViewNode> view(ResourceId id) {
    return resolve(id).filter(ViewNode.class::isInstance).map(ViewNode.class::cast);
  }

  // ----------------------------------------------------------------------
  // Snapshot resolution
  // ----------------------------------------------------------------------

  @Override
  public SnapshotPin snapshotPinFor(
      String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault) {

    return snapshots.snapshotPinFor(cid, tableId, override, asOfDefault);
  }

  @Override
  public ResourceId resolveName(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    String accountId = requireAccountId(cid);

    // system object?
    Optional<SystemObjectDefinition> sys =
        sysObjects.resolveDefinition(ref, engine.engineKind(), engine.engineVersion());
    if (sys.isPresent()) {
      return sysObjects.toResourceId(sys.get(), engine.engineKind());
    }

    // table / view?
    Optional<ResolvedRelation> t = names.resolveTableRelation(accountId, ref);
    Optional<ResolvedRelation> v = names.resolveViewRelation(accountId, ref);

    if (t.isPresent() && v.isPresent()) {
      throw GrpcErrors.invalidArgument(
          cid, "query.input.ambiguous", Map.of("name", ref.toString()));
    }

    return t.map(ResolvedRelation::resourceId)
        .or(() -> v.map(ResolvedRelation::resourceId))
        .orElseThrow(
            () ->
                GrpcErrors.invalidArgument(
                    cid, "query.input.unresolved", Map.of("name", ref.toString())));
  }

  public String schemaJsonFor(String cid, TableNode tbl, SnapshotRef snapshot) {
    return snapshots.schemaJsonFor(cid, tbl, snapshot, tbl::schemaJson);
  }

  public SchemaResolution schemaFor(String cid, ResourceId tblId, SnapshotRef snapshot) {
    TableNode tbl =
        table(tblId)
            .orElseThrow(() -> GrpcErrors.notFound(cid, "table", Map.of("id", tblId.getId())));
    return new SchemaResolution(tbl, schemaJsonFor(cid, tbl, snapshot));
  }

  public record SchemaResolution(TableNode table, String schemaJson) {}

  // ----------------------------------------------------------------------
  // Name resolution
  // ----------------------------------------------------------------------

  public ResourceId resolveCatalog(String cid, String name) {
    return names.resolveCatalogId(cid, requireAccountId(cid), name);
  }

  public ResourceId resolveNamespace(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    return names.resolveNamespaceId(cid, requireAccountId(cid), ref);
  }

  public ResourceId resolveTable(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "table");
    return names.resolveTableId(cid, requireAccountId(cid), ref);
  }

  public ResourceId resolveView(String cid, NameRef ref) {
    validateNameRef(cid, ref);
    validateRelationName(cid, ref, "view");
    return names.resolveViewId(cid, requireAccountId(cid), ref);
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

  // ----------------------------------------------------------------------
  // Unified relation listing (tables + views + sysobjects)
  // ----------------------------------------------------------------------

  public List<GraphNode> listRelations(ResourceId catalogId) {
    return lister.listRelations(catalogId, engine.engineKind(), engine.engineVersion());
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    List<ResourceId> ids = names.listNamespaces(catalogId.getAccountId(), catalogId.getId());
    return ids.stream().map(this::namespace).flatMap(Optional::stream).toList();
  }

  public List<TableNode> listTablesInCatalog(ResourceId catalogId) {
    List<ResourceId> ids = names.listTableIds(catalogId.getAccountId(), catalogId.getId());
    return ids.stream().map(this::table).flatMap(Optional::stream).toList();
  }

  public List<TableNode> listTablesInNamespace(ResourceId namespaceId) {
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
  // Builtin Objects listing
  // ----------------------------------------------------------------------
  public BuiltinNodes builtinNodes(String engineKind, String engineVersion) {
    return builtin.nodesFor(engineKind, engineVersion);
  }

  public List<GraphNode> asGraphNodes(BuiltinNodes nodes) {
    List<GraphNode> out = new ArrayList<>();
    out.addAll(nodes.functions());
    out.addAll(nodes.operators());
    out.addAll(nodes.types());
    out.addAll(nodes.casts());
    out.addAll(nodes.collations());
    out.addAll(nodes.aggregates());
    return out;
  }

  // ----------------------------------------------------------------------
  // Engine hints
  // ----------------------------------------------------------------------

  public Optional<EngineHint> engineHint(GraphNode node, EngineKey key, String type, String cid) {

    return hints == null ? Optional.empty() : hints.get(node, key, type, cid);
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
      throw GrpcErrors.invalidArgument(cid, "catalog.missing", Map.of());
  }

  private void validateRelationName(String cid, NameRef ref, String type) {
    if (ref.getName().isBlank())
      throw GrpcErrors.invalidArgument(cid, type + ".name.missing", Map.of("name", ref.getName()));
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
  // Dependency setters (for injection/configuration)
  // ----------------------------------------------------------------------
  public void setSnapshotHelper(SnapshotHelper helper) {
    this.snapshots = helper;
  }

  public void setPrincipalProvider(PrincipalProvider principal) {
    this.principal = principal;
  }

  public void setEngineContextProvider(EngineContextProvider engine) {
    this.engine = engine;
  }

  public void setSystemObjectRegistry(SystemObjectResolver sysObjects) {
    this.sysObjects = sysObjects;
  }

  public void setBuiltinRegistry(BuiltinNodeRegistry builtin) {
    this.builtin = builtin;
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
