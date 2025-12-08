package ai.floedb.floecat.service.metagraph;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc.SnapshotServiceBlockingStub;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.cache.GraphCacheKey;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.cache.GraphCacheManager;
import ai.floedb.floecat.service.metagraph.hint.EngineHintManager;
import ai.floedb.floecat.service.metagraph.loader.NodeLoader;
import ai.floedb.floecat.service.metagraph.resolver.FullyQualifiedResolver;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver.ResolvedRelation;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * MetadataGraph orchestrates cached access to {@link RelationNode}s.
 *
 * <p>This class delegates cache lifecycle, repository lookups, and snapshot semantics to helper
 * components so higher layers can reason about catalogs/namespaces/tables/views without issuing
 * redundant storage calls. See {@code docs/metadata-graph.md} for an architectural overview and
 * usage guidelines.
 */
@ApplicationScoped
public class MetadataGraph {

  private static final Logger LOG = Logger.getLogger(MetadataGraph.class);

  private final GraphCacheManager cacheManager;
  private final NodeLoader nodeLoader;
  private final NameResolver nameResolver;
  private final FullyQualifiedResolver fqResolver;
  private final SnapshotHelper snapshotHelper;
  private final EngineHintManager engineHintManager;
  private final BuiltinNodeRegistry builtinNodeRegistry;
  private PrincipalProvider principalProvider;
  private final MeterRegistry meterRegistry;
  private Timer loadTimer;

  public MetadataGraph(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      SnapshotRepository snapshotRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository) {
    this(
        catalogRepository,
        namespaceRepository,
        snapshotRepository,
        tableRepository,
        viewRepository,
        null,
        null,
        null,
        50_000L,
        null,
        null);
  }

  @Inject
  public MetadataGraph(
      CatalogRepository catalogRepository,
      NamespaceRepository namespaceRepository,
      SnapshotRepository snapshotRepository,
      TableRepository tableRepository,
      ViewRepository viewRepository,
      @GrpcClient("floecat") SnapshotServiceBlockingStub snapshotStub,
      MeterRegistry meterRegistry,
      PrincipalProvider principalProvider,
      @ConfigProperty(name = "floecat.metadata.graph.cache-max-size", defaultValue = "50000")
          long cacheMaxSize,
      EngineHintManager engineHintManager,
      BuiltinNodeRegistry builtinNodeRegistry) {
    this.meterRegistry = meterRegistry;
    this.cacheManager = new GraphCacheManager(cacheMaxSize > 0, cacheMaxSize, meterRegistry);
    this.nodeLoader =
        new NodeLoader(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    this.nameResolver =
        new NameResolver(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    this.fqResolver =
        new FullyQualifiedResolver(
            catalogRepository, namespaceRepository, tableRepository, viewRepository);
    this.snapshotHelper = new SnapshotHelper(snapshotRepository, snapshotStub);
    this.engineHintManager = engineHintManager;
    this.builtinNodeRegistry = builtinNodeRegistry;
    this.principalProvider = principalProvider;
  }

  @PostConstruct
  void initMetrics() {
    if (meterRegistry != null) {
      this.loadTimer = meterRegistry.timer("floecat.metadata.graph.load");
    }
  }

  /**
   * Resolve a resource into its cached {@link RelationNode}.
   *
   * @param id resource identifier (must include kind + account)
   * @return optional node (empty when resource is missing or not yet supported)
   */
  public Optional<RelationNode> resolve(ResourceId id) {
    Optional<MutationMeta> metaOpt = nodeLoader.mutationMeta(id);
    if (metaOpt.isEmpty()) {
      return Optional.empty();
    }
    MutationMeta meta = metaOpt.get();
    GraphCacheKey key = new GraphCacheKey(id, meta.getPointerVersion());
    RelationNode cached = cacheManager.get(id, key);
    if (cached != null) {
      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "MetadataGraph cache hit for %s (version=%d)", id.getId(), meta.getPointerVersion());
      }
      return Optional.of(cached);
    }
    Timer.Sample sample =
        loadTimer != null && meterRegistry != null ? Timer.start(meterRegistry) : null;
    Optional<RelationNode> loaded = nodeLoader.load(id, meta);
    loaded.ifPresent(node -> cacheManager.put(id, key, node));
    if (sample != null && loadTimer != null) {
      sample.stop(loadTimer);
    }
    if (loaded.isEmpty() && LOG.isTraceEnabled()) {
      LOG.tracef(
          "MetadataGraph miss for %s (version=%d) returned empty",
          id.getId(), meta.getPointerVersion());
    }
    return loaded;
  }

  /** Convenience wrapper returning a {@link CatalogNode}. */
  public Optional<CatalogNode> catalog(ResourceId id) {
    return resolve(id).filter(CatalogNode.class::isInstance).map(CatalogNode.class::cast);
  }

  /** Convenience wrapper returning a {@link NamespaceNode}. */
  public Optional<NamespaceNode> namespace(ResourceId id) {
    return resolve(id).filter(NamespaceNode.class::isInstance).map(NamespaceNode.class::cast);
  }

  /** Convenience wrapper returning a {@link TableNode}. */
  public Optional<TableNode> table(ResourceId id) {
    return resolve(id).filter(TableNode.class::isInstance).map(TableNode.class::cast);
  }

  /** Convenience wrapper returning a {@link ViewNode}. */
  public Optional<ViewNode> view(ResourceId id) {
    return resolve(id).filter(ViewNode.class::isInstance).map(ViewNode.class::cast);
  }

  /** Computes a {@link SnapshotPin} honoring overrides and default AS OF timestamps. */
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    return snapshotHelper.snapshotPinFor(correlationId, tableId, override, asOfDefault);
  }

  /** Evict every cached version of the provided resource. */
  public void invalidate(ResourceId id) {
    cacheManager.invalidate(id);
    if (engineHintManager != null) {
      engineHintManager.invalidate(id);
    }
  }

  /** Fetches an engine-specific hint for the provided relation node, when available. */
  public Optional<EngineHint> engineHint(
      RelationNode node, EngineKey engineKey, String hintType, String correlationId) {
    if (engineHintManager == null) {
      return Optional.empty();
    }
    return engineHintManager.get(node, engineKey, hintType, correlationId);
  }

  public BuiltinNodeRegistry.BuiltinNodes builtinNodes(String engineKind, String engineVersion) {
    if (builtinNodeRegistry == null) {
      throw new IllegalStateException("BuiltinNodeRegistry not configured");
    }
    return builtinNodeRegistry.nodesFor(engineKind, engineVersion);
  }

  /** Test-only hook for overriding snapshot client. */
  void setSnapshotClient(SnapshotHelper.SnapshotClient client) {
    snapshotHelper.setSnapshotClient(client);
  }

  /** Test-only hook for overriding principal provider. */
  void setPrincipalProvider(PrincipalProvider provider) {
    this.principalProvider = provider;
  }

  /** Resolves a {@link NameRef} into a {@link ResourceId}, mirroring DirectoryService semantics. */
  public ResourceId resolveName(String correlationId, NameRef ref) {
    validateNameRef(correlationId, ref);

    if (ref.hasResourceId()) {
      return ref.getResourceId();
    }

    String accountId = requireAccountId(correlationId);

    Optional<ResolvedRelation> tableResolved = nameResolver.resolveTableRelation(accountId, ref);
    Optional<ResolvedRelation> viewResolved = nameResolver.resolveViewRelation(accountId, ref);

    if (tableResolved.isPresent() && viewResolved.isPresent()) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.input.ambiguous", Map.of("name", ref.toString()));
    }

    if (tableResolved.isPresent()) {
      return tableResolved.get().resourceId();
    }

    if (viewResolved.isPresent()) {
      return viewResolved.get().resourceId();
    }

    throw GrpcErrors.invalidArgument(
        correlationId, "query.input.unresolved", Map.of("name", ref.toString()));
  }

  /** Resolve a catalog display name into its {@link ResourceId}. */
  public ResourceId resolveCatalog(String correlationId, String catalogName) {
    String accountId = requireAccountId(correlationId);
    return nameResolver.resolveCatalogId(correlationId, accountId, catalogName);
  }

  /** Resolve a namespace reference (catalog + path/name) into its {@link ResourceId}. */
  public ResourceId resolveNamespace(String correlationId, NameRef ref) {
    validateNameRef(correlationId, ref);
    String accountId = requireAccountId(correlationId);
    return nameResolver.resolveNamespaceId(correlationId, accountId, ref);
  }

  /** Resolve a table reference into its {@link ResourceId}. */
  public ResourceId resolveTable(String correlationId, NameRef ref) {
    validateNameRef(correlationId, ref);
    validateRelationName(correlationId, ref, "table");
    String accountId = requireAccountId(correlationId);
    return nameResolver.resolveTableId(correlationId, accountId, ref);
  }

  /** Resolve a view reference into its {@link ResourceId}. */
  public ResourceId resolveView(String correlationId, NameRef ref) {
    validateNameRef(correlationId, ref);
    validateRelationName(correlationId, ref, "view");
    String accountId = requireAccountId(correlationId);
    return nameResolver.resolveViewId(correlationId, accountId, ref);
  }

  /** Build a {@link NameRef} for the provided namespace identifier (if present). */
  public Optional<NameRef> namespaceName(ResourceId namespaceId) {
    return namespace(namespaceId)
        .flatMap(
            ns ->
                catalog(ns.catalogId())
                    .map(catalog -> buildNamespaceNameRef(ns, catalog.displayName())));
  }

  /** Build a {@link NameRef} for the provided table identifier (if present). */
  public Optional<NameRef> tableName(ResourceId tableId) {
    return table(tableId)
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

  /** Build a {@link NameRef} for the provided view identifier (if present). */
  public Optional<NameRef> viewName(ResourceId viewId) {
    return view(viewId)
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

  /** Result container for ResolveFQ helper methods. */
  public record ResolveResult(
      List<QualifiedRelation> relations, int totalSize, String nextPageToken) {

    public ResolveResult {
      relations = List.copyOf(relations);
      nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
  }

  /** Pairing of a resolved {@link NameRef} with its {@link ResourceId}. */
  public record QualifiedRelation(NameRef name, ResourceId resourceId) {}

  public ResolveResult resolveTables(
      String correlationId, List<NameRef> names, int limit, String pageToken) {
    String accountId = requireAccountId(correlationId);
    return fqResolver.resolveTableList(correlationId, accountId, names, limit, pageToken);
  }

  public ResolveResult resolveTables(
      String correlationId, NameRef prefix, int limit, String pageToken) {
    validateNameRef(correlationId, prefix);
    String accountId = requireAccountId(correlationId);
    return fqResolver.resolveTablesByPrefix(correlationId, accountId, prefix, limit, pageToken);
  }

  public ResolveResult resolveViews(
      String correlationId, List<NameRef> names, int limit, String pageToken) {
    String accountId = requireAccountId(correlationId);
    return fqResolver.resolveViewList(correlationId, accountId, names, limit, pageToken);
  }

  public ResolveResult resolveViews(
      String correlationId, NameRef prefix, int limit, String pageToken) {
    validateNameRef(correlationId, prefix);
    String accountId = requireAccountId(correlationId);
    return fqResolver.resolveViewsByPrefix(correlationId, accountId, prefix, limit, pageToken);
  }

  private NameRef buildNamespaceNameRef(NamespaceNode namespace, String catalogDisplayName) {
    return NameRef.newBuilder()
        .setCatalog(catalogDisplayName)
        .addAllPath(namespace.pathSegments())
        .setName(namespace.displayName())
        .setResourceId(namespace.id())
        .build();
  }

  private NameRef buildRelationNameRef(
      String relationDisplayName,
      ResourceId relationId,
      NamespaceNode namespace,
      String catalogDisplayName) {
    List<String> path = new java.util.ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      path.add(namespace.displayName());
    }
    return NameRef.newBuilder()
        .setCatalog(catalogDisplayName)
        .addAllPath(path)
        .setName(relationDisplayName)
        .setResourceId(relationId)
        .build();
  }

  private void validateNameRef(String correlationId, NameRef ref) {
    if (ref == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId, "catalog.missing", Map.of());
    }
  }

  private void validateRelationName(String correlationId, NameRef ref, String type) {
    if (ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId, type + ".name.missing", Map.of("name", ref.getName()));
    }
  }

  private String requireAccountId(String correlationId) {
    var principalContext = principalProvider.get();
    if (principalContext == null || principalContext.getAccountId() == null) {
      throw new IllegalStateException("MetadataGraph requires account context");
    }
    return principalContext.getAccountId();
  }

  /**
   * Returns the {@link TableNode} and effective schema JSON for the provided table/snapshot
   * combination.
   */
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    TableNode node =
        table(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId())));
    return snapshotHelper.schemaFor(correlationId, node, snapshot, node::schemaJson);
  }

  /**
   * Computes the effective schema JSON for a {@link TableNode}, honoring optional snapshot
   * overrides. Falls back to the table-level schema when the snapshot payload is missing/blank.
   */
  public String schemaJsonFor(String correlationId, TableNode tableNode, SnapshotRef snapshot) {
    return snapshotHelper.schemaJsonFor(correlationId, tableNode, snapshot, tableNode::schemaJson);
  }

  /** Bundles the table node and schema JSON for callers that need both. */
  public record SchemaResolution(TableNode table, String schemaJson) {}
}
