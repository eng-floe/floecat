package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Materialised graph of builtin system nodes for a specific engine kind/version. */
@ApplicationScoped
public final class SystemGraph {

  private static final String SYSTEM_ACCOUNT = "_system";
  private static final int DEFAULT_SNAPSHOT_CACHE_SIZE = 16;

  private final SystemNodeRegistry registry;
  private final Instant createdAt = Instant.now();
  private final Map<VersionKey, GraphSnapshot> snapshots;
  private final int snapshotCacheSize;

  @Inject
  public SystemGraph(
      SystemNodeRegistry registry,
      @ConfigProperty(name = "floecat.system.graph.snapshot-cache-size", defaultValue = "16")
          int cacheSize) {
    this.registry = registry;
    int effective = cacheSize <= 0 ? DEFAULT_SNAPSHOT_CACHE_SIZE : cacheSize;
    this.snapshotCacheSize = Math.max(1, effective);
    this.snapshots =
        Collections.synchronizedMap(
            new LinkedHashMap<VersionKey, GraphSnapshot>(snapshotCacheSize, 0.75f, true) {
              private static final long serialVersionUID = 1L;

              protected boolean removeEldestEntry(Map.Entry<VersionKey, GraphSnapshot> eldest) {
                return size() > SystemGraph.this.snapshotCacheSize;
              }
            });
  }

  /**
   * Resolves a node inside the synthetic system catalog for the requested engine parameters.
   *
   * <p>The snapshot exposes the prebuilt nodes by id so resolution remains fast (map lookup)
   * without reconstructing the node list on each call.
   */
  public Optional<GraphNode> resolve(ResourceId id, String engineKind, String engineVersion) {
    return snapshotFor(engineKind, engineVersion).resolve(id);
  }

  /**
   * Lists every namespace/table relation that belongs to the engine-catalog root.
   *
   * <p>The snapshot already groups namespace→relations so we can return the bucket without
   * recalculating it on every invocation.
   */
  public List<GraphNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {
    ResourceId expected = systemCatalogId(engineKind);
    if (catalogId == null || !catalogId.equals(expected)) {
      return List.of();
    }
    return snapshotFor(engineKind, engineVersion).tableRelations();
  }

  /**
   * Retrieves the pre-bucketed relations under a namespace so scanners can enumerate tables
   * quickly.
   */
  public List<GraphNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion) {
    return snapshotFor(engineKind, engineVersion)
        .relationsByNamespace()
        .getOrDefault(namespaceId, List.of());
  }

  public List<NamespaceNode> listNamespaces(
      ResourceId catalogId, String engineKind, String engineVersion) {
    ResourceId expected = systemCatalogId(engineKind);
    if (catalogId == null || !catalogId.equals(expected)) {
      return List.of();
    }
    return snapshotFor(engineKind, engineVersion).namespaceNodes();
  }

  public List<ResourceId> listCatalogs() {
    synchronized (snapshots) {
      return snapshots.keySet().stream()
          .map(key -> systemCatalogId(key.engineKind()))
          .distinct()
          .toList();
    }
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return snapshotForCatalog(catalogId).namespaceNodes();
  }

  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return resolveAny(id).filter(NamespaceNode.class::isInstance).map(NamespaceNode.class::cast);
  }

  public Optional<TableNode> tryTable(ResourceId id) {
    return Optional.empty();
  }

  public Optional<ViewNode> tryView(ResourceId id) {
    return Optional.empty();
  }

  public List<TableNode> listTables(ResourceId namespaceId) {
    return List.of();
  }

  public List<ViewNode> listViews(ResourceId namespaceId) {
    return List.of();
  }

  public Optional<String> tableSchemaJson(ResourceId tableId) {
    return Optional.empty();
  }

  /** Builds a new snapshot for the requested engine version. */
  private GraphSnapshot snapshotFor(String engineKind, String engineVersion) {
    if (engineKind == null
        || engineKind.isBlank()
        || engineVersion == null
        || engineVersion.isBlank()) {
      return GraphSnapshot.empty();
    }
    String normalizedKind = engineKind.toLowerCase(Locale.ROOT);
    VersionKey key = new VersionKey(normalizedKind, engineVersion);
    GraphSnapshot cached = snapshots.get(key);
    if (cached != null) {
      return cached;
    }
    GraphSnapshot snapshot = build(normalizedKind, engineVersion);
    snapshots.put(key, snapshot);
    return snapshot;
  }

  private GraphSnapshot snapshotForCatalog(ResourceId catalogId) {
    if (catalogId == null) return GraphSnapshot.empty();
    synchronized (snapshots) {
      for (Map.Entry<VersionKey, GraphSnapshot> entry : snapshots.entrySet()) {
        ResourceId candidate = systemCatalogId(entry.getKey().engineKind());
        if (catalogId.equals(candidate)) {
          return entry.getValue();
        }
      }
    }
    return GraphSnapshot.empty();
  }

  private Optional<GraphNode> resolveAny(ResourceId id) {
    if (id == null) return Optional.empty();
    synchronized (snapshots) {
      for (GraphSnapshot snapshot : snapshots.values()) {
        Optional<GraphNode> found = snapshot.resolve(id);
        if (found.isPresent()) {
          return found;
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Constructs the snapshot by gathering namespace/table nodes from the registry and organizing
   * them per namespace/catalog.
   */
  private GraphSnapshot build(String engineKind, String engineVersion) {
    var nodes = registry.nodesFor(engineKind, engineVersion);
    if (nodes == null) {
      return GraphSnapshot.empty();
    }

    SystemCatalogData catalog = nodes.toCatalogData();
    long version = versionFromFingerprint(nodes.fingerprint());
    ResourceId catalogId = systemCatalogId(engineKind);

    Map<ResourceId, GraphNode> nodesById = new ConcurrentHashMap<>(nodesById(nodes));
    Map<ResourceId, List<GraphNode>> relationsByNamespace = new ConcurrentHashMap<>();
    Map<ResourceId, List<ResourceId>> relationIdsByNamespace = new ConcurrentHashMap<>();
    List<GraphNode> tableRelations = new ArrayList<>();

    Map<String, ResourceId> namespaceIds =
        catalog.namespaces().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ns -> NameRefUtil.canonical(ns.name()),
                    ns ->
                        SystemNodeRegistry.resourceId(
                            engineKind, ResourceKind.RK_NAMESPACE, ns.name())));

    // Build table nodes and map them to namespaces
    for (SystemTableDef table : catalog.tables()) {
      Optional<ResourceId> namespaceId = findNamespaceId(table.name(), namespaceIds);
      if (namespaceId.isEmpty()) {
        continue;
      }
      ResourceId tableId =
          SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TABLE, table.name());
      SystemTableNode node =
          new SystemTableNode(
              tableId,
              version,
              createdAt,
              engineVersion,
              table.displayName(),
              namespaceId.get(),
              table.columns(),
              table.scannerId(),
              Map.of());
      nodesById.put(tableId, node);
      relationsByNamespace
          .computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>())
          .add(node);
      relationIdsByNamespace
          .computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>())
          .add(tableId);
      tableRelations.add(node);
    }

    // Build namespace nodes with relation ids attached
    List<NamespaceNode> namespaceNodes = new ArrayList<>();
    for (SystemNamespaceDef ns : catalog.namespaces()) {
      ResourceId namespaceId =
          SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_NAMESPACE, ns.name());
      List<ResourceId> relations = relationIdsByNamespace.getOrDefault(namespaceId, List.of());
      NamespaceNode node =
          new NamespaceNode(
              namespaceId,
              version,
              createdAt,
              catalogId,
              List.copyOf(ns.name().getPathList()),
              ns.displayName(),
              GraphNodeOrigin.SYSTEM,
              Map.of(),
              relations.isEmpty() ? Optional.empty() : Optional.of(List.copyOf(relations)),
              Map.of());
      nodesById.put(namespaceId, node);
      namespaceNodes.add(node);
      relationsByNamespace.computeIfAbsent(namespaceId, ignored -> List.of());
    }

    return new GraphSnapshot(
        List.copyOf(namespaceNodes),
        List.copyOf(tableRelations),
        relationsByNamespace.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))),
        Map.copyOf(nodesById));
  }

  private static Map<ResourceId, GraphNode> nodesById(BuiltinNodes nodes) {
    Map<ResourceId, GraphNode> entries = new ConcurrentHashMap<>();
    entries.putAll(entriesFromList(nodes.functions()));
    entries.putAll(entriesFromList(nodes.operators()));
    entries.putAll(entriesFromList(nodes.types()));
    entries.putAll(entriesFromList(nodes.casts()));
    entries.putAll(entriesFromList(nodes.collations()));
    entries.putAll(entriesFromList(nodes.aggregates()));
    return entries;
  }

  private static Map<ResourceId, GraphNode> entriesFromList(List<? extends GraphNode> nodes) {
    return nodes.stream().collect(Collectors.toUnmodifiableMap(GraphNode::id, node -> node));
  }

  private static ResourceId systemCatalogId(String engineKind) {
    return ResourceId.newBuilder()
        .setAccountId(SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId(engineKind + ":system")
        .build();
  }

  private static Optional<ResourceId> findNamespaceId(
      NameRef name, Map<String, ResourceId> namespaceIds) {
    String canonical = NameRefUtil.canonical(name);
    int idx = canonical.lastIndexOf('.');
    String namespaceKey = idx < 0 ? canonical : canonical.substring(0, idx);
    if (namespaceKey.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(namespaceIds.get(namespaceKey));
  }

  private static long versionFromFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.isBlank()) {
      return 0L;
    }
    String prefix = fingerprint.length() >= 16 ? fingerprint.substring(0, 16) : fingerprint;
    try {
      return Long.parseUnsignedLong(prefix, 16);
    } catch (NumberFormatException e) {
      return prefix.hashCode();
    }
  }

  /**
   * Lightweight view that stores the grouped relations + lookup map for the synthetic catalog.
   *
   * <p>It keeps namespace buckets and the catalog list so `listRelations*` can return results
   * without recomputing the grouping logic.
   */
  private static final class GraphSnapshot {

    private static final GraphSnapshot EMPTY =
        new GraphSnapshot(List.of(), List.of(), Map.of(), Map.of());

    private final List<NamespaceNode> namespaceNodes;
    private final List<GraphNode> tableRelations;
    private final Map<ResourceId, List<GraphNode>> relationsByNamespace;
    private final Map<ResourceId, GraphNode> nodesById;

    GraphSnapshot(
        List<NamespaceNode> namespaceNodes,
        List<GraphNode> tableRelations,
        Map<ResourceId, List<GraphNode>> relationsByNamespace,
        Map<ResourceId, GraphNode> nodesById) {
      this.namespaceNodes = namespaceNodes;
      this.tableRelations = tableRelations;
      this.relationsByNamespace = relationsByNamespace;
      this.nodesById = nodesById;
    }

    static GraphSnapshot empty() {
      return EMPTY;
    }

    List<NamespaceNode> namespaceNodes() {
      return namespaceNodes;
    }

    List<GraphNode> tableRelations() {
      return tableRelations;
    }

    Map<ResourceId, List<GraphNode>> relationsByNamespace() {
      return relationsByNamespace;
    }

    Optional<GraphNode> resolve(ResourceId id) {
      return Optional.ofNullable(nodesById.get(id));
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
