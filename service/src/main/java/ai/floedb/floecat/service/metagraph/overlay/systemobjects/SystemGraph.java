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

package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
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
import java.util.stream.Stream;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Materialised graph of builtin system nodes for a specific engine kind/version. */
@ApplicationScoped
public final class SystemGraph {
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

  public Optional<CatalogNode> catalog(ResourceId id, String engineKind, String engineVersion) {
    return snapshotFor(engineKind, engineVersion).catalog(id);
  }

  /**
   * Lists every system view/table relation that belongs to the engine-catalog root.
   *
   * <p>The snapshot already groups namespace→relations so we can return the bucket without
   * recalculating it on every invocation.
   */
  public List<GraphNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {

    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);

    return snapshot.namespaces().stream()
        .flatMap(
            ns ->
                Stream.concat(
                    snapshot.tablesInNamespace(ns.id()).stream(),
                    snapshot.viewsInNamespace(ns.id()).stream()))
        .map(GraphNode.class::cast)
        .toList();
  }

  /**
   * Retrieves the pre-bucketed relations under a namespace so scanners can enumerate tables
   * quickly.
   */
  public List<GraphNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);

    return Stream.concat(
            snapshot.tablesInNamespace(namespaceId).stream(),
            snapshot.viewsInNamespace(namespaceId).stream())
        .map(GraphNode.class::cast)
        .toList();
  }

  public List<FunctionNode> listFunctions(
      ResourceId namespaceId, String engineKind, String engineVersion) {
    return snapshotFor(engineKind, engineVersion).functionsInNamespace(namespaceId);
  }

  /**
   * Returns all namespaces in the snapshot.
   *
   * <p>The snapshot is already scoped to a single system catalog, so {@code catalogId} is not used
   * for filtering here.
   */
  public List<NamespaceNode> listNamespaces(
      ResourceId catalogId, String engineKind, String engineVersion) {
    return snapshotFor(engineKind, engineVersion).namespaces();
  }

  public List<TypeNode> listTypes(ResourceId catalogId, String engineKind, String engineVersion) {
    return registry.nodesFor(engineKind, engineVersion).types();
  }

  public List<ResourceId> listCatalogs() {
    return registry.engineKinds().stream().map(SystemGraph::systemCatalogId).toList();
  }

  /**
   * Resolves a table by name reference in the system catalog.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved table resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveTable(NameRef ref, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.tableNames().get(canonical));
  }

  /**
   * Resolves a view by name reference in the system catalog.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved view resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveView(NameRef ref, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.viewNames().get(canonical));
  }

  /**
   * Resolves a namespace by name reference in the system catalog.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved namespace resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveNamespace(
      NameRef ref, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.namespaceNames().get(canonical));
  }

  /**
   * Resolves a relation (table, view, or namespace) by name reference in the system catalog.
   *
   * <p>Tries table, view, then namespace in order.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveName(NameRef ref, String engineKind, String engineVersion) {
    // For simplicity, try table, view, namespace in order
    return resolveTable(ref, engineKind, engineVersion)
        .or(() -> resolveView(ref, engineKind, engineVersion))
        .or(() -> resolveNamespace(ref, engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a table by its resource ID.
   *
   * @param id the table resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> tableName(ResourceId id, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    return snapshot
        .resolve(id)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .flatMap(table -> buildTableNameRef(table, engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a view by its resource ID.
   *
   * @param id the view resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> viewName(ResourceId id, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    return snapshot
        .resolve(id)
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .flatMap(view -> buildViewNameRef(view, engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a namespace by its resource ID.
   *
   * @param id the namespace resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> namespaceName(ResourceId id, String engineKind, String engineVersion) {
    GraphSnapshot snapshot = snapshotFor(engineKind, engineVersion);
    return snapshot
        .resolve(id)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(ns -> buildNamespaceNameRef(ns, engineKind));
  }

  private Optional<NameRef> buildTableNameRef(
      TableNode table, String engineKind, String engineVersion) {
    return resolve(table.namespaceId(), engineKind, engineVersion)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(
            ns ->
                NameRefUtil.name(ns.displayName(), table.displayName()).toBuilder()
                    .setCatalog(engineKind)
                    .build());
  }

  private Optional<NameRef> buildViewNameRef(
      ViewNode view, String engineKind, String engineVersion) {
    return resolve(view.namespaceId(), engineKind, engineVersion)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(
            ns ->
                NameRefUtil.name(ns.displayName(), view.displayName()).toBuilder()
                    .setCatalog(engineKind)
                    .build());
  }

  private NameRef buildNamespaceNameRef(NamespaceNode ns, String engineKind) {
    return NameRef.newBuilder().setCatalog(engineKind).addPath(ns.displayName()).build();
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return snapshotForCatalog(catalogId).namespaces();
  }

  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return resolveAny(id).filter(NamespaceNode.class::isInstance).map(NamespaceNode.class::cast);
  }

  /** Builds a new snapshot for the requested engine version. */
  private GraphSnapshot snapshotFor(String engineKind, String engineVersion) {
    String normalizedKind =
        (engineKind == null || engineKind.isBlank())
            ? SystemNodeRegistry.FLOECAT_DEFAULT_CATALOG
            : engineKind.toLowerCase(Locale.ROOT);
    String normalizedVersion = engineVersion == null ? "" : engineVersion;

    VersionKey key = new VersionKey(normalizedKind, normalizedVersion);
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

    CatalogNode catalogNode =
        new CatalogNode(
            catalogId,
            version,
            createdAt,
            engineKind,
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());

    Map<ResourceId, GraphNode> nodesById = new ConcurrentHashMap<>(nodesById(nodes));
    nodesById.put(catalogId, catalogNode);
    Map<ResourceId, List<TableNode>> tablesByNamespace = new ConcurrentHashMap<>();
    Map<ResourceId, List<ViewNode>> viewsByNamespace = new ConcurrentHashMap<>();
    Map<ResourceId, List<ResourceId>> relationIdsByNamespace = new ConcurrentHashMap<>();
    List<NamespaceNode> namespaceNodes = new ArrayList<>();
    Map<ResourceId, List<FunctionNode>> functionsByNamespace = new ConcurrentHashMap<>();
    Map<String, ResourceId> tableNames = new ConcurrentHashMap<>();
    Map<String, ResourceId> viewNames = new ConcurrentHashMap<>();
    Map<String, ResourceId> namespaceNames = new ConcurrentHashMap<>();

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
      tablesByNamespace.computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>()).add(node);
      relationIdsByNamespace
          .computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>())
          .add(tableId);
      tableNames.put(NameRefUtil.canonical(table.name()), tableId);
    }

    // Build view nodes and map them to namespaces
    for (SystemViewDef view : catalog.views()) {
      Optional<ResourceId> namespaceId = findNamespaceId(view.name(), namespaceIds);
      if (namespaceId.isEmpty()) {
        continue;
      }

      ResourceId viewId =
          SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_VIEW, view.name());

      ViewNode node =
          new ViewNode(
              viewId,
              version,
              createdAt,
              catalogId,
              namespaceId.get(),
              view.displayName(),
              view.sql(),
              view.dialect(),
              view.columns(),
              List.of(), // baseRelations (can be wired later)
              List.of(), // creationSearchPath
              Map.of(),
              Optional.empty(),
              Map.of());

      nodesById.put(viewId, node);

      viewsByNamespace.computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>()).add(node);

      relationIdsByNamespace
          .computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>())
          .add(viewId);
      viewNames.put(NameRefUtil.canonical(view.name()), viewId);
    }

    // Build namespace nodes with relation ids attached
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
      tablesByNamespace.computeIfAbsent(namespaceId, ignored -> List.of());
      viewsByNamespace.computeIfAbsent(namespaceId, ignored -> List.of());
      namespaceNames.put(NameRefUtil.canonical(ns.name()), namespaceId);
    }

    // Build function nodes and map them to namespaces
    for (FunctionNode fn : nodes.functions()) {
      Optional<ResourceId> nsId =
          findNamespaceId(
              fn.displayName() != null ? NameRefUtil.name(fn.displayName()) : null, namespaceIds);
      nsId.ifPresent(
          id -> functionsByNamespace.computeIfAbsent(id, ignored -> new ArrayList<>()).add(fn));
    }

    return new GraphSnapshot(
        List.copyOf(namespaceNodes),
        tablesByNamespace.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))),
        viewsByNamespace.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))),
        functionsByNamespace.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))),
        Map.copyOf(nodesById),
        Map.copyOf(tableNames),
        Map.copyOf(viewNames),
        Map.copyOf(namespaceNames));
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
    String id =
        (engineKind == null || engineKind.isBlank())
            ? SystemNodeRegistry.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId(id)
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

  private record VersionKey(String engineKind, String engineVersion) {}
}
