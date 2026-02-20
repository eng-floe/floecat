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
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
  public Optional<GraphNode> resolve(ResourceId id, EngineContext ctx) {
    return snapshotFor(ctx).resolve(id);
  }

  public Optional<GraphNode> resolve(ResourceId id, String engineKind, String engineVersion) {
    return resolve(id, EngineContext.of(engineKind, engineVersion));
  }

  public Optional<CatalogNode> catalog(ResourceId id, EngineContext ctx) {
    return snapshotFor(ctx).catalog(id);
  }

  public Optional<CatalogNode> catalog(ResourceId id, String engineKind, String engineVersion) {
    return catalog(id, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Lists every system view/table relation that belongs to the engine-catalog root.
   *
   * <p>The snapshot already groups namespace→relations so we can return the bucket without
   * recalculating it on every invocation.
   */
  public List<RelationNode> listRelations(ResourceId catalogId, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);

    return snapshot.namespaces().stream()
        .flatMap(
            ns ->
                Stream.concat(
                    snapshot.tablesInNamespace(ns.id()).stream(),
                    snapshot.viewsInNamespace(ns.id()).stream()))
        .map(RelationNode.class::cast)
        .toList();
  }

  public List<RelationNode> listRelations(
      ResourceId catalogId, String engineKind, String engineVersion) {
    return listRelations(catalogId, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Retrieves the pre-bucketed relations under a namespace so scanners can enumerate tables
   * quickly.
   *
   * @param catalogId is ignored as system objects have their own semantics for catalog
   */
  public List<RelationNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);

    return Stream.concat(
            snapshot.tablesInNamespace(namespaceId).stream(),
            snapshot.viewsInNamespace(namespaceId).stream())
        .map(RelationNode.class::cast)
        .toList();
  }

  public List<RelationNode> listRelationsInNamespace(
      ResourceId catalogId, ResourceId namespaceId, String engineKind, String engineVersion) {
    return listRelationsInNamespace(
        catalogId, namespaceId, EngineContext.of(engineKind, engineVersion));
  }

  public List<FunctionNode> listFunctions(ResourceId namespaceId, EngineContext ctx) {
    return snapshotFor(ctx).functionsInNamespace(namespaceId);
  }

  public List<FunctionNode> listFunctions(
      ResourceId namespaceId, String engineKind, String engineVersion) {
    return listFunctions(namespaceId, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Returns all namespaces in the snapshot.
   *
   * <p>The snapshot is already scoped to a single system catalog, so {@code catalogId} is not used
   * for filtering here.
   */
  public List<NamespaceNode> listNamespaces(ResourceId catalogId, EngineContext ctx) {
    return snapshotFor(ctx).namespaces();
  }

  public List<NamespaceNode> listNamespaces(
      ResourceId catalogId, String engineKind, String engineVersion) {
    return listNamespaces(catalogId, EngineContext.of(engineKind, engineVersion));
  }

  public List<TypeNode> listTypes(ResourceId catalogId, EngineContext ctx) {
    return registry.nodesFor(ctx).types();
  }

  public List<TypeNode> listTypes(ResourceId catalogId, String engineKind, String engineVersion) {
    return listTypes(catalogId, EngineContext.of(engineKind, engineVersion));
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
  public Optional<ResourceId> resolveTable(NameRef ref, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.tableNames().get(canonical));
  }

  public Optional<ResourceId> resolveTable(NameRef ref, String engineKind, String engineVersion) {
    return resolveTable(ref, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Resolves a view by name reference in the system catalog.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved view resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveView(NameRef ref, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.viewNames().get(canonical));
  }

  public Optional<ResourceId> resolveView(NameRef ref, String engineKind, String engineVersion) {
    return resolveView(ref, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Resolves a namespace by name reference in the system catalog.
   *
   * @param ref the name reference to resolve
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the resolved namespace resource ID, or empty if not found
   */
  public Optional<ResourceId> resolveNamespace(NameRef ref, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    String canonical = NameRefUtil.canonical(ref);
    return Optional.ofNullable(snapshot.namespaceNames().get(canonical));
  }

  public Optional<ResourceId> resolveNamespace(
      NameRef ref, String engineKind, String engineVersion) {
    return resolveNamespace(ref, EngineContext.of(engineKind, engineVersion));
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
  public Optional<ResourceId> resolveName(NameRef ref, EngineContext ctx) {
    // For simplicity, try table, view, namespace in order
    return resolveTable(ref, ctx)
        .or(() -> resolveView(ref, ctx))
        .or(() -> resolveNamespace(ref, ctx));
  }

  public Optional<ResourceId> resolveName(NameRef ref, String engineKind, String engineVersion) {
    return resolveName(ref, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a table by its resource ID.
   *
   * @param id the table resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> tableName(ResourceId id, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    return snapshot
        .resolve(id)
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .flatMap(table -> buildTableNameRef(table, ctx));
  }

  public Optional<NameRef> tableName(ResourceId id, String engineKind, String engineVersion) {
    return tableName(id, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a view by its resource ID.
   *
   * @param id the view resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> viewName(ResourceId id, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    return snapshot
        .resolve(id)
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .flatMap(view -> buildViewNameRef(view, ctx));
  }

  public Optional<NameRef> viewName(ResourceId id, String engineKind, String engineVersion) {
    return viewName(id, EngineContext.of(engineKind, engineVersion));
  }

  /**
   * Gets the fully qualified name of a namespace by its resource ID.
   *
   * @param id the namespace resource ID
   * @param engineKind the engine kind for the system catalog
   * @param engineVersion the engine version for the system catalog
   * @return the fully qualified name reference, or empty if not found
   */
  public Optional<NameRef> namespaceName(ResourceId id, EngineContext ctx) {
    GraphSnapshot snapshot = snapshotFor(ctx);
    return snapshot
        .resolve(id)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(ns -> buildNamespaceNameRef(ns, ctx));
  }

  public Optional<NameRef> namespaceName(ResourceId id, String engineKind, String engineVersion) {
    return namespaceName(id, EngineContext.of(engineKind, engineVersion));
  }

  private Optional<NameRef> buildTableNameRef(TableNode table, EngineContext ctx) {
    return resolve(table.namespaceId(), ctx)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(
            ns ->
                NameRefUtil.name(ns.displayName(), table.displayName()).toBuilder()
                    .setCatalog(engineCatalogKind(ctx))
                    .build());
  }

  private Optional<NameRef> buildViewNameRef(ViewNode view, EngineContext ctx) {
    return resolve(view.namespaceId(), ctx)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .map(
            ns ->
                NameRefUtil.name(ns.displayName(), view.displayName()).toBuilder()
                    .setCatalog(engineCatalogKind(ctx))
                    .build());
  }

  private NameRef buildNamespaceNameRef(NamespaceNode ns, EngineContext ctx) {
    return NameRef.newBuilder()
        .setCatalog(engineCatalogKind(ctx))
        .addPath(ns.displayName())
        .build();
  }

  private static String engineCatalogKind(EngineContext ctx) {
    if (ctx == null) {
      return EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;
    }
    return ctx.normalizedKind();
  }

  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return snapshotForCatalog(catalogId).namespaces();
  }

  public Optional<NamespaceNode> tryNamespace(ResourceId id) {
    return resolveAny(id).filter(NamespaceNode.class::isInstance).map(NamespaceNode.class::cast);
  }

  /** Builds a new snapshot for the requested engine version. */
  private GraphSnapshot snapshotFor(EngineContext ctx) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;
    String normalizedKind = engineCatalogKind(canonical);
    String normalizedVersion = canonical.normalizedVersion();

    VersionKey key = new VersionKey(normalizedKind, normalizedVersion);
    GraphSnapshot cached = snapshots.get(key);
    if (cached != null) {
      return cached;
    }
    GraphSnapshot snapshot = build(canonical, normalizedKind, normalizedVersion);
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
  private GraphSnapshot build(EngineContext ctx, String normalizedKind, String normalizedVersion) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;
    var nodes = registry.nodesFor(canonical);
    if (nodes == null) {
      return GraphSnapshot.empty();
    }

    long version = versionFromFingerprint(nodes.fingerprint());
    ResourceId catalogId = systemCatalogId(normalizedKind);

    CatalogNode catalogNode =
        new CatalogNode(
            catalogId,
            version,
            createdAt,
            normalizedKind,
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());

    Map<ResourceId, GraphNode> nodesById = new ConcurrentHashMap<>(nodesById(nodes));
    nodesById.put(catalogId, catalogNode);

    List<NamespaceNode> namespaceNodes = nodes.namespaceNodes();
    Map<ResourceId, List<TableNode>> tablesByNamespace =
        new ConcurrentHashMap<>(nodes.tablesByNamespace());
    Map<ResourceId, List<ViewNode>> viewsByNamespace =
        new ConcurrentHashMap<>(nodes.viewsByNamespace());
    Map<String, ResourceId> tableNames = new ConcurrentHashMap<>(nodes.tableNames());
    Map<String, ResourceId> viewNames = new ConcurrentHashMap<>(nodes.viewNames());
    Map<String, ResourceId> namespaceNames = new ConcurrentHashMap<>(nodes.namespaceNames());

    namespaceNodes.forEach(
        ns -> {
          tablesByNamespace.computeIfAbsent(ns.id(), ignored -> List.of());
          viewsByNamespace.computeIfAbsent(ns.id(), ignored -> List.of());
        });

    Map<ResourceId, List<FunctionNode>> functionsByNamespace = new ConcurrentHashMap<>();

    for (FunctionNode fn : nodes.functions()) {
      ResourceId nsId = fn.namespaceId();
      if (nsId != null) {
        functionsByNamespace.computeIfAbsent(nsId, ignored -> new ArrayList<>()).add(fn);
      }
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
    entries.putAll(entriesFromList(nodes.namespaceNodes()));
    entries.putAll(entriesFromList(nodes.tableNodes()));
    entries.putAll(entriesFromList(nodes.viewNodes()));
    return entries;
  }

  private static Map<ResourceId, GraphNode> entriesFromList(List<? extends GraphNode> nodes) {
    return nodes.stream().collect(Collectors.toUnmodifiableMap(GraphNode::id, node -> node));
  }

  private static ResourceId systemCatalogId(String engineKind) {
    return SystemNodeRegistry.systemCatalogContainerId(engineKind);
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
