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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Immutable context during a system object scan.
 *
 * <p>This provides view/relation/namespace resolution through a minimal graph view abstraction. It
 * is safe, cache-aware, and keeps core decoupled from the full MetadataGraph implementation.
 */
public record SystemObjectScanContext(
    CatalogGraphView graph,
    NameRef name,
    ResourceId queryDefaultCatalogId,
    CatalogContext catalogContext,
    StatsProvider statsProvider,
    ConstraintProvider constraintProvider,
    CatalogListingProvider catalogListingProvider,
    ConcurrentMap<Object, Object> memoizedValues)
    implements MetadataResolutionContext {

  public SystemObjectScanContext {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(queryDefaultCatalogId, "queryDefaultCatalogId");
    catalogContext = Objects.requireNonNull(catalogContext, "catalogContext");
    statsProvider = statsProvider == null ? StatsProvider.NONE : statsProvider;
    constraintProvider = constraintProvider == null ? ConstraintProvider.NONE : constraintProvider;
    catalogListingProvider =
        catalogListingProvider == null ? CatalogListingProvider.NONE : catalogListingProvider;
    memoizedValues = memoizedValues == null ? new ConcurrentHashMap<>() : memoizedValues;
  }

  /** A catalog as sys.catalog lists it; {@code description} is null when not loaded. */
  public record CatalogEntry(ResourceId id, String name, String description) {}

  /**
   * Lists the catalogs visible to the scanning account. {@code includeDescription} false lets the
   * implementation skip loading catalog objects.
   */
  @FunctionalInterface
  public interface CatalogListingProvider {
    CatalogListingProvider NONE = includeDescription -> List.of();

    List<CatalogEntry> listVisibleCatalogs(boolean includeDescription);
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      CatalogContext catalogContext) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext,
        StatsProvider.NONE,
        ConstraintProvider.NONE,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      CatalogContext catalogContext,
      StatsProvider statsProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext,
        statsProvider,
        ConstraintProvider.NONE,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      CatalogContext catalogContext,
      StatsProvider statsProvider,
      ConstraintProvider constraintProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext,
        statsProvider,
        constraintProvider,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      CatalogContext catalogContext,
      StatsProvider statsProvider,
      ConstraintProvider constraintProvider,
      CatalogListingProvider catalogListingProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext,
        statsProvider,
        constraintProvider,
        catalogListingProvider,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext(engineContext),
        StatsProvider.NONE,
        ConstraintProvider.NONE,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext,
      StatsProvider statsProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext(engineContext),
        statsProvider,
        ConstraintProvider.NONE,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogGraphView graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext,
      StatsProvider statsProvider,
      ConstraintProvider constraintProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        catalogContext(engineContext),
        statsProvider,
        constraintProvider,
        CatalogListingProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public GraphNode resolve(ResourceId id) {
    return graph.resolve(id, catalogContext).orElseThrow();
  }

  private static CatalogContext catalogContext(EngineContext engineContext) {
    return CatalogContext.forEngine(engineContext);
  }

  @Override
  public CatalogGraphView graphView() {
    return graph;
  }

  @Override
  public ResourceId catalogId() {
    return queryDefaultCatalogId;
  }

  public Optional<GraphNode> tryResolve(ResourceId id) {
    return graph.resolve(id, catalogContext);
  }

  /** Lightweight namespace refs from the graph view. */
  public List<CatalogGraphView.NamespaceRef> listNamespaceRefs() {
    return graph.listNamespaceRefs(queryDefaultCatalogId, catalogContext);
  }

  /**
   * Catalogs visible to the scanning account, for account-level system objects such as sys.catalog.
   * Empty when the context was built without a catalog listing, which is distinct from an account
   * with no catalogs.
   */
  public Optional<List<CatalogEntry>> listVisibleCatalogs(boolean includeDescription) {
    if (catalogListingProvider == CatalogListingProvider.NONE) {
      return Optional.empty();
    }
    return Optional.of(catalogListingProvider.listVisibleCatalogs(includeDescription));
  }

  /** Lightweight namespace refs matching the supplied information-schema names. */
  public List<CatalogGraphView.NamespaceRef> listNamespaceRefsByName(java.util.Set<String> names) {
    return graph.listNamespaceRefsByName(queryDefaultCatalogId, names, catalogContext);
  }

  /** Lightweight relation refs for a namespace. */
  public List<CatalogGraphView.RelationRef> listRelationRefs(ResourceId namespaceId) {
    return graph.listRelationRefs(queryDefaultCatalogId, namespaceId, catalogContext);
  }

  /** Lightweight relation refs matching the supplied names. */
  public List<CatalogGraphView.RelationRef> listRelationRefsByName(
      ResourceId namespaceId, java.util.Set<String> names) {
    return graph.listRelationRefsByName(queryDefaultCatalogId, namespaceId, names, catalogContext);
  }

  /** Tables + views */
  public List<RelationNode> listRelations(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(queryDefaultCatalogId, namespaceId, catalogContext);
  }

  /** Tables only */
  public List<TableNode> listTables(ResourceId namespaceId) {
    return graph
        .listRelationsInNamespace(queryDefaultCatalogId, namespaceId, catalogContext)
        .stream()
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .toList();
  }

  /** Views only */
  public List<ViewNode> listViews(ResourceId namespaceId) {
    return graph
        .listRelationsInNamespace(queryDefaultCatalogId, namespaceId, catalogContext)
        .stream()
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .toList();
  }

  public List<NamespaceNode> listNamespaces() {
    return graph.listNamespaces(queryDefaultCatalogId, catalogContext);
  }

  public List<FunctionNode> listFunctions(ResourceId namespaceId) {
    return graph.listFunctions(queryDefaultCatalogId, namespaceId, catalogContext);
  }

  public List<TypeNode> listTypes() {
    return graph.listTypes(queryDefaultCatalogId, catalogContext);
  }

  @Override
  public StatsProvider statsProvider() {
    return statsProvider;
  }

  @SuppressWarnings("unchecked")
  public <T> T memoized(Object key, Supplier<T> supplier) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(supplier, "supplier");
    return (T)
        memoizedValues.computeIfAbsent(
            key,
            ignored -> {
              T computed = supplier.get();
              if (computed == null) {
                throw new IllegalStateException("memoized value cannot be null for key: " + key);
              }
              return computed;
            });
  }
}
