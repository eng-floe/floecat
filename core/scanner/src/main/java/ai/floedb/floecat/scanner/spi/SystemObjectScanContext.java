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
    CatalogOverlay graph,
    NameRef name,
    ResourceId queryDefaultCatalogId,
    EngineContext engineContext,
    StatsProvider statsProvider,
    ConstraintProvider constraintProvider,
    ConcurrentMap<Object, Object> memoizedValues)
    implements MetadataResolutionContext {

  public SystemObjectScanContext {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(queryDefaultCatalogId, "queryDefaultCatalogId");
    engineContext = engineContext == null ? EngineContext.empty() : engineContext;
    statsProvider = statsProvider == null ? StatsProvider.NONE : statsProvider;
    constraintProvider = constraintProvider == null ? ConstraintProvider.NONE : constraintProvider;
    memoizedValues = memoizedValues == null ? new ConcurrentHashMap<>() : memoizedValues;
  }

  public SystemObjectScanContext(
      CatalogOverlay graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        engineContext,
        StatsProvider.NONE,
        ConstraintProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogOverlay graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext,
      StatsProvider statsProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        engineContext,
        statsProvider,
        ConstraintProvider.NONE,
        new ConcurrentHashMap<>());
  }

  public SystemObjectScanContext(
      CatalogOverlay graph,
      NameRef name,
      ResourceId queryDefaultCatalogId,
      EngineContext engineContext,
      StatsProvider statsProvider,
      ConstraintProvider constraintProvider) {
    this(
        graph,
        name,
        queryDefaultCatalogId,
        engineContext,
        statsProvider,
        constraintProvider,
        new ConcurrentHashMap<>());
  }

  public GraphNode resolve(ResourceId id) {
    return graph.resolve(id).orElseThrow();
  }

  @Override
  public CatalogOverlay overlay() {
    return graph;
  }

  @Override
  public ResourceId catalogId() {
    return queryDefaultCatalogId;
  }

  public Optional<GraphNode> tryResolve(ResourceId id) {
    return graph.resolve(id);
  }

  /** Tables + views */
  public List<RelationNode> listRelations(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(queryDefaultCatalogId, namespaceId);
  }

  /** Tables only */
  public List<TableNode> listTables(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(queryDefaultCatalogId, namespaceId).stream()
        .filter(TableNode.class::isInstance)
        .map(TableNode.class::cast)
        .toList();
  }

  /** Views only */
  public List<ViewNode> listViews(ResourceId namespaceId) {
    return graph.listRelationsInNamespace(queryDefaultCatalogId, namespaceId).stream()
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .toList();
  }

  public List<NamespaceNode> listNamespaces() {
    return graph.listNamespaces(queryDefaultCatalogId);
  }

  public List<FunctionNode> listFunctions(ResourceId namespaceId) {
    return graph.listFunctions(queryDefaultCatalogId, namespaceId);
  }

  public List<TypeNode> listTypes() {
    return graph.listTypes(queryDefaultCatalogId);
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
