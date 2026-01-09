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

package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    EngineContext engineContext)
    implements MetadataResolutionContext {

  public SystemObjectScanContext {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(queryDefaultCatalogId, "queryDefaultCatalogId");
    engineContext = engineContext == null ? EngineContext.empty() : engineContext;
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
  public List<GraphNode> listRelations(ResourceId namespaceId) {
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
}
