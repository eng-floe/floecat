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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.scanner.utils;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTestCatalogOverlay implements CatalogOverlay {

  protected final Map<ResourceId, GraphNode> nodes = new HashMap<>();
  protected final Map<ResourceId, List<RelationNode>> relationsByNamespace = new HashMap<>();
  protected final Map<ResourceId, List<FunctionNode>> functionsByNamespace = new HashMap<>();
  protected final Map<ResourceId, List<SchemaColumn>> explicitSchemas = new HashMap<>();

  public BaseTestCatalogOverlay addNode(GraphNode node) {
    nodes.put(node.id(), node);
    return this;
  }

  public BaseTestCatalogOverlay addRelation(ResourceId namespaceId, RelationNode node) {
    relationsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(node);
    addNode(node);
    return this;
  }

  public BaseTestCatalogOverlay addFunction(ResourceId namespaceId, FunctionNode fn) {
    functionsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(fn);
    addNode(fn);
    return this;
  }

  public BaseTestCatalogOverlay setTableSchema(ResourceId tableId, List<SchemaColumn> schema) {
    explicitSchemas.put(tableId, List.copyOf(schema));
    return this;
  }

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.ofNullable(nodes.get(id));
  }

  @Override
  public List<RelationNode> listRelations(ResourceId catalogId) {
    return relationsByNamespace.values().stream().flatMap(List::stream).toList();
  }

  @Override
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return nodes.values().stream()
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .toList();
  }

  @Override
  public List<RelationNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
    return relationsByNamespace.getOrDefault(namespaceId, List.of());
  }

  @Override
  public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
    return functionsByNamespace.getOrDefault(namespaceId, List.of());
  }

  @Override
  public List<TypeNode> listTypes(ResourceId catalogId) {
    return nodes.values().stream()
        .filter(TypeNode.class::isInstance)
        .map(TypeNode.class::cast)
        .toList();
  }

  @Override
  public Optional<ResourceId> resolveCatalog(String correlationId, String name) {
    throw unsupported();
  }

  @Override
  public Optional<ResourceId> resolveNamespace(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public Optional<ResourceId> resolveTable(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public Optional<ResourceId> resolveView(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public Optional<ResourceId> resolveName(String correlationId, NameRef ref) {
    throw unsupported();
  }

  @Override
  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    throw unsupported();
  }

  @Override
  public ResolveResult batchResolveTables(
      String correlationId, List<NameRef> items, int limit, String token) {
    throw unsupported();
  }

  @Override
  public ResolveResult listTablesByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {
    throw unsupported();
  }

  @Override
  public Optional<ResourceId> resolveSystemTable(NameRef ref) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> resolveSystemTableName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public ResolveResult batchResolveViews(
      String correlationId, List<NameRef> items, int limit, String token) {
    throw unsupported();
  }

  @Override
  public ResolveResult listViewsByPrefix(
      String correlationId, NameRef prefix, int limit, String token) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> namespaceName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> tableName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<NameRef> viewName(ResourceId id) {
    throw unsupported();
  }

  @Override
  public Optional<CatalogNode> catalog(ResourceId id) {
    throw unsupported();
  }

  @Override
  public SchemaResolution schemaFor(
      String correlationId, ResourceId tableId, SnapshotRef snapshot) {
    throw unsupported();
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {
    return explicitSchemas.getOrDefault(tableId, List.of());
  }

  protected static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Not used in this test");
  }
}
