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

package ai.floedb.floecat.systemcatalog.util;

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
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared stub implementation of {@link CatalogOverlay} for unit tests. */
public class TestCatalogOverlay implements CatalogOverlay {

  private final Map<ResourceId, GraphNode> nodes = new HashMap<>();
  private final Map<ResourceId, List<RelationNode>> relationsByNamespace = new HashMap<>();
  private final Map<ResourceId, List<FunctionNode>> functionsByNamespace = new HashMap<>();
  private final Map<ResourceId, List<SchemaColumn>> tableSchemas = new HashMap<>();
  private final Map<String, TypeNode> typesByQName = new HashMap<>();

  // ------------------------
  // Test helpers
  // ------------------------

  public TestCatalogOverlay addNode(GraphNode node) {
    nodes.put(node.id(), node);
    return this;
  }

  public TestCatalogOverlay addRelation(ResourceId namespaceId, RelationNode node) {
    relationsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(node);
    addNode(node);
    return this;
  }

  public TestCatalogOverlay addFunction(ResourceId namespaceId, FunctionNode fn) {
    functionsByNamespace.computeIfAbsent(namespaceId, k -> new ArrayList<>()).add(fn);
    addNode(fn);
    return this;
  }

  public TestCatalogOverlay addType(ResourceId namespaceId, TypeNode type) {
    addNode(type);
    typesByQName.put(namespaceId.getId() + "." + type.displayName(), type);
    return this;
  }

  public TestCatalogOverlay setTableSchema(ResourceId tableId, List<SchemaColumn> schema) {
    tableSchemas.put(tableId, List.copyOf(schema));
    return this;
  }

  public Optional<TypeNode> findType(String namespace, String name) {
    return Optional.ofNullable(typesByQName.get(namespace + "." + name));
  }

  // ------------------------
  // CatalogOverlay impl
  // ------------------------

  @Override
  public Optional<GraphNode> resolve(ResourceId id) {
    return Optional.ofNullable(nodes.get(id));
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
  public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
    return nodes.values().stream()
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .toList();
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {

    // 1. Explicit test schema (highest priority)
    List<SchemaColumn> explicit = tableSchemas.get(tableId);
    if (explicit != null) {
      return explicit;
    }

    // 2. System table schema
    GraphNode node = nodes.get(tableId);
    if (node instanceof SystemTableNode system) {
      return system.columns();
    }

    // 3. Nothing defined
    return List.of();
  }

  @Override
  public List<RelationNode> listRelations(ResourceId catalogId) {
    throw unsupported();
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Not used in this test");
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
}
