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

package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.utils.BaseTestCatalogOverlay;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared stub implementation of {@link BaseTestCatalogOverlay} for unit tests. */
public class TestCatalogOverlay extends BaseTestCatalogOverlay {

  private final Map<TypeKey, TypeNode> typesByQName = new HashMap<>();
  private final Map<ResourceId, AtomicInteger> tableSchemaLookups = new HashMap<>();
  private final Map<String, AtomicInteger> resolveTableLookups = new HashMap<>();

  public TestCatalogOverlay addType(TypeNode type) {
    addNode(type);
    ResourceId namespaceId = type.namespaceId();
    typesByQName.put(new TypeKey(namespaceId, type.displayName()), type);
    return this;
  }

  public Optional<TypeNode> findType(ResourceId namespaceId, String name) {
    return Optional.ofNullable(typesByQName.get(new TypeKey(namespaceId, name)));
  }

  public int tableSchemaLookupCount(ResourceId tableId) {
    return tableSchemaLookups.getOrDefault(tableId, new AtomicInteger()).get();
  }

  public int tableSchemaLookupCountTotal() {
    return tableSchemaLookups.values().stream().mapToInt(AtomicInteger::get).sum();
  }

  public int resolveTableLookupCount(String canonicalName) {
    return resolveTableLookups.getOrDefault(canonicalName, new AtomicInteger()).get();
  }

  public int resolveTableLookupCountTotal() {
    return resolveTableLookups.values().stream().mapToInt(AtomicInteger::get).sum();
  }

  @Override
  public Optional<ResourceId> resolveTable(String correlationId, NameRef ref) {
    if (ref == null || ref.getName().isBlank()) {
      return Optional.empty();
    }
    String target = NameRefUtil.canonical(ref);
    resolveTableLookups.computeIfAbsent(target, ignored -> new AtomicInteger()).incrementAndGet();
    for (GraphNode node : nodes.values()) {
      if (!(node instanceof UserTableNode table)) {
        continue;
      }
      GraphNode namespaceNode = nodes.get(table.namespaceId());
      if (!(namespaceNode instanceof NamespaceNode namespace)) {
        continue;
      }
      NameRef.Builder tableRefBuilder = NameRef.newBuilder().setName(table.displayName());
      tableRefBuilder.addAllPath(namespace.pathSegments());
      if (!namespace.displayName().isBlank()) {
        tableRefBuilder.addPath(namespace.displayName());
      }
      if (NameRefUtil.canonical(tableRefBuilder.build()).equalsIgnoreCase(target)) {
        return Optional.of(table.id());
      }
    }
    return Optional.empty();
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {
    tableSchemaLookups.computeIfAbsent(tableId, ignored -> new AtomicInteger()).incrementAndGet();
    List<SchemaColumn> explicit = super.tableSchema(tableId);
    if (!explicit.isEmpty()) {
      return explicit;
    }
    GraphNode node = resolve(tableId).orElse(null);
    if (node instanceof SystemTableNode system) {
      return system.columns();
    }
    return List.of();
  }

  private record TypeKey(ResourceId namespaceId, String name) {}
}
