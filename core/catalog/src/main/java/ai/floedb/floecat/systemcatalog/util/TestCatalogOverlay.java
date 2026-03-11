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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.utils.BaseTestCatalogOverlay;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared stub implementation of {@link BaseTestCatalogOverlay} for unit tests. */
public class TestCatalogOverlay extends BaseTestCatalogOverlay {

  private final Map<TypeKey, TypeNode> typesByQName = new HashMap<>();

  public TestCatalogOverlay addType(TypeNode type) {
    addNode(type);
    ResourceId namespaceId = type.namespaceId();
    typesByQName.put(new TypeKey(namespaceId, type.displayName()), type);
    return this;
  }

  public Optional<TypeNode> findType(ResourceId namespaceId, String name) {
    return Optional.ofNullable(typesByQName.get(new TypeKey(namespaceId, name)));
  }

  @Override
  public List<SchemaColumn> tableSchema(ResourceId tableId) {
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
