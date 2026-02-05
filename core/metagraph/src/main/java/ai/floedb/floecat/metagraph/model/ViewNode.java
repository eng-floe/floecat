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

package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable view node encapsulating SQL definition and dependency references.
 *
 * <p>The node stores base relation IDs only; traversal APIs resolve them to aligned {@link
 * GraphNode}s so callers always see a consistent tree.
 */
public record ViewNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    ResourceId namespaceId,
    String displayName,
    String sql,
    String dialect,
    List<SchemaColumn> outputColumns,
    List<ResourceId> baseRelations,
    List<String> creationSearchPath,
    GraphNodeOrigin origin,
    Map<String, String> properties,
    Optional<String> owner,
    Map<EngineHintKey, EngineHint> engineHints)
    implements GraphNode {

  public ViewNode {
    outputColumns = List.copyOf(outputColumns);
    baseRelations = List.copyOf(baseRelations);
    creationSearchPath = List.copyOf(creationSearchPath);
    properties = Map.copyOf(properties);
    owner = owner == null ? Optional.empty() : owner;
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.VIEW;
  }

  @Override
  public GraphNodeOrigin origin() {
    return origin;
  }
}
