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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable namespace node tracking hierarchy and optional children.
 *
 * <p>Child relations are only populated for listing RPCs; query-resolution paths can leave the
 * field empty to avoid unnecessary cache churn.
 */
public record NamespaceNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    ResourceId catalogId,
    List<String> pathSegments,
    String displayName,
    GraphNodeOrigin origin,
    Map<String, String> properties,
    Optional<List<ResourceId>> relationIds,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public NamespaceNode {
    pathSegments = List.copyOf(pathSegments);
    properties = Map.copyOf(properties);
    relationIds =
        relationIds == null ? Optional.empty() : relationIds.map(list -> List.copyOf(list));
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.NAMESPACE;
  }

  @Override
  public GraphNodeOrigin origin() {
    return origin;
  }
}
