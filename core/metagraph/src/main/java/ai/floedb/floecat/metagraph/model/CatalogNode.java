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
 * Immutable view of a catalog resource.
 *
 * <p>Catalog nodes primarily act as anchors for namespace/table traversal, so the model keeps their
 * metadata intentionally small.
 */
public record CatalogNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String displayName,
    Map<String, String> properties,
    Optional<String> connectorId,
    Optional<String> policyRef,
    Optional<List<ResourceId>> namespaceIds,
    Map<EngineKey, EngineHint> engineHints)
    implements GraphNode {

  public CatalogNode {
    properties = Map.copyOf(properties);
    connectorId = connectorId == null ? Optional.empty() : connectorId;
    policyRef = policyRef == null ? Optional.empty() : policyRef;
    namespaceIds =
        namespaceIds == null ? Optional.empty() : namespaceIds.map(list -> List.copyOf(list));
    engineHints = Map.copyOf(engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.CATALOG;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.USER;
  }
}
