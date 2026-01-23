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
import java.util.Map;

/** Relation node describing a builtin SQL type. */
public record TypeNode(
    ResourceId id,
    long version,
    Instant metadataUpdatedAt,
    String engineVersion,
    String displayName,
    String category,
    boolean array,
    ResourceId elementType,
    Map<EngineHintKey, EngineHint> engineHints)
    implements GraphNode {

  public TypeNode {
    engineHints = Map.copyOf(engineHints == null ? Map.of() : engineHints);
  }

  @Override
  public GraphNodeKind kind() {
    return GraphNodeKind.TYPE;
  }

  @Override
  public GraphNodeOrigin origin() {
    return GraphNodeOrigin.SYSTEM;
  }
}
