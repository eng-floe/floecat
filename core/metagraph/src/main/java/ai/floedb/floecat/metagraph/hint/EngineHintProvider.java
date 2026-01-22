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

package ai.floedb.floecat.metagraph.hint;

import ai.floedb.floecat.metagraph.model.*;
import java.util.Optional;

/** Provider of engine-specific hint payloads for relation nodes. */
public interface EngineHintProvider {

  /** Returns true when this provider can compute the requested hint type for the node kind. */
  boolean supports(GraphNodeKind kind, String payloadType);

  /** Returns true when the provider can serve the engine/version represented by the key. */
  boolean isAvailable(EngineKey engineKey);

  /**
   * Stable fingerprint describing the hint inputs for caching.
   *
   * <p>Implementations may combine pointer versions, schema hashes, provider versions, etc. to
   * ensure recomputation occurs only when relevant inputs change.
   */
  String fingerprint(GraphNode node, EngineKey engineKey, String payloadType);

  /** Computes the actual hint payload; empty when no hint applies. */
  Optional<EngineHint> compute(
      GraphNode node, EngineKey engineKey, String payloadType, String correlationId);
}
