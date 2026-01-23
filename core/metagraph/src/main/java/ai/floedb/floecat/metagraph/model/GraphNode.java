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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base type for every immutable metadata node tracked by the query graph.
 *
 * <p>Nodes mirror the logical catalog objects described in {@code FloeCat_Architecture_Book.md}
 * (catalogs, namespaces, tables, views, system relations). Each node captures the pointer version
 * so cache invalidation is deterministic, while {@link EngineHint} payloads allow planner-specific
 * extensions without mutating the core structure.
 */
public interface GraphNode {

  /** Stable identifier (account + kind + UUID) for the node. */
  ResourceId id();

  /** Pointer version/Etag used for cache invalidation. */
  long version();

  /** Human-readable name for display purposes. */
  String displayName();

  /**
   * Timestamp of the last metadata mutation for this node.
   *
   * <p>The timestamp reflects repository-level changes (schema updates, view edits) but is not tied
   * to snapshot pointer updates, which are tracked separately.
   */
  Instant metadataUpdatedAt();

  /** Logical kind returned to planners. */
  GraphNodeKind kind();

  /** Origin of the node */
  GraphNodeOrigin origin();

  /**
   * Engine-specific hint map keyed by kind/version/payloadType.
   *
   * <p>Implementations should return immutable maps. Use {@link #engineHint(String, String,
   * String)} for convenience lookups or {@link #engineHintsFor(String, String)} to retrieve all
   * hints for a specific engine pair.
   */
  Map<EngineHintKey, EngineHint> engineHints();

  /**
   * Lookup helper for an engine/version/payloadType triple.
   *
   * @param engineKind planner/executor kind (e.g. TRINO)
   * @param engineVersion semantic version string
   * @param payloadType hint category requested by the caller
   * @return the hint payload if present
   */
  default Optional<EngineHint> engineHint(
      String engineKind, String engineVersion, String payloadType) {
    return Optional.ofNullable(
        engineHints().get(new EngineHintKey(engineKind, engineVersion, payloadType)));
  }

  /**
   * Returns every hint for the given engine key, keyed by payloadType.
   *
   * @param engineKind planner/executor kind (e.g. TRINO)
   * @param engineVersion semantic version string
   * @return map from payloadType → hint payload
   *     <p>Note: This method rebuilds a map every call and iterates all stored hints, so keep it
   *     limited to tests or rare diagnostics; hot execution paths should be satisfied with the
   *     constant-time engineHint(engineKind, engineVersion, payloadType) overload instead.
   */
  default Map<String, EngineHint> engineHintsFor(String engineKind, String engineVersion) {
    Objects.requireNonNull(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");
    return engineHints().entrySet().stream()
        .filter(
            entry ->
                engineKind.equals(entry.getKey().engineKind())
                    && engineVersion.equals(entry.getKey().engineVersion()))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().payloadType(),
                Map.Entry::getValue,
                (first, second) -> second));
  }
}
