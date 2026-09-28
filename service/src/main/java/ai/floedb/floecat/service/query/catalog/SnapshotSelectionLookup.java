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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.TablePin;
import java.util.Optional;
import java.util.OptionalLong;

/** Query-scoped snapshot selection lookup used by planner bundle assembly. */
interface SnapshotSelectionLookup {
  OptionalLong resolvedSnapshotId(ResourceId tableId);

  /** The query's resolved selection for this table, when it has one. */
  default Optional<TablePin> resolvedSelection(ResourceId tableId) {
    return Optional.empty();
  }

  /** Whether the resolved selection represents the live CURRENT snapshot. */
  default boolean selectsCurrent(ResourceId tableId) {
    return false;
  }

  /**
   * The stats generation ref frozen on this table's pin. Empty means no pin, no stats generation at
   * pin time, or a store that does not track stats generations.
   */
  default Optional<String> resolvedStatsGenerationRef(ResourceId tableId) {
    return Optional.empty();
  }

  /**
   * The constraints ref frozen on this table's pin — the resolved root entry's immutable bundle
   * identity, copied onto the pin at construction. Empty means no bundle existed at pin time (or no
   * selection): the query deterministically serves no constraints for this attempt, even if a
   * bundle appears mid-query. The serving path loads the bundle by this ref, never the live
   * pointer.
   */
  default Optional<ResolvedConstraintsRef> resolvedConstraintsRef(ResourceId tableId) {
    return Optional.empty();
  }

  /** Immutable identity of a resolved constraints bundle: content-addressed URI + version. */
  record ResolvedConstraintsRef(String uri, String version) {}
}
