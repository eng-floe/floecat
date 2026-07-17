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
import java.util.Optional;
import java.util.OptionalLong;

/** Query-scoped snapshot pin lookup used by planner bundle assembly. */
interface SnapshotPinLookup {
  OptionalLong pinnedSnapshotId(ResourceId tableId);

  /**
   * The stats generation ref frozen on this table's pin. Empty means no pin, no stats generation at
   * pin time, or a store that does not track stats generations.
   */
  default Optional<String> pinnedStatsGenerationRef(ResourceId tableId) {
    return Optional.empty();
  }

  /**
   * The constraints ref frozen on this table's pin — the pinned root entry's immutable bundle
   * identity, copied onto the pin at construction. Empty means no bundle existed at pin time (or no
   * pin): the query deterministically serves no constraints for its lifetime, even if a bundle
   * appears mid-query. The serving path loads the bundle by this ref, never the live pointer.
   */
  default Optional<PinnedConstraintsRef> pinnedConstraintsRef(ResourceId tableId) {
    return Optional.empty();
  }

  /** Immutable identity of a pinned constraints bundle: content-addressed URI + version. */
  record PinnedConstraintsRef(String uri, String version) {}
}
