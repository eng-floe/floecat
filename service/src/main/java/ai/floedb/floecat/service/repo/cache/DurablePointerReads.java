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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.Optional;

/**
 * Pointer reads that deliberately bypass the planner index.
 *
 * <p>For a read whose emptiness is load-bearing, the index is the wrong oracle whenever the caller
 * can observe objects the index was never told about. The blob sweep is the case: it discovers
 * tables by listing blob prefixes, so it asks about tables the catalog holds no identity row for.
 * The index is authoritative only over the tables that row names, so it answers absent for those --
 * and an absent root there reads as "nothing referenced" and deletes live blobs.
 *
 * <p>Reads only, and a separate type rather than a second {@code PointerStore} injection, so that
 * bypassing the index stays a decision a reader can see. Mutations must still go through the
 * indexed store or the index would not learn about them.
 */
public final class DurablePointerReads {
  private final PointerStore durable;

  public DurablePointerReads(PointerStore durable) {
    this.durable = java.util.Objects.requireNonNull(durable, "durable");
  }

  /**
   * The settled value, because a caller reaching past the index needs the authority, not a view.
   */
  public Optional<Pointer> get(String key) {
    return durable.getConsistent(key);
  }
}
