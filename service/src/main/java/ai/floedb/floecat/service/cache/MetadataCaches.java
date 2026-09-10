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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.service.repo.cache.IndexedPointerStore;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.RawPointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/** Builds the single pointer-store seam used by planning, mutation, and maintenance code. */
@ApplicationScoped
public class MetadataCaches {
  @Produces
  @Singleton
  @CachedPointerStore
  public PointerStore cachedPointerStore(
      @RawPointerStore PointerStore raw, PlanningPointerIndex index) {
    return new IndexedPointerStore(raw, index);
  }

  /** Callers do not select a cached or durable view; the indexed store makes that decision. */
  @Produces
  @Singleton
  public PointerStore pointerStore(@CachedPointerStore PointerStore indexed) {
    return indexed;
  }

  @Produces
  @Singleton
  public PlanningPointerIndex pointers(
      @RawPointerStore PointerStore raw, Observability observability) {
    PlanningPointerIndex index = new PlanningPointerIndex(raw);
    CacheMetrics metrics = new CacheMetrics(observability, "service", "metadata-index", "pointer");
    metrics.trackAccounts(
        index::loadingPartitionCount,
        "Planner pointer partitions still loading",
        Tag.of(TagKey.RESULT, "loading"));
    metrics.trackAccounts(
        index::completePartitionCount,
        "Planner pointer partitions complete",
        Tag.of(TagKey.RESULT, "complete"));
    return index;
  }
}
