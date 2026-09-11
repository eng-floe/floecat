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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.RawPointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.time.Duration;
import org.jboss.logging.Logger;

/** Builds the single pointer-store seam used by planning, mutation, and maintenance code. */
@ApplicationScoped
public class MetadataCaches {
  private static final Logger LOG = Logger.getLogger(MetadataCaches.class);

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
      @RawPointerStore PointerStore raw,
      Observability observability,
      Instance<PlanningPointerIndex.Ownership> configuredOwnership) {
    PlanningPointerIndex.Ownership ownership =
        configuredOwnership.isUnsatisfied()
            ? PlanningPointerIndex.Ownership.ALWAYS_OWNED
            : configuredOwnership.get();
    Tag[] baseTags =
        new Tag[] {Tag.of(TagKey.COMPONENT, "service"), Tag.of(TagKey.OPERATION, "metadata-index")};
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            raw,
            ownership,
            new PlanningPointerIndex.WarmObserver() {
              @Override
              public void started(String accountId) {
                observability.counter(ServiceMetrics.PlanningPointer.WARM_STARTS, 1, baseTags);
              }

              @Override
              public void completed(String accountId, Duration duration) {
                observability.timer(
                    ServiceMetrics.PlanningPointer.WARM_LATENCY,
                    duration,
                    append(baseTags, Tag.of(TagKey.RESULT, "success")));
              }

              @Override
              public void failed(String accountId, Duration duration, Throwable failure) {
                Tag[] tags =
                    append(
                        baseTags,
                        Tag.of(TagKey.RESULT, "error"),
                        Tag.of(TagKey.EXCEPTION, failure.getClass().getSimpleName()));
                observability.timer(ServiceMetrics.PlanningPointer.WARM_LATENCY, duration, tags);
                observability.counter(ServiceMetrics.PlanningPointer.WARM_ERRORS, 1, tags);
                LOG.warnf(
                    failure,
                    "planner_pointer_warm_failed account_id=%s duration=%s",
                    accountId,
                    duration);
              }
            });
    observability.gauge(
        ServiceMetrics.PlanningPointer.ENTRIES,
        index::entryCount,
        "Planner pointer entries resident",
        baseTags);
    observability.gauge(
        ServiceMetrics.PlanningPointer.PARTITIONS,
        index::loadingPartitionCount,
        "Planner pointer partitions still loading",
        append(baseTags, Tag.of(TagKey.RESULT, "loading")));
    observability.gauge(
        ServiceMetrics.PlanningPointer.PARTITIONS,
        index::completePartitionCount,
        "Planner pointer partitions complete",
        append(baseTags, Tag.of(TagKey.RESULT, "complete")));
    return index;
  }

  private static Tag[] append(Tag[] base, Tag... extra) {
    Tag[] result = java.util.Arrays.copyOf(base, base.length + extra.length);
    System.arraycopy(extra, 0, result, base.length, extra.length);
    return result;
  }
}
