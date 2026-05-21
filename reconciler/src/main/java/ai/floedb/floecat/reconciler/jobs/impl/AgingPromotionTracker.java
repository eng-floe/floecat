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

package ai.floedb.floecat.reconciler.jobs.impl;

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks starvation-aging promotions for scheduler job stores.
 *
 * <p>A "promotion" is recorded when a job that has been waiting longer than its class's aging
 * threshold is dispatched. The promotion counter is a metric signal — it does <em>not</em> reorder
 * the ready queue. The side map prevents double-counting: once a job is recorded as promoted, it
 * will not be counted again within the cooldown window.
 *
 * <p>Usage pattern in {@code leaseNext()}:
 *
 * <pre>{@code
 * agingTracker.cleanupExpired(now);          // called once per leaseNext() invocation
 * // ... find the best job candidate (bestJobId, bestCls) ...
 * long ageMs = now - createdAtMs.getOrDefault(bestJobId, now);
 * agingTracker.recordIfEligible(bestJobId, ageMs, bestCls, now);
 * }</pre>
 *
 * <p>This class is package-private and shared by all {@link
 * ai.floedb.floecat.reconciler.jobs.ReconcileJobStore} implementations.
 */
public final class AgingPromotionTracker {

  /** Side map: jobId → promotion-expiry-ms. Cleaned lazily in {@link #cleanupExpired}. */
  private final Map<String, Long> promotedJobIds = new ConcurrentHashMap<>();

  /** Cumulative count of starvation-aging promotions since this instance was created. */
  private final AtomicLong total = new AtomicLong();

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Removes expired entries from the side map.
   *
   * <p>Should be called once per {@code leaseNext()} invocation before any eligibility checks.
   * Bounded: only iterates entries that exist (typically a small set since only aging jobs are
   * tracked).
   *
   * @param now current wall-clock time in ms
   */
  public void cleanupExpired(long now) {
    if (!promotedJobIds.isEmpty()) {
      promotedJobIds.entrySet().removeIf(e -> e.getValue() < now);
    }
  }

  /**
   * Records a starvation-aging promotion for the given job if it is eligible.
   *
   * <p>A job is eligible when:
   *
   * <ul>
   *   <li>Its age exceeds the class's threshold ({@link SchedulerStoreHelpers#agingThresholdMs}).
   *   <li>It has not been promoted within the current cooldown window.
   * </ul>
   *
   * <p>When eligible, the job is added to the side map with an expiry of {@code now + cooldownMs},
   * and the cumulative counter is incremented.
   *
   * @param jobId the job being dispatched
   * @param ageMs elapsed time since the job was enqueued, in ms
   * @param cls the priority class of the job
   * @param now current wall-clock time in ms
   * @return {@code true} if a promotion was recorded; {@code false} otherwise
   */
  public boolean recordIfEligible(String jobId, long ageMs, StatsPriorityClass cls, long now) {
    if (!promotedJobIds.containsKey(jobId) && ageMs > SchedulerStoreHelpers.agingThresholdMs(cls)) {
      promotedJobIds.put(jobId, now + SchedulerStoreHelpers.AGING_COOLDOWN_MS);
      total.incrementAndGet();
      return true;
    }
    return false;
  }

  /**
   * Returns the cumulative number of starvation-aging promotions recorded since this tracker was
   * created.
   */
  public long totalPromotions() {
    return total.get();
  }
}
