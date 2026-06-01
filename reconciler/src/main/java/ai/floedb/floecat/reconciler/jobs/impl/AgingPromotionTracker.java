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

import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks starvation-aging promotion events for metrics reporting.
 *
 * <p>This class does <em>not</em> reorder any queue — it only records when a job's effective
 * priority class was temporarily elevated due to age. The actual promotion (dispatching the job at
 * a higher class) happens in the store's lease loop; this tracker simply prevents the same job from
 * being counted twice within the cooldown window and maintains a cumulative counter for the {@code
 * reconcile.aging.promotions} metric.
 *
 * <p>All public methods are thread-safe.
 */
public final class AgingPromotionTracker {

  /**
   * Maps jobId → promotion-expiry-ms. An entry here means the job was recently promoted and should
   * not be re-promoted until the entry expires.
   */
  private final ConcurrentHashMap<String, Long> promotedJobIds = new ConcurrentHashMap<>();

  /** Cumulative number of promotions since the tracker was created. */
  private final AtomicLong total = new AtomicLong();

  /**
   * Records a promotion for {@code jobId} if the job has been waiting longer than its aging
   * threshold and is not currently in the cooldown window.
   *
   * @param jobId the job being considered for promotion
   * @param ageMs how long the job has been waiting (ms since enqueue)
   * @param cls the job's current (un-promoted) priority class
   * @param nowMs current epoch-ms timestamp
   * @return {@code true} if the promotion was recorded (caller may dispatch at promoted class)
   */
  public boolean recordIfEligible(String jobId, long ageMs, StatsPriorityClass cls, long nowMs) {
    if (ageMs < SchedulerStoreHelpers.agingThresholdMs(cls)) {
      return false; // not old enough
    }
    Long expiryMs = promotedJobIds.get(jobId);
    if (expiryMs != null && nowMs < expiryMs) {
      return false; // in cooldown
    }
    // Record promotion: update expiry and increment counter.
    promotedJobIds.put(jobId, nowMs + SchedulerStoreHelpers.AGING_COOLDOWN_MS);
    total.incrementAndGet();
    return true;
  }

  /**
   * Removes expired promotion entries. Intended to be called lazily on each {@code leaseNext()}
   * invocation to prevent the side map from growing without bound.
   */
  public void cleanupExpired(long nowMs) {
    promotedJobIds.entrySet().removeIf(e -> nowMs >= e.getValue());
  }

  /**
   * Returns the total number of aging promotions recorded since this tracker was created. Suitable
   * for delta-emitting a counter metric.
   */
  public long totalPromotions() {
    return total.get();
  }
}
