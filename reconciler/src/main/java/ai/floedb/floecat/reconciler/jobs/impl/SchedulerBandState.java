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

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe health-band state machine for a single job-store instance.
 *
 * <h2>Transitions</h2>
 *
 * <ul>
 *   <li>{@link #maybeEscalate} — called on each {@code leaseNext()} or {@code enqueue()} call;
 *       rate-limited to at most once per {@link #BAND_REFRESH_INTERVAL_MS} to keep the hot path
 *       cheap. Only escalates; never downgrades.
 *   <li>{@link #maybeClearRedOnP0Drain} — fast-path P0 drain check; clears RED immediately when P0
 *       queue empties, regardless of the rate-limit.
 *   <li>{@link #computeAndSet} — authoritative full-scan called from {@code queueStats()}; both
 *       escalates and downgrades; updates the stored band.
 * </ul>
 *
 * <h2>Thread safety</h2>
 *
 * <p>All methods are safe for concurrent callers. {@link #maybeEscalate} uses a compare-and-swap on
 * {@link #lastEscalateMs} to enforce the rate limit without blocking.
 */
final class SchedulerBandState {

  /** P0 job age above which the band escalates to RED immediately. */
  static final long P0_RED_BUDGET_MS = 1_000L;

  /** P2 queue depth above which the band escalates to at least ORANGE. */
  static final long P2_ORANGE_THRESHOLD = 200L;

  /** P3 queue depth above which the band escalates to at least YELLOW. */
  static final long P3_YELLOW_THRESHOLD = 500L;

  /** Minimum interval between {@link #maybeEscalate} evaluations (avoids hot-path contention). */
  static final long BAND_REFRESH_INTERVAL_MS = 1_000L;

  private final AtomicReference<SchedulerHealthBand> current =
      new AtomicReference<>(SchedulerHealthBand.GREEN);
  private final AtomicLong lastEscalateMs = new AtomicLong(0L);

  /** Returns the current health band. O(1), non-blocking. */
  SchedulerHealthBand current() {
    return current.get();
  }

  /**
   * Rate-limited escalation check. Compares depth thresholds and P0 staleness against the current
   * band; escalates if any threshold is exceeded. Never downgrades — downgrade happens only in
   * {@link #computeAndSet}. Returns the current band after the check.
   */
  SchedulerHealthBand maybeEscalate(
      long nowMs, Map<StatsPriorityClass, Long> queuedByClass, long oldestP0AgeMs) {
    // Rate-limit: only run the full check once per BAND_REFRESH_INTERVAL_MS.
    long last = lastEscalateMs.get();
    if (nowMs - last < BAND_REFRESH_INTERVAL_MS) {
      return current.get();
    }
    if (!lastEscalateMs.compareAndSet(last, nowMs)) {
      return current.get(); // another thread won the CAS
    }

    SchedulerHealthBand computed = computeBand(queuedByClass, oldestP0AgeMs);
    current.updateAndGet(prev -> prev.ordinal() >= computed.ordinal() ? prev : computed);
    return current.get();
  }

  /**
   * Immediately clears RED when the P0 queue is empty. This is a fast-path check that runs on every
   * {@code leaseNext()} invocation without rate-limiting so that P0 drain is reflected without
   * waiting for the next {@link #maybeEscalate} tick.
   */
  void maybeClearRedOnP0Drain(Map<StatsPriorityClass, Long> queuedByClass) {
    if (current.get() != SchedulerHealthBand.RED) {
      return;
    }
    long p0Depth = queuedByClass.getOrDefault(StatsPriorityClass.P0_SYNC, 0L);
    if (p0Depth == 0L) {
      current.compareAndSet(SchedulerHealthBand.RED, SchedulerHealthBand.ORANGE);
    }
  }

  /**
   * Authoritative band computation called from {@code queueStats()}. Both escalates and downgrades,
   * and updates the stored band. Returns the new band.
   */
  SchedulerHealthBand computeAndSet(
      Map<StatsPriorityClass, Long> queuedByClass, long oldestP0AgeMs) {
    SchedulerHealthBand computed = computeBand(queuedByClass, oldestP0AgeMs);
    current.set(computed);
    lastEscalateMs.set(System.currentTimeMillis()); // reset rate-limit after authoritative scan
    return computed;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private static SchedulerHealthBand computeBand(
      Map<StatsPriorityClass, Long> queuedByClass, long oldestP0AgeMs) {
    // RED: P0 job is stalled beyond the sync budget.
    if (oldestP0AgeMs > P0_RED_BUDGET_MS) {
      return SchedulerHealthBand.RED;
    }
    long p2Depth = queuedByClass.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L);
    if (p2Depth > P2_ORANGE_THRESHOLD) {
      return SchedulerHealthBand.ORANGE;
    }
    long p3Depth = queuedByClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L);
    if (p3Depth > P3_YELLOW_THRESHOLD) {
      return SchedulerHealthBand.YELLOW;
    }
    return SchedulerHealthBand.GREEN;
  }
}
