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
 * Encapsulates the mutable health-band state for a single job-store instance.
 *
 * <p>Health band transitions follow asymmetric update paths: enqueue-time escalation is fast
 * (within 1 s, via bounded depth checks), while full-scan recomputation (typically every 15 s)
 * applies an authoritative value in either direction. RED clearance is immediate once the P0 ready
 * queue empties.
 *
 * <p>Three methods cover the three call sites that affect band state:
 *
 * <ol>
 *   <li>{@link #maybeEscalate} — called at enqueue time; rate-limited to once per second via CAS.
 *   <li>{@link #maybeClearRedOnP0Drain} — called after every successful dispatch; O(1) on the
 *       common path (not RED).
 *   <li>{@link #computeAndSet} — called from the periodic {@code queueStats()} full scan; applies
 *       the authoritative band from the full snapshot.
 * </ol>
 *
 * <p>P1_FRESHNESS depth is intentionally excluded from band computation — P1 jobs are expected to
 * drain within seconds of dispatch and should not affect the backpressure signal. A large P1
 * backlog may therefore show GREEN band; monitor {@code queue.depth_by_class} with {@code
 * priority_class=P1_FRESHNESS} separately if P1 SLO alerting is needed.
 *
 * <p>This class is declared {@code public} and shared across modules — used by both the in-memory
 * store ({@code reconciler} module) and the durable store ({@code service} module). It is not part
 * of the public API and must not be used outside of job-store implementations.
 */
public final class SchedulerBandState {

  // ---------------------------------------------------------------------------
  // Thresholds — package-private so tests can reference them
  // ---------------------------------------------------------------------------

  /** Oldest P0 job age (ms) that triggers an immediate escalation to RED. */
  static final long P0_RED_BUDGET_MS = 1_000L;

  /** P2_REPAIR ready-queue depth that triggers ORANGE. */
  static final long P2_ORANGE_THRESHOLD = 200L;

  /** P3_BACKGROUND ready-queue depth that triggers YELLOW. */
  static final long P3_YELLOW_THRESHOLD = 500L;

  /** Minimum interval between escalation checks (ms). */
  static final long BAND_REFRESH_INTERVAL_MS = 1_000L;

  // ---------------------------------------------------------------------------
  // Mutable state
  // ---------------------------------------------------------------------------

  private final AtomicReference<SchedulerHealthBand> current =
      new AtomicReference<>(SchedulerHealthBand.GREEN);

  /**
   * Timestamp of the last escalation attempt. A CAS on this value elects exactly one thread per
   * {@link #BAND_REFRESH_INTERVAL_MS} to run the escalation body, preventing concurrent O(n) scans
   * under burst enqueue load.
   */
  private final AtomicLong lastEscalateMs = new AtomicLong(0L);

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Returns the current health band. Thread-safe; reads one {@link AtomicReference}. */
  public SchedulerHealthBand current() {
    return current.get();
  }

  /**
   * Potentially escalates the health band based on current queue depths and P0 starvation.
   *
   * <p>Rate-limited to at most once per {@link #BAND_REFRESH_INTERVAL_MS}. Only escalates — never
   * downgrades. Downgrade is handled exclusively by {@link #computeAndSet}.
   *
   * @param now current wall-clock time in ms
   * @param depths ready-queue depth by priority class (from {@code readyQueue.sizeByAllClasses()})
   * @param oldestP0AgeMs age of the oldest P0 queued job in ms; pass {@code 0L} when no P0 jobs are
   *     queued (avoids the O(n) jobs-map scan cost in the common case)
   */
  public void maybeEscalate(long now, Map<StatsPriorityClass, Long> depths, long oldestP0AgeMs) {
    long last = lastEscalateMs.get();
    if (now - last < BAND_REFRESH_INTERVAL_MS) {
      return;
    }
    // CAS to claim this escalation slot — only one thread runs the body per TTL period.
    if (!lastEscalateMs.compareAndSet(last, now)) {
      return;
    }

    SchedulerHealthBand required = computeRequired(depths, oldestP0AgeMs);
    SchedulerHealthBand c;
    while ((c = current.get()).ordinal() < required.ordinal()) {
      if (current.compareAndSet(c, required)) {
        break;
      }
      // Another thread concurrently escalated — re-read and check whether the new band is still
      // below the required level.
    }
  }

  /**
   * Clears a P0-timeout RED band immediately when the P0 ready queue empties.
   *
   * <p>O(1) on the common path (band not RED): one {@link AtomicReference#get()} and return. When
   * RED and P0 depth is zero, CAS-downgrades to the band warranted by remaining P2/P3 depths.
   *
   * <p>Bounded TOCTOU: a new P0 job could arrive between the depth check and the CAS. If that
   * happens the CAS succeeds and momentarily sets a non-RED band; the next {@link #maybeEscalate}
   * call (within 1 s) re-escalates. This one-cycle window is acceptable.
   *
   * @param depths ready-queue depth by priority class (fresh snapshot, O(1) reads)
   */
  public void maybeClearRedOnP0Drain(Map<StatsPriorityClass, Long> depths) {
    if (current.get() != SchedulerHealthBand.RED) {
      return;
    }
    if (depths.getOrDefault(StatsPriorityClass.P0_SYNC, 0L) > 0L) {
      return;
    }
    // P0 queue is empty — recompute target band from P2/P3 depths and CAS-downgrade.
    SchedulerHealthBand required = computeRequired(depths, 0L);
    current.compareAndSet(SchedulerHealthBand.RED, required);
  }

  /**
   * Authoritatively sets the band from a full-state snapshot.
   *
   * <p>Called from the periodic {@code queueStats()} full scan. The full scan is authoritative for
   * both escalation and downgrade because it has a complete queue snapshot (including oldest queued
   * P0 age), unlike enqueue-time escalation which intentionally uses a bounded approximation.
   *
   * <p>This method therefore applies the computed band in both directions (up and down). A
   * concurrent enqueue-time {@link #maybeEscalate} can still race with this write, but the next
   * full scan (or next enqueue-time escalate) converges quickly.
   *
   * @param depths ready-queue depth by priority class (from the full scan)
   * @param oldestP0AgeMs age of the oldest P0 queued job; pass {@code 0L} if no P0 jobs
   * @return the band that was computed and written
   */
  public SchedulerHealthBand computeAndSet(
      Map<StatsPriorityClass, Long> depths, long oldestP0AgeMs) {
    SchedulerHealthBand band = computeRequired(depths, oldestP0AgeMs);
    current.set(band);
    return band;
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Package-private for testing: directly force the health band, bypassing all transition rules.
   */
  void setForTest(SchedulerHealthBand band) {
    current.set(band);
  }

  /**
   * Package-private for testing: reset the escalation TTL so the next call to maybeEscalate
   * triggers immediately.
   */
  void resetEscalateCooldownForTest() {
    lastEscalateMs.set(0L);
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private static SchedulerHealthBand computeRequired(
      Map<StatsPriorityClass, Long> depths, long oldestP0AgeMs) {
    long p0Depth = depths.getOrDefault(StatsPriorityClass.P0_SYNC, 0L);
    long p2Depth = depths.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L);
    long p3Depth = depths.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L);

    if (p0Depth > 0L && oldestP0AgeMs > P0_RED_BUDGET_MS) {
      return SchedulerHealthBand.RED;
    }
    if (p2Depth > P2_ORANGE_THRESHOLD) {
      return SchedulerHealthBand.ORANGE;
    }
    if (p3Depth > P3_YELLOW_THRESHOLD) {
      return SchedulerHealthBand.YELLOW;
    }
    return SchedulerHealthBand.GREEN;
  }
}
