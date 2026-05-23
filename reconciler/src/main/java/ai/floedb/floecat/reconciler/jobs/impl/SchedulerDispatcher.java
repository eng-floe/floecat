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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared WRR dispatch logic for both in-memory and durable reconcile job stores.
 *
 * <p>Holds per-lane virtual-time counters (WRR state) and provides:
 *
 * <ul>
 *   <li>{@link #selectWinner} — WRR tournament among eligible candidates
 *   <li>{@link #recordDispatch} — increments the winner lane's virtual-time counter
 *   <li>{@link #shouldBlockLowerClasses} — P0 guard decision
 * </ul>
 *
 * <p>State is in-memory and best-effort — counters reset on restart. That is intentional: WRR is a
 * fairness optimization, not a correctness requirement. Brief unfairness after a restart is
 * acceptable.
 */
public final class SchedulerDispatcher {

  /**
   * Maximum number of eligible candidates the <em>in-memory</em> store compares in one WRR
   * tournament per priority class. Candidates beyond this cap are left for subsequent lease cycles.
   *
   * <p>The durable store does not use this cap directly — it collects all due candidates per class
   * (bounded by {@code readyScanLimit} pages) before running WRR selection. The constant is kept
   * here for consistency and to allow both stores to reference the same value if needed.
   *
   * <p>Score values are bounded well below {@link Long#MAX_VALUE} (max ~600 with default weights),
   * so negating the score in {@link #selectWinner}'s comparator is safe from overflow.
   */
  static final int MAX_WRR_CANDIDATES = 8;

  /**
   * A candidate eligible for WRR tournament selection.
   *
   * @param jobId the job identifier
   * @param laneKey the WRR fairness unit (account:table, or blank for system jobs)
   * @param score the priority score — higher wins ties in virtual time
   */
  record WrrCandidate(String jobId, String laneKey, long score) {}

  // Per-lane dispatch count. Used as WRR virtual time: lower count = dispatch sooner.
  // ConcurrentHashMap provides thread safety for concurrent callers — multiple leaseNext()
  // invocations can run simultaneously. Individual merge() and getOrDefault() calls are atomic;
  // WRR selection reads a snapshot of the map which may be transiently inconsistent under high
  // concurrency. This is intentional: WRR is a best-effort fairness optimisation and a stale read
  // has no correctness consequence.
  private final ConcurrentHashMap<String, Long> laneServiceCounts = new ConcurrentHashMap<>();

  /**
   * Select the WRR winner from a list of eligible candidates.
   *
   * <p>Ranks candidates by {@code (virtualTime ASC, score DESC)}: lowest virtual time wins; ties
   * broken by score. Does NOT call {@link #recordDispatch} — the caller must do so after
   * successfully leasing the winner.
   *
   * @param candidates up to {@link #MAX_WRR_CANDIDATES} eligible candidates; the caller is
   *     responsible for capping the input list
   * @return the WRR winner, or empty if {@code candidates} is empty
   */
  public Optional<WrrCandidate> selectWinner(List<WrrCandidate> candidates) {
    if (candidates.isEmpty()) {
      return Optional.empty();
    }
    return candidates.stream()
        .min(
            Comparator.comparingLong(
                    (WrrCandidate c) -> laneServiceCounts.getOrDefault(c.laneKey(), 0L))
                .thenComparingLong(c -> -c.score())); // negate: higher score preferred
  }

  /**
   * Record that a job was successfully dispatched from {@code laneKey}. Increments that lane's
   * virtual-time counter. No-op for blank lane keys (system jobs with no fairness unit).
   */
  public void recordDispatch(String laneKey) {
    if (laneKey != null && !laneKey.isBlank()) {
      laneServiceCounts.merge(laneKey, 1L, Long::sum);
    }
  }

  /**
   * Returns the current WRR virtual time for the given lane key. Used by the durable store to sort
   * lane representatives before dispatch.
   */
  public long virtualTimeFor(String laneKey) {
    if (laneKey == null || laneKey.isBlank()) return 0L;
    return laneServiceCounts.getOrDefault(laneKey, 0L);
  }

  /**
   * Whether the P0 priority guard should block lower priority classes after scanning {@code cls}.
   *
   * <p>Returns {@code true} only when {@code cls == P0_SYNC} and at least one candidate was
   * <em>blocked</em> (lane-held, backoff-deferred, snapshot-lease-failed, or WRR loser) — meaning
   * this executor could potentially run the job but cannot do so right now.
   *
   * <p>Filter-rejected candidates (wrong execution class, wrong pinned executor) do NOT trigger the
   * guard: an executor that cannot run a P0 HEAVY job is not obligated to sit idle — it should
   * dispatch lower-priority work it is capable of.
   *
   * @param cls the priority class just scanned
   * @param blockedCount number of this-executor-compatible candidates that existed but could not be
   *     leased (lane contention, backoff, snapshot, CAS race, WRR loser)
   */
  public static boolean shouldBlockLowerClasses(StatsPriorityClass cls, int blockedCount) {
    return cls == StatsPriorityClass.P0_SYNC && blockedCount > 0;
  }
}
