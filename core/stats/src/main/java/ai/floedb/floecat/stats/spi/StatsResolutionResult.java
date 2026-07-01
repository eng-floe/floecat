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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.Objects;
import java.util.Optional;

/**
 * Result of a single stats resolution attempt by the orchestrator.
 *
 * <p>Carries both the optional stats payload and the {@link StatsSyncOutcome} that describes how
 * the result was obtained — or why it is absent. Callers that only care about the payload can call
 * {@link #stats()} directly; callers that need quality context should inspect {@link #outcome()}.
 */
public record StatsResolutionResult(
    Optional<TargetStatsRecord> stats,
    StatsSyncOutcome outcome,
    String outcomeDetail,
    boolean stale) {

  public StatsResolutionResult {
    stats = Objects.requireNonNull(stats, "stats");
    outcome = Objects.requireNonNull(outcome, "outcome");
    outcomeDetail = outcomeDetail == null ? "" : outcomeDetail;
  }

  /** {@code true} if stats data is present in this result. */
  public boolean hasStats() {
    return stats.isPresent();
  }

  public static StatsResolutionResult hit(TargetStatsRecord record) {
    return new StatsResolutionResult(
        Optional.of(Objects.requireNonNull(record, "record")), StatsSyncOutcome.HIT, "", false);
  }

  /**
   * Stats from a prior snapshot (stale fallback). The result is semantically a hit but the planner
   * should mark it {@code HIT_STALE} in the response.
   */
  public static StatsResolutionResult staleHit(TargetStatsRecord record, String detail) {
    return new StatsResolutionResult(
        Optional.of(Objects.requireNonNull(record, "record")), StatsSyncOutcome.HIT, detail, true);
  }

  public static StatsResolutionResult captured(TargetStatsRecord record) {
    return new StatsResolutionResult(
        Optional.of(Objects.requireNonNull(record, "record")),
        StatsSyncOutcome.CAPTURED,
        "",
        false);
  }

  public static StatsResolutionResult partial(String detail) {
    return new StatsResolutionResult(Optional.empty(), StatsSyncOutcome.PARTIAL, detail, false);
  }

  public static StatsResolutionResult timeout(String detail) {
    return new StatsResolutionResult(Optional.empty(), StatsSyncOutcome.TIMEOUT, detail, false);
  }

  public static StatsResolutionResult failed(String detail) {
    return new StatsResolutionResult(Optional.empty(), StatsSyncOutcome.FAILED, detail, false);
  }

  public static StatsResolutionResult skipped(String reason) {
    return new StatsResolutionResult(Optional.empty(), StatsSyncOutcome.SKIPPED, reason, false);
  }
}
