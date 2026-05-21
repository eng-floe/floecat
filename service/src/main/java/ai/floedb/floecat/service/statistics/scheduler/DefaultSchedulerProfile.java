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

package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Default scheduler profile: deterministic 3-factor scoring with band-based admission control.
 *
 * <h2>Priority assignment</h2>
 *
 * <p>The {@link #assign} implementation computes a {@code priorityScore} for async jobs using three
 * factors: coverage gap, row delta, and staleness age. Each factor contributes a value in [0, 100]
 * and is multiplied by a configurable weight (defaults: coverage=3, delta=2, age=1). Scores are
 * additive; higher scores are dispatched first within a priority class.
 *
 * <table>
 *   <caption>Factor contributions</caption>
 *   <tr><th>Factor</th><th>Input source</th><th>Contribution</th></tr>
 *   <tr><td>Coverage</td><td>{@link SchedulerContext#coverageLevel}</td>
 *       <td>NONE→100, PARTIAL→50, FULL→0</td></tr>
 *   <tr><td>Delta</td><td>{@link SchedulerContext#snapshotDeltaRows}</td>
 *       <td>empty→50, &gt;1M rows→80, &gt;100K rows→40, else→10</td></tr>
 *   <tr><td>Age</td><td>{@link SchedulerContext#lastSuccessfulCaptureMs}</td>
 *       <td>empty→100; linear 0→100 over {@code maxAgeMs} (default 24h)</td></tr>
 * </table>
 *
 * <p>When a request carries non-empty {@code columnSelectors}, coverage is treated as {@link
 * CoverageLevel#NONE} for those columns regardless of the aggregate coverage level, biasing the job
 * toward capturing rather than skipping.
 *
 * <h2>Admission control</h2>
 *
 * <p>P0 and P1 are always admitted. P2 is deferred only in RED. P3 is deferred in ORANGE and RED.
 * YELLOW applies no deferral at this layer; the store's own probabilistic deferral handles P3 under
 * YELLOW.
 *
 * <h2>Preemption victim selection</h2>
 *
 * <p>Selects the most recently started eligible candidate (highest {@code startedAtMs}) to minimise
 * wasted work. Returns empty if the candidate list is empty.
 */
@ApplicationScoped
@SchedulerProfile(name = "default")
public class DefaultSchedulerProfile
    implements SchedulerPriorityPolicy, SchedulerAdmissionPolicy, SchedulerPreemptionPolicy {

  // ---- Score factor weights ----
  private final int weightCoverage;
  private final int weightDelta;
  private final int weightAge;

  /** Maximum age before the age factor saturates at 100. Default: 24 hours in milliseconds. */
  private final long maxAgeMs;

  // ---- Delta row-count thresholds ----
  private static final long DELTA_LARGE_ROWS = 1_000_000L;
  private static final long DELTA_MEDIUM_ROWS = 100_000L;

  @Inject
  DefaultSchedulerProfile(
      @ConfigProperty(name = "floecat.stats.scheduler.scoring.weight.coverage", defaultValue = "3")
          int weightCoverage,
      @ConfigProperty(name = "floecat.stats.scheduler.scoring.weight.delta", defaultValue = "2")
          int weightDelta,
      @ConfigProperty(name = "floecat.stats.scheduler.scoring.weight.age", defaultValue = "1")
          int weightAge,
      @ConfigProperty(
              name = "floecat.stats.scheduler.scoring.max-age-ms",
              defaultValue = "86400000")
          long maxAgeMs) {
    this.weightCoverage = weightCoverage > 0 ? weightCoverage : 3;
    this.weightDelta = weightDelta > 0 ? weightDelta : 2;
    this.weightAge = weightAge > 0 ? weightAge : 1;
    this.maxAgeMs = maxAgeMs > 0L ? maxAgeMs : 86_400_000L;
  }

  // ---------------------------------------------------------------------------
  // SchedulerPriorityPolicy
  // ---------------------------------------------------------------------------

  /**
   * Computes a 3-factor priority score for an async stats capture request.
   *
   * <p>The returned {@link PriorityAssignment#priorityClass()} is always {@link
   * StatsPriorityClass#P3_BACKGROUND}. The stats orchestrator determines the actual class from
   * enqueue context (P0 for sync, P2 for follow-up, P3 for background) and does not use the
   * policy's class field for that path — only {@link PriorityAssignment#score()} and {@link
   * PriorityAssignment#laneKey()} are applied. Custom profiles that need to influence urgency
   * should express it through the score.
   *
   * <p>P0_SYNC is never returned.
   */
  @Override
  public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
    String tableKey = request.tableId().getId();
    long score = computeScore(request, context, tableKey);
    String laneKey = request.tableId().getAccountId() + ":" + tableKey;
    return new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, score, laneKey);
  }

  // ---------------------------------------------------------------------------
  // SchedulerAdmissionPolicy
  // ---------------------------------------------------------------------------

  /**
   * Band-based admission control.
   *
   * <ul>
   *   <li>P0_SYNC: always {@link AdmissionDecision#ADMIT} (invariant required by contract).
   *   <li>P1_FRESHNESS: always ADMIT.
   *   <li>P2_REPAIR: DEFER only in RED; ADMIT otherwise.
   *   <li>P3_BACKGROUND: DEFER in ORANGE or RED; ADMIT in GREEN or YELLOW.
   * </ul>
   */
  @Override
  public AdmissionDecision decide(PriorityAssignment assignment, SchedulerHealthBand currentBand) {
    return switch (assignment.priorityClass()) {
      case P0_SYNC, P1_FRESHNESS -> AdmissionDecision.ADMIT;
      case P2_REPAIR ->
          (currentBand == SchedulerHealthBand.RED)
              ? AdmissionDecision.DEFER
              : AdmissionDecision.ADMIT;
      case P3_BACKGROUND ->
          (currentBand == SchedulerHealthBand.ORANGE || currentBand == SchedulerHealthBand.RED)
              ? AdmissionDecision.DEFER
              : AdmissionDecision.ADMIT;
    };
  }

  // ---------------------------------------------------------------------------
  // SchedulerPreemptionPolicy
  // ---------------------------------------------------------------------------

  /**
   * Selects the most recently started candidate (highest {@code startedAtMs}) to minimise wasted
   * completed work. Returns empty if the candidate list is empty.
   *
   * <p>Never returns a {@link StatsPriorityClass#P0_SYNC} job (the candidates list provided by the
   * scheduler is already filtered to exclude P0 jobs, but this method enforces the invariant
   * defensively).
   */
  @Override
  public Optional<String> selectVictim(
      String incomingJobId, List<RunningJobInfo> candidates, SchedulerContext context) {
    return candidates.stream()
        .filter(c -> c.priorityClass() != StatsPriorityClass.P0_SYNC)
        .max(Comparator.comparingLong(RunningJobInfo::startedAtMs))
        .map(RunningJobInfo::jobId);
  }

  // ---------------------------------------------------------------------------
  // Score computation
  // ---------------------------------------------------------------------------

  private long computeScore(
      StatsCaptureRequest request, SchedulerContext context, String tableKey) {
    long coverageScore = coverageScore(request, context, tableKey);
    long deltaScore = deltaScore(context, tableKey, request.snapshotId());
    long ageScore = ageScore(context, tableKey);
    return (long) weightCoverage * coverageScore
        + (long) weightDelta * deltaScore
        + (long) weightAge * ageScore;
  }

  /**
   * Coverage factor: NONE→100, PARTIAL→50, FULL→0.
   *
   * <p>When the request carries column selectors, coverage is treated as NONE for those columns
   * regardless of the aggregate level, biasing the job toward capturing.
   */
  private long coverageScore(
      StatsCaptureRequest request, SchedulerContext context, String tableKey) {
    CoverageLevel coverage;
    if (!request.columnSelectors().isEmpty()) {
      // Column-scoped request: per-column coverage not tracked yet; bias toward NONE.
      coverage = CoverageLevel.NONE;
    } else {
      coverage = context.coverageLevel(tableKey, request.snapshotId());
    }
    return switch (coverage) {
      case NONE -> 100L;
      case PARTIAL -> 50L;
      case FULL -> 0L;
    };
  }

  /**
   * Delta factor: empty (unknown) → 50; {@literal >}1M rows → 80; {@literal >}100K rows → 40; else
   * → 10.
   */
  private long deltaScore(SchedulerContext context, String tableKey, long snapshotId) {
    var deltaOpt = context.snapshotDeltaRows(tableKey, snapshotId);
    if (deltaOpt.isEmpty()) {
      return 50L; // unknown delta: conservative mid-range
    }
    long delta = deltaOpt.getAsLong();
    if (delta > DELTA_LARGE_ROWS) {
      return 80L;
    } else if (delta > DELTA_MEDIUM_ROWS) {
      return 40L;
    } else {
      return 10L;
    }
  }

  /**
   * Age factor: empty (never captured) → 100; linear 0→100 over {@link #maxAgeMs}; clamped at 100.
   */
  private long ageScore(SchedulerContext context, String tableKey) {
    var lastCaptureOpt = context.lastSuccessfulCaptureMs(tableKey);
    if (lastCaptureOpt.isEmpty()) {
      return 100L; // never captured: maximum urgency
    }
    long ageMs = System.currentTimeMillis() - lastCaptureOpt.getAsLong();
    if (ageMs <= 0L) {
      return 0L;
    }
    if (ageMs >= maxAgeMs) {
      return 100L;
    }
    return (ageMs * 100L) / maxAgeMs;
  }
}
