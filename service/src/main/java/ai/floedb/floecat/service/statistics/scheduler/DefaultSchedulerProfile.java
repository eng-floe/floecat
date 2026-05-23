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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Default scheduler profile: deterministic 3-factor scoring with band-based admission control.
 *
 * <h2>Priority assignment</h2>
 *
 * <p>The {@link #assign} implementation computes a {@code priorityScore} for async jobs using three
 * factors: coverage gap, row delta, and staleness age. Each factor contributes a value in [0, 100]
 * and is multiplied by a configurable weight (defaults: coverage=3, delta=2, age=1). The weighted
 * sum is then scaled by a planner demand multiplier and divided by a cost estimate for the
 * requested stat kinds, then normalised to [0, 1000]. Higher scores are dispatched first within a
 * priority class.
 *
 * <table>
 *   <caption>Factor contributions and modifiers</caption>
 *   <tr><th>Factor/modifier</th><th>Input source</th><th>Effect</th></tr>
 *   <tr><td>Coverage</td><td>{@link SchedulerContext#coverageLevel}</td>
 *       <td>NONE→100, PARTIAL→50, FULL→0 (× weight, default 3)</td></tr>
 *   <tr><td>Delta</td><td>{@link SchedulerContext#snapshotDeltaRows}</td>
 *       <td>empty→50, &gt;1M rows→80, &gt;100K rows→40, else→10 (× weight, default 2)</td></tr>
 *   <tr><td>Age</td><td>{@link SchedulerContext#lastSuccessfulCaptureMs}</td>
 *       <td>empty→100; linear 0→100 over {@code maxAgeMs} (default 24h) (× weight, default 1)</td></tr>
 *   <tr><td>Demand multiplier</td><td>{@link SchedulerContext#recentPlannerRequestCount}</td>
 *       <td>[floor, 1.0] — saturates at {@code demandSaturation} hits (default floor=0.15, sat=50)</td></tr>
 *   <tr><td>Cost divisor</td><td>{@code requestedKinds}</td>
 *       <td>ROW_COUNT/FILE_COUNT/TOTAL_BYTES→1.0, NULL_COUNT/MIN_MAX→2.0, NDV/HISTOGRAM→3.0,
 *           empty→2.0</td></tr>
 * </table>
 *
 * <p>The final score is normalised to [0, 1000]: {@code min(1000, round(demandMultiplier × rawValue
 * / costDivisor × 1000 / maxRawValue))} where {@code maxRawValue = (weightCoverage + weightDelta +
 * weightAge) × 100}.
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

  /**
   * Floor value of the demand multiplier (applied when a table has zero recent planner demand).
   * Range: (0, 1]. Default: 0.15 — jobs for idle tables get 15% of a fully-demanded table's score.
   */
  private final double demandMultiplierFloor;

  /**
   * Number of planner demand hits at which the demand multiplier saturates at 1.0. Default: 50.
   * Tables with ≥ saturations hits get multiplier=1.0; hits above this are not rewarded further.
   */
  private final long demandSaturation;

  private final Observability observability;

  // ---- Delta row-count thresholds ----
  private static final long DELTA_LARGE_ROWS = 1_000_000L;
  private static final long DELTA_MEDIUM_ROWS = 100_000L;

  private static final Tag COMPONENT = Tag.of(TagKey.COMPONENT, "service");
  private static final Tag OPERATION = Tag.of(TagKey.OPERATION, "scoring");

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
          long maxAgeMs,
      @ConfigProperty(
              name = "floecat.stats.scheduler.scoring.demand.multiplier-floor",
              defaultValue = "0.15")
          double demandMultiplierFloor,
      @ConfigProperty(
              name = "floecat.stats.scheduler.scoring.demand.saturation",
              defaultValue = "50")
          long demandSaturation,
      Observability observability) {
    // A configured weight of 0 is treated as the default (not "disable this factor").
    // To effectively disable a factor, set its weight to a very small positive value (e.g. 1)
    // relative to other weights.
    this.weightCoverage = weightCoverage > 0 ? weightCoverage : 3;
    this.weightDelta = weightDelta > 0 ? weightDelta : 2;
    this.weightAge = weightAge > 0 ? weightAge : 1;
    this.maxAgeMs = maxAgeMs > 0L ? maxAgeMs : 86_400_000L;
    this.demandMultiplierFloor =
        (demandMultiplierFloor > 0.0 && demandMultiplierFloor <= 1.0)
            ? demandMultiplierFloor
            : 0.15;
    this.demandSaturation = demandSaturation > 0L ? demandSaturation : 50L;
    this.observability = Objects.requireNonNull(observability, "observability");
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
    // tableKey must include accountId to match SchedulerSignalIndex key convention
    // ("accountId:tableId") so that scoring signals are correctly scoped per-account.
    String tableKey =
        SchedulerSignalIndex.tableKey(request.tableId().getAccountId(), request.tableId().getId());
    long score = computeScore(request, context, tableKey);
    String laneKey = tableKey; // laneKey and tableKey are now the same compound form
    // DefaultSchedulerProfile always assigns P3_BACKGROUND from this path (P0/P2 are set by the
    // orchestrator directly; P1 is set by the planner worker for new snapshots). Build the
    // assignment first so the metric tag derives from the actual returned class rather than a
    // hardcoded literal — this stays correct if a subclass or future profile changes the class.
    PriorityAssignment assignment =
        new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, score, laneKey);
    observability.summary(
        ServiceMetrics.Reconcile.SCORING_SCORE_DIST,
        score,
        COMPONENT,
        OPERATION,
        Tag.of("priority_class", assignment.priorityClass().name().toLowerCase()));
    return assignment;
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
    long rawValue =
        (long) weightCoverage * coverageScore
            + (long) weightDelta * deltaScore
            + (long) weightAge * ageScore;
    double multiplier = computeDemandMultiplier(context, tableKey, request);
    double costScore = computeCostScore(request.requestedKinds());
    // Normalise to [0, 1000]: (multiplier × rawValue / costScore) / maxRawValue × 1000.
    // maxRawValue = (weightCoverage + weightDelta + weightAge) × 100 — the theoretical maximum
    // raw score when all three factors are at their ceiling and the demand multiplier is 1.0.
    double maxRawValue = ((double) weightCoverage + weightDelta + weightAge) * 100.0;
    return Math.min(1000L, Math.round((multiplier * rawValue / costScore) * 1000.0 / maxRawValue));
  }

  /**
   * Cost estimate for the requested stat kinds. Higher-cost stat kinds reduce the effective score
   * relative to cheaper work, biasing the scheduler toward cheap-but-valuable captures.
   *
   * <ul>
   *   <li>NDV or HISTOGRAM (full-column scans) → 3.0 (expensive)
   *   <li>NULL_COUNT or MIN_MAX (partial-column reads) → 2.0 (medium)
   *   <li>ROW_COUNT, FILE_COUNT, TOTAL_BYTES (footer reads) → 1.0 (cheap)
   *   <li>Empty set (kinds not specified) → 2.0 (conservative medium)
   * </ul>
   *
   * <p>When a request mixes kinds, the most expensive kind dominates (max over the set).
   */
  private static double computeCostScore(Set<StatsKind> requestedKinds) {
    if (requestedKinds.isEmpty()) {
      return 2.0; // conservative medium when kinds are unspecified
    }
    double max = 1.0;
    for (StatsKind kind : requestedKinds) {
      double kindCost =
          switch (kind) {
            case NDV, HISTOGRAM -> 3.0;
            case NULL_COUNT, MIN_MAX -> 2.0;
            default -> 1.0; // ROW_COUNT, FILE_COUNT, TOTAL_BYTES
          };
      if (kindCost > max) {
        max = kindCost;
      }
    }
    return max;
  }

  /**
   * Demand multiplier: scales the raw priority score by how frequently the planner has requested
   * stats for this table (and any column selectors) in the current demand window.
   *
   * <p>Returns a value in [{@link #demandMultiplierFloor}, 1.0]. When {@code totalDemand} is zero
   * (idle table), the floor is returned, biasing idle tables toward lower priority relative to
   * heavily-queried tables. At {@link #demandSaturation} or more hits, the multiplier saturates at
   * 1.0; additional demand is not rewarded further.
   */
  private double computeDemandMultiplier(
      SchedulerContext context, String tableKey, StatsCaptureRequest request) {
    long totalDemand = context.recentPlannerRequestCount(tableKey);
    if (!request.columnSelectors().isEmpty()) {
      for (String selector : request.columnSelectors()) {
        String normalized = selector.trim().toLowerCase(java.util.Locale.ROOT);
        totalDemand += context.recentColumnRequestCount(tableKey, normalized);
      }
    }
    if (totalDemand <= 0L) {
      return demandMultiplierFloor;
    }
    return demandMultiplierFloor
        + (1.0 - demandMultiplierFloor) * Math.min(1.0, (double) totalDemand / demandSaturation);
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
