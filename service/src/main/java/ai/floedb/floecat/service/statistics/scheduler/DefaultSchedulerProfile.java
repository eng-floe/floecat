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

import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.CoverageLevel;
import ai.floedb.floecat.stats.spi.SchedulerHealthBand;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerAdmissionPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerContext;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPreemptionPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPriorityPolicy;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerProfile;
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

@ApplicationScoped
@SchedulerProfile(name = "default")
public class DefaultSchedulerProfile
    implements SchedulerPriorityPolicy, SchedulerAdmissionPolicy, SchedulerPreemptionPolicy {

  private final int weightCoverage;
  private final int weightDelta;
  private final int weightAge;
  private final long maxAgeMs;
  private final double demandMultiplierFloor;
  private final long demandSaturation;
  private final Observability observability;

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

  @Override
  public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
    String tableKey = request.tableId().getAccountId() + ":" + request.tableId().getId();
    long score = computeScore(request, context, tableKey);
    String laneKey = tableKey;
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

  @Override
  public Optional<String> selectVictim(
      String incomingJobId, List<RunningJobInfo> candidates, SchedulerContext context) {
    return candidates.stream()
        .filter(c -> c.priorityClass() != StatsPriorityClass.P0_SYNC)
        .max(Comparator.comparingLong(RunningJobInfo::startedAtMs))
        .map(RunningJobInfo::jobId);
  }

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
    double maxRawValue = ((double) weightCoverage + weightDelta + weightAge) * 100.0;
    return Math.min(1000L, Math.round((multiplier * rawValue / costScore) * 1000.0 / maxRawValue));
  }

  private double computeDemandMultiplier(
      SchedulerContext context, String tableKey, StatsCaptureRequest request) {
    long totalDemand = context.recentPlannerRequestCount(tableKey);
    if (!request.columnSelectors().isEmpty()) {
      for (String selector : request.columnSelectors()) {
        totalDemand +=
            context.recentColumnRequestCount(
                tableKey, selector.trim().toLowerCase(java.util.Locale.ROOT));
      }
    }
    if (totalDemand <= 0L) return demandMultiplierFloor;
    return demandMultiplierFloor
        + (1.0 - demandMultiplierFloor) * Math.min(1.0, (double) totalDemand / demandSaturation);
  }

  private static double computeCostScore(Set<StatsKind> requestedKinds) {
    if (requestedKinds.isEmpty()) return 2.0;
    double max = 1.0;
    for (StatsKind kind : requestedKinds) {
      double cost =
          switch (kind) {
            case NDV, HISTOGRAM -> 3.0;
            case NULL_COUNT, MIN_MAX -> 2.0;
            default -> 1.0;
          };
      if (cost > max) max = cost;
    }
    return max;
  }

  private long coverageScore(
      StatsCaptureRequest request, SchedulerContext context, String tableKey) {
    CoverageLevel coverage =
        request.columnSelectors().isEmpty()
            ? context.coverageLevel(tableKey, request.snapshotId())
            : CoverageLevel.NONE;
    return switch (coverage) {
      case NONE -> 100L;
      case PARTIAL -> 50L;
      case FULL -> 0L;
    };
  }

  private long deltaScore(SchedulerContext context, String tableKey, long snapshotId) {
    var opt = context.snapshotDeltaRows(tableKey, snapshotId);
    if (opt.isEmpty()) return 50L;
    long d = opt.getAsLong();
    if (d > DELTA_LARGE_ROWS) return 80L;
    if (d > DELTA_MEDIUM_ROWS) return 40L;
    return 10L;
  }

  private long ageScore(SchedulerContext context, String tableKey) {
    var opt = context.lastSuccessfulCaptureMs(tableKey);
    if (opt.isEmpty()) return 100L;
    long ageMs = System.currentTimeMillis() - opt.getAsLong();
    if (ageMs <= 0L) return 0L;
    if (ageMs >= maxAgeMs) return 100L;
    return (ageMs * 100L) / maxAgeMs;
  }
}
