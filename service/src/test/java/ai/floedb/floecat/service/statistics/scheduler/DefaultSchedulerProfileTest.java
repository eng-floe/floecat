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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerAdmissionPolicy.AdmissionDecision;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPreemptionPolicy.RunningJobInfo;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.NoopObservability;
import ai.floedb.floecat.telemetry.Tag;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultSchedulerProfile}.
 *
 * <p>Uses a stub {@link SchedulerContext} to exercise each scoring factor in isolation and verify
 * admission-control and preemption behaviour.
 */
class DefaultSchedulerProfileTest {

  // Default weights: coverage=3, delta=2, age=1; max-age=24h
  private DefaultSchedulerProfile profile;

  @BeforeEach
  void setUp() {
    profile = new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
  }

  // ---------------------------------------------------------------------------
  // Scoring: coverage factor
  // ---------------------------------------------------------------------------

  @Test
  void coverageNoneProducesHighestScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    // raw=3*100+2*50+1*100=500; normalised=round(500*500/600)=round(416.67)=417
    assertEquals(417L, score);
  }

  @Test
  void coveragePartialProducesMidScore() {
    long score = scoreWith(ctx(CoverageLevel.PARTIAL, OptionalLong.empty(), OptionalLong.empty()));
    // raw=3*50+2*50+1*100=350; normalised=round(350*500/600)=round(291.67)=292
    assertEquals(292L, score);
  }

  @Test
  void coverageFullProducesZeroCoverageContribution() {
    long score = scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty()));
    // raw=3*0+2*50+1*100=200; normalised=round(200*500/600)=round(166.67)=167
    assertEquals(167L, score);
  }

  // ---------------------------------------------------------------------------
  // Scoring: delta factor
  // ---------------------------------------------------------------------------

  @Test
  void largeDeltaProducesHighDeltaScore() {
    long score =
        scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(2_000_000L), OptionalLong.empty()));
    // raw=300+160+100=560; normalised=round(560*500/600)=round(466.67)=467
    assertEquals(467L, score);
  }

  @Test
  void mediumDeltaProducesMediumDeltaScore() {
    long score =
        scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(500_000L), OptionalLong.empty()));
    // raw=300+80+100=480; normalised=round(480*500/600)=round(400.0)=400
    assertEquals(400L, score);
  }

  @Test
  void smallDeltaProducesLowDeltaScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(1_000L), OptionalLong.empty()));
    // raw=300+20+100=420; normalised=round(420*500/600)=round(350.0)=350
    assertEquals(350L, score);
  }

  @Test
  void unknownDeltaProducesMidDeltaScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    // raw=300+100+100=500 (unknown delta→50); normalised=417
    assertEquals(417L, score);
  }

  // ---------------------------------------------------------------------------
  // Scoring: age factor
  // ---------------------------------------------------------------------------

  @Test
  void neverCapturedProducesMaxAgeScore() {
    // lastCaptureMs absent → age=100; delta unknown → 50
    long score = scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty()));
    // raw=0+100+100=200; normalised=round(200*500/600)=167
    assertEquals(167L, score);
  }

  @Test
  void recentCaptureProducesLowAgeScore() {
    long lastCaptureMs = System.currentTimeMillis();
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // raw=0+100+≈0=≈100; normalised≈round(100*500/600)=round(83.33)=83; allow small clock skew
    assertTrue(score < 92L, "Recent capture should yield low age contribution, got: " + score);
  }

  @Test
  void halfAgedCaptureProducesMidAgeScore() {
    long halfAgeMs = 43_200_000L; // 12 h (half of 24 h maxAge)
    long lastCaptureMs = System.currentTimeMillis() - halfAgeMs;
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // raw=0+100+≈50=≈150; normalised=round(150*500/600)=round(125.0)=125; allow ±2 for clock drift
    assertTrue(score >= 123L && score <= 127L, "Half-age capture should yield ~125, got: " + score);
  }

  @Test
  void overdueCaptureSaturatesAgeScoreAt100() {
    long overdueMs = 172_800_000L; // 48 h (2× maxAge)
    long lastCaptureMs = System.currentTimeMillis() - overdueMs;
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // raw=0+100+100=200 (age clamped); normalised=167
    assertEquals(167L, score);
  }

  // ---------------------------------------------------------------------------
  // Admission control
  // ---------------------------------------------------------------------------

  @Test
  void p0AlwaysAdmittedInAllBands() {
    var p0 = new PriorityAssignment(StatsPriorityClass.P0_SYNC, 0L, "");
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          AdmissionDecision.ADMIT, profile.decide(p0, band), "P0 must be ADMIT in band " + band);
    }
  }

  @Test
  void p1AlwaysAdmittedInAllBands() {
    var p1 = new PriorityAssignment(StatsPriorityClass.P1_FRESHNESS, 0L, "");
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          AdmissionDecision.ADMIT, profile.decide(p1, band), "P1 must be ADMIT in band " + band);
    }
  }

  @Test
  void p2AdmittedInGreenYellowOrange() {
    var p2 = new PriorityAssignment(StatsPriorityClass.P2_REPAIR, 0L, "");
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p2, SchedulerHealthBand.GREEN));
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p2, SchedulerHealthBand.YELLOW));
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p2, SchedulerHealthBand.ORANGE));
  }

  @Test
  void p2DeferredInRed() {
    var p2 = new PriorityAssignment(StatsPriorityClass.P2_REPAIR, 0L, "");
    assertEquals(AdmissionDecision.DEFER, profile.decide(p2, SchedulerHealthBand.RED));
  }

  @Test
  void p3AdmittedInGreenAndYellow() {
    var p3 = new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, 0L, "");
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p3, SchedulerHealthBand.GREEN));
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p3, SchedulerHealthBand.YELLOW));
  }

  @Test
  void p3DeferredInOrangeAndRed() {
    var p3 = new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, 0L, "");
    assertEquals(AdmissionDecision.DEFER, profile.decide(p3, SchedulerHealthBand.ORANGE));
    assertEquals(AdmissionDecision.DEFER, profile.decide(p3, SchedulerHealthBand.RED));
  }

  // ---------------------------------------------------------------------------
  // Preemption victim selection
  // ---------------------------------------------------------------------------

  @Test
  void emptyListReturnsEmpty() {
    assertTrue(
        profile.selectVictim("incoming", List.of(), bandCtx(SchedulerHealthBand.RED)).isEmpty());
  }

  @Test
  void p0CandidateNeverSelected() {
    var p0 = new RunningJobInfo("p0-job", StatsPriorityClass.P0_SYNC, 1000L, 0, 10);
    assertTrue(
        profile.selectVictim("incoming", List.of(p0), bandCtx(SchedulerHealthBand.RED)).isEmpty(),
        "P0 must never be selected as preemption victim");
  }

  @Test
  void mostRecentlyStartedCandidateSelected() {
    // Most recently started = highest startedAtMs
    var older = new RunningJobInfo("older-job", StatsPriorityClass.P3_BACKGROUND, 1000L, 5, 10);
    var newer = new RunningJobInfo("newer-job", StatsPriorityClass.P3_BACKGROUND, 5000L, 1, 10);
    var result =
        profile.selectVictim("incoming", List.of(older, newer), bandCtx(SchedulerHealthBand.RED));
    assertTrue(result.isPresent());
    assertEquals(
        "newer-job",
        result.get(),
        "Most recently started (highest startedAtMs) should be selected");
  }

  @Test
  void mixedP0AndP3PicksP3Candidate() {
    var p0 = new RunningJobInfo("p0-job", StatsPriorityClass.P0_SYNC, 9000L, 0, 1);
    var p3 = new RunningJobInfo("p3-job", StatsPriorityClass.P3_BACKGROUND, 8000L, 2, 10);
    var result =
        profile.selectVictim("incoming", List.of(p0, p3), bandCtx(SchedulerHealthBand.RED));
    assertTrue(result.isPresent());
    assertEquals("p3-job", result.get(), "Should select P3 candidate, not P0");
  }

  // ---------------------------------------------------------------------------
  // Column selector: bias toward capturing
  // ---------------------------------------------------------------------------

  @Test
  void columnSelectorBiasesCoverageToNoneRegardlessOfContextCoverage() {
    // When column selectors are present, coverage is treated as NONE for scoring even if the
    // SchedulerContext reports FULL — ensuring targeted column requests are not deprioritised.
    var ctxFull = ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty());
    long scoreWithoutSelector = scoreWith(ctxFull);

    // Build a request with a column selector; same context still reports FULL coverage.
    var requestWithSelector =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                StatsTarget.getDefaultInstance())
            .columnSelectors(Set.of("col_a"))
            .build();
    long scoreWithSelector = profile.assign(requestWithSelector, ctxFull).score();

    assertTrue(
        scoreWithSelector > scoreWithoutSelector,
        "Column-scoped request should produce a higher score (NONE coverage) than a FULL-coverage"
            + " request without selectors. Without: "
            + scoreWithoutSelector
            + ", with: "
            + scoreWithSelector);
  }

  // ---------------------------------------------------------------------------
  // Demand multiplier
  // ---------------------------------------------------------------------------

  @Test
  void zeroDemandAppliesFloorMultiplier() {
    // floor=0.15, saturation=50; demand=0 → multiplier=0.15
    // raw=500, costScore=2.0, maxRaw=600 → normalised=round(0.15*500/2.0*1000/600)=round(62.5)=63
    var profile15 =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
    long score =
        profile15
            .assign(
                req("acct", "tbl"),
                ctxWithDemand(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty(), 0L))
            .score();
    assertEquals(63L, score, "demand=0 should apply floor multiplier 0.15, normalised to 63");
  }

  @Test
  void saturationDemandGivesMultiplierOne() {
    // demand=50 (at saturation) → multiplier=1.0; normalised=417
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    assertEquals(417L, score, "demand at saturation should give multiplier=1.0, normalised to 417");
  }

  @Test
  void halfSaturationDemandGivesMidMultiplier() {
    // floor=0.15, saturation=50; demand=25 → multiplier=0.15 + 0.85*0.5 = 0.575
    // raw=500, costScore=2.0, maxRaw=600 →
    // normalised=round(0.575*500/2.0*1000/600)=round(239.58)=240
    var profile15 =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
    long score =
        profile15
            .assign(
                req("acct", "tbl"),
                ctxWithDemand(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty(), 25L))
            .score();
    assertEquals(
        240L, score, "demand=25 (half saturation) should give multiplier=0.575, normalised to 240");
  }

  @Test
  void demandAboveSaturationDoesNotExceedMultiplierOne() {
    // demand=200 (4× saturation) → multiplier still capped at 1.0; normalised=417
    var profile15 =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
    long score =
        profile15
            .assign(
                req("acct", "tbl"),
                ctxWithDemand(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty(), 200L))
            .score();
    assertEquals(
        417L, score, "demand above saturation must not exceed multiplier=1.0, normalised to 417");
  }

  @Test
  void higherDemandProducesHigherScoreThanLowerDemand() {
    var lowCtx = ctxWithDemand(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty(), 5L);
    var highCtx =
        ctxWithDemand(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty(), 40L);
    long lowScore = profile.assign(req("acct", "tbl"), lowCtx).score();
    long highScore = profile.assign(req("acct", "tbl"), highCtx).score();
    assertTrue(highScore > lowScore, "higher demand should produce higher score");
  }

  // ---------------------------------------------------------------------------
  // Cost scoring
  // ---------------------------------------------------------------------------

  @Test
  void expensiveKindsReduceScoreRelativeToCheapKinds() {
    // Same coverage/delta/age/demand; NDV is expensive (3.0) vs ROW_COUNT is cheap (1.0).
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    var expensiveReq =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.NDV))
            .build();
    var cheapReq =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .build();
    long expensiveScore = profile.assign(expensiveReq, ctx).score();
    long cheapScore = profile.assign(cheapReq, ctx).score();
    assertTrue(
        cheapScore > expensiveScore,
        "cheap kinds should produce higher score than expensive kinds");
  }

  @Test
  void ndvOrHistogramCostIsThree() {
    // costScore=3.0 for NDV: raw=500, multiplier=1.0, maxRaw=600
    // normalised=round(500/3.0*1000/600)=round(277.78)=278
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    var reqNdv =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.NDV))
            .build();
    assertEquals(278L, profile.assign(reqNdv, ctx).score());

    var reqHist =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.HISTOGRAM))
            .build();
    assertEquals(278L, profile.assign(reqHist, ctx).score());
  }

  @Test
  void rowCountCostIsOne() {
    // costScore=1.0 for ROW_COUNT: raw=500, multiplier=1.0, maxRaw=600
    // normalised=round(500/1.0*1000/600)=round(833.33)=833
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    var reqRowCount =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .build();
    assertEquals(833L, profile.assign(reqRowCount, ctx).score());
  }

  @Test
  void mixedKindsUsesMostExpensiveCost() {
    // NDV (3.0) mixed with ROW_COUNT (1.0) → costScore=3.0 → normalised=278
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    var reqMixed =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl").build(),
                1L,
                ai.floedb.floecat.catalog.rpc.StatsTarget.getDefaultInstance())
            .requestedKinds(Set.of(StatsKind.NDV, StatsKind.ROW_COUNT))
            .build();
    assertEquals(278L, profile.assign(reqMixed, ctx).score());
  }

  @Test
  void emptyKindsFallsBackToMediumCost() {
    // Empty requestedKinds → costScore=2.0 → normalised=417
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    assertEquals(417L, score, "empty kinds should use medium cost 2.0");
  }

  // ---------------------------------------------------------------------------
  // Lane key derivation
  // ---------------------------------------------------------------------------

  @Test
  void assignProducesLaneKeyContainingAccountAndTable() {
    var request = req("account-1", "table-abc");
    var assignment =
        profile.assign(
            request, ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    assertNotNull(assignment.laneKey());
    assertTrue(
        assignment.laneKey().contains("account-1"),
        "Lane key must contain accountId, got: " + assignment.laneKey());
    assertTrue(
        assignment.laneKey().contains("table-abc"),
        "Lane key must contain tableId, got: " + assignment.laneKey());
  }

  @Test
  void differentTablesProduceDifferentLaneKeys() {
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    assertNotEquals(
        profile.assign(req("acct", "table-1"), ctx).laneKey(),
        profile.assign(req("acct", "table-2"), ctx).laneKey());
  }

  // ---------------------------------------------------------------------------
  // Scoring summary metric
  // ---------------------------------------------------------------------------

  @Test
  void assignRecordsScoringScoreDistSummary() {
    List<Double> capturedValues = new ArrayList<>();
    // Recording observability that captures summary() calls for SCORING_SCORE_DIST.
    var recordingObs =
        new CapturingSummaryObservability(
            capturedValues, ServiceMetrics.Reconcile.SCORING_SCORE_DIST);
    var recordingProfile =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, recordingObs);
    var ctx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());

    recordingProfile.assign(req("acct", "tbl"), ctx);

    assertEquals(
        1, capturedValues.size(), "assign() should record exactly one summary observation");
    // raw=300+100+100=500, costScore=2.0 (empty kinds), maxRaw=600, multiplier=1.0 → normalised=417
    assertEquals(417.0, capturedValues.get(0), "Recorded score should equal computed score");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private long scoreWith(SchedulerContext context) {
    return profile.assign(req("acct", "tbl"), context).score();
  }

  /**
   * Stub context with fixed coverage, delta, and lastCapture values. Band is always GREEN.
   *
   * <p>Returns demand at saturation (50) so the demand multiplier is 1.0, keeping existing score
   * assertions numerically clean. Use {@link #ctxWithDemand} when testing the multiplier directly.
   */
  private static SchedulerContext ctx(
      CoverageLevel coverage, OptionalLong deltaRows, OptionalLong lastCaptureMs) {
    return ctxWithDemand(coverage, deltaRows, lastCaptureMs, 50L);
  }

  /** Stub context with configurable demand count, in addition to coverage/delta/lastCapture. */
  private static SchedulerContext ctxWithDemand(
      CoverageLevel coverage, OptionalLong deltaRows, OptionalLong lastCaptureMs, long demand) {
    return new SchedulerContext() {
      @Override
      public SchedulerHealthBand currentBand() {
        return SchedulerHealthBand.GREEN;
      }

      @Override
      public Map<StatsPriorityClass, Long> queueDepthByClass() {
        return new EnumMap<>(StatsPriorityClass.class);
      }

      @Override
      public OptionalLong lastSuccessfulCaptureMs(String tableId) {
        return lastCaptureMs;
      }

      @Override
      public CoverageLevel coverageLevel(String tableId, long snapshotId) {
        return coverage;
      }

      @Override
      public OptionalLong snapshotDeltaRows(String tableId, long snapshotId) {
        return deltaRows;
      }

      @Override
      public long recentPlannerRequestCount(String tableId) {
        return demand;
      }
    };
  }

  /** Stub context with a fixed health band and default (conservative) coverage values. */
  private static SchedulerContext bandCtx(SchedulerHealthBand band) {
    return new SchedulerContext() {
      @Override
      public SchedulerHealthBand currentBand() {
        return band;
      }

      @Override
      public Map<StatsPriorityClass, Long> queueDepthByClass() {
        return Map.of();
      }

      @Override
      public OptionalLong lastSuccessfulCaptureMs(String tableId) {
        return OptionalLong.empty();
      }

      @Override
      public CoverageLevel coverageLevel(String tableId, long snapshotId) {
        return CoverageLevel.NONE;
      }

      @Override
      public OptionalLong snapshotDeltaRows(String tableId, long snapshotId) {
        return OptionalLong.empty();
      }
    };
  }

  private static StatsCaptureRequest req(String accountId, String tableId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId(accountId).setId(tableId).build(),
            1L,
            StatsTarget.getDefaultInstance())
        .build();
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Minimal {@link ai.floedb.floecat.telemetry.Observability} stub that records {@code summary()}
   * calls for a specific metric into a provided list. All other methods are no-ops.
   */
  private static final class CapturingSummaryObservability
      implements ai.floedb.floecat.telemetry.Observability {

    private static final ai.floedb.floecat.telemetry.ObservationScope NOOP_SCOPE =
        new ai.floedb.floecat.telemetry.ObservationScope() {
          @Override
          public void success() {}

          @Override
          public void error(Throwable t) {}

          @Override
          public void retry() {}
        };

    private final List<Double> captured;
    private final MetricId target;

    CapturingSummaryObservability(List<Double> captured, MetricId target) {
      this.captured = captured;
      this.target = target;
    }

    @Override
    public void counter(MetricId metric, double amount, Tag... tags) {}

    @Override
    public void summary(MetricId metric, double value, Tag... tags) {
      if (target.equals(metric)) {
        captured.add(value);
      }
    }

    @Override
    public void timer(MetricId metric, Duration duration, Tag... tags) {}

    @Override
    public <T extends Number> void gauge(
        MetricId metric, Supplier<T> supplier, String description, Tag... tags) {}

    @Override
    public ai.floedb.floecat.telemetry.ObservationScope observe(
        Category category, String component, String operation, Tag... tags) {
      return NOOP_SCOPE;
    }
  }
}
