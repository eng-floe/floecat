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
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
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
    profile = new DefaultSchedulerProfile(3, 2, 1, 86_400_000L);
  }

  // ---------------------------------------------------------------------------
  // Scoring: coverage factor
  // ---------------------------------------------------------------------------

  @Test
  void coverageNoneProducesHighestScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    // coverage=3*100=300, delta=2*50=100, age=1*100=100 → 500
    assertEquals(500L, score);
  }

  @Test
  void coveragePartialProducesMidScore() {
    long score = scoreWith(ctx(CoverageLevel.PARTIAL, OptionalLong.empty(), OptionalLong.empty()));
    // coverage=3*50=150, delta=2*50=100, age=1*100=100 → 350
    assertEquals(350L, score);
  }

  @Test
  void coverageFullProducesZeroCoverageContribution() {
    long score = scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty()));
    // coverage=3*0=0, delta=2*50=100, age=1*100=100 → 200
    assertEquals(200L, score);
  }

  // ---------------------------------------------------------------------------
  // Scoring: delta factor
  // ---------------------------------------------------------------------------

  @Test
  void largeDeltaProducesHighDeltaScore() {
    long score =
        scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(2_000_000L), OptionalLong.empty()));
    // coverage=300, delta=2*80=160, age=100 → 560
    assertEquals(560L, score);
  }

  @Test
  void mediumDeltaProducesMediumDeltaScore() {
    long score =
        scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(500_000L), OptionalLong.empty()));
    // coverage=300, delta=2*40=80, age=100 → 480
    assertEquals(480L, score);
  }

  @Test
  void smallDeltaProducesLowDeltaScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.of(1_000L), OptionalLong.empty()));
    // coverage=300, delta=2*10=20, age=100 → 420
    assertEquals(420L, score);
  }

  @Test
  void unknownDeltaProducesMidDeltaScore() {
    long score = scoreWith(ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    // coverage=300, delta=2*50=100 (unknown→50), age=100 → 500
    assertEquals(500L, score);
  }

  // ---------------------------------------------------------------------------
  // Scoring: age factor
  // ---------------------------------------------------------------------------

  @Test
  void neverCapturedProducesMaxAgeScore() {
    // lastCaptureMs absent → age=100; delta unknown → 50
    long score = scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty()));
    // coverage=3*0=0, delta=2*50=100, age=1*100=100 → 200
    assertEquals(200L, score);
  }

  @Test
  void recentCaptureProducesLowAgeScore() {
    long lastCaptureMs = System.currentTimeMillis();
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // coverage=0, delta=100 (unknown), age≈0 → ≈100; allow small clock skew
    assertTrue(score < 110L, "Recent capture should yield low age contribution, got: " + score);
  }

  @Test
  void halfAgedCaptureProducesMidAgeScore() {
    long halfAgeMs = 43_200_000L; // 12 h (half of 24 h maxAge)
    long lastCaptureMs = System.currentTimeMillis() - halfAgeMs;
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // coverage=0, delta=100 (unknown), age≈50 → ≈150; allow ±2 for clock drift
    assertTrue(score >= 148L && score <= 152L, "Half-age capture should yield ~150, got: " + score);
  }

  @Test
  void overdueCaptureSaturatesAgeScoreAt100() {
    long overdueMs = 172_800_000L; // 48 h (2× maxAge)
    long lastCaptureMs = System.currentTimeMillis() - overdueMs;
    long score =
        scoreWith(ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.of(lastCaptureMs)));
    // coverage=0, delta=100 (unknown), age=100 (clamped) → 200
    assertEquals(200L, score);
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
  // Helpers
  // ---------------------------------------------------------------------------

  private long scoreWith(SchedulerContext context) {
    return profile.assign(req("acct", "tbl"), context).score();
  }

  /** Stub context with fixed coverage, delta, and lastCapture values. Band is always GREEN. */
  private static SchedulerContext ctx(
      CoverageLevel coverage, OptionalLong deltaRows, OptionalLong lastCaptureMs) {
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
}
