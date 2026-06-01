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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.spi.CoverageLevel;
import ai.floedb.floecat.stats.spi.SchedulerHealthBand;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerAdmissionPolicy.AdmissionDecision;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerContext;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import ai.floedb.floecat.telemetry.NoopObservability;
import java.util.EnumMap;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultSchedulerProfileTest {

  private DefaultSchedulerProfile profile;

  @BeforeEach
  void setUp() {
    // weights=3/2/1, maxAge=24h, demandFloor=0.15, demandSaturation=50
    profile = new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
  }

  private static StatsCaptureRequest req(String accountId, String tableId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId(accountId).setId(tableId).build(),
            1L,
            StatsTarget.getDefaultInstance())
        .build();
  }

  /** Context that returns saturation demand (multiplier=1.0) to keep score arithmetic clean. */
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
      public OptionalLong lastSuccessfulCaptureMs(String t) {
        return lastCaptureMs;
      }

      @Override
      public CoverageLevel coverageLevel(String t, long s) {
        return coverage;
      }

      @Override
      public OptionalLong snapshotDeltaRows(String t, long s) {
        return deltaRows;
      }

      @Override
      public long recentPlannerRequestCount(String t) {
        return 50L;
      } // saturation
    };
  }

  // ---------------------------------------------------------------------------
  // Scoring
  // ---------------------------------------------------------------------------

  @Test
  void coverageNoneProducesHighestScore() {
    // raw=500, mult=1.0, cost=2.0, maxRaw=600 → round(500/2*1000/600)=417
    long score =
        profile
            .assign(
                req("acct", "tbl"),
                ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()))
            .score();
    assertEquals(417L, score);
  }

  @Test
  void coverageFullProducesLowestScore() {
    // raw=200 → round(200/2*1000/600)=167
    long score =
        profile
            .assign(
                req("acct", "tbl"),
                ctx(CoverageLevel.FULL, OptionalLong.empty(), OptionalLong.empty()))
            .score();
    assertEquals(167L, score);
  }

  @Test
  void laneKeyContainsAccountAndTable() {
    PriorityAssignment a =
        profile.assign(
            req("acct1", "table-x"),
            ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    assertTrue(a.laneKey().contains("acct1"));
    assertTrue(a.laneKey().contains("table-x"));
  }

  @Test
  void assignAlwaysReturnP3Background() {
    PriorityAssignment a =
        profile.assign(
            req("acct", "tbl"),
            ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty()));
    assertEquals(StatsPriorityClass.P3_BACKGROUND, a.priorityClass());
  }

  // ---------------------------------------------------------------------------
  // Admission
  // ---------------------------------------------------------------------------

  @Test
  void p0AlwaysAdmittedInAllBands() {
    var p0 = new PriorityAssignment(StatsPriorityClass.P0_SYNC, 0L, "");
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(AdmissionDecision.ADMIT, profile.decide(p0, band));
    }
  }

  @Test
  void p1AlwaysAdmittedInAllBands() {
    var p1 = new PriorityAssignment(StatsPriorityClass.P1_FRESHNESS, 0L, "");
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(AdmissionDecision.ADMIT, profile.decide(p1, band));
    }
  }

  @Test
  void p3DeferredInOrangeAndRed() {
    var p3 = new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, 0L, "");
    assertEquals(AdmissionDecision.DEFER, profile.decide(p3, SchedulerHealthBand.ORANGE));
    assertEquals(AdmissionDecision.DEFER, profile.decide(p3, SchedulerHealthBand.RED));
  }

  @Test
  void p3AdmittedInGreenAndYellow() {
    var p3 = new PriorityAssignment(StatsPriorityClass.P3_BACKGROUND, 0L, "");
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p3, SchedulerHealthBand.GREEN));
    assertEquals(AdmissionDecision.ADMIT, profile.decide(p3, SchedulerHealthBand.YELLOW));
  }

  // ---------------------------------------------------------------------------
  // Invariant validation
  // ---------------------------------------------------------------------------

  @Test
  void invariantValidationPassesForDefaultProfile() {
    var stubCtx = ctx(CoverageLevel.NONE, OptionalLong.empty(), OptionalLong.empty());
    // Should not throw — DefaultSchedulerProfile satisfies all three invariants.
    SchedulerPolicyRegistry.validateAdmissionP0Invariant(profile, stubCtx);
    SchedulerPolicyRegistry.validatePreemptionP0Invariant(profile, stubCtx);
    SchedulerPolicyRegistry.validatePriorityP0Invariant(profile, stubCtx);
  }
}
