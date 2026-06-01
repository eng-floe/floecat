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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchedulerStoreHelpersTest {

  // ---------------------------------------------------------------------------
  // admissionDeferMs
  // ---------------------------------------------------------------------------

  @Test
  void p0AlwaysAdmittedInAllBands() {
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          0L,
          SchedulerStoreHelpers.admissionDeferMs(StatsPriorityClass.P0_SYNC, band),
          "P0 must never be deferred, band=" + band);
    }
  }

  @Test
  void p1AlwaysAdmittedInAllBands() {
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          0L,
          SchedulerStoreHelpers.admissionDeferMs(StatsPriorityClass.P1_FRESHNESS, band),
          "P1 must never be deferred, band=" + band);
    }
  }

  @Test
  void p2OnlyDeferredInRed() {
    assertEquals(
        0L,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P2_REPAIR, SchedulerHealthBand.GREEN));
    assertEquals(
        0L,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P2_REPAIR, SchedulerHealthBand.YELLOW));
    assertEquals(
        0L,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P2_REPAIR, SchedulerHealthBand.ORANGE));
    assertEquals(
        SchedulerStoreHelpers.DEFER_DELAY_MS,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P2_REPAIR, SchedulerHealthBand.RED));
  }

  @Test
  void p3AlwaysAdmittedInGreen() {
    assertEquals(
        0L,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.GREEN));
  }

  @Test
  void p3AlwaysDeferredInOrangeAndRed() {
    assertEquals(
        SchedulerStoreHelpers.DEFER_DELAY_MS,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.ORANGE));
    assertEquals(
        SchedulerStoreHelpers.DEFER_DELAY_MS,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.RED));
  }

  @Test
  void p3PolicyDeferredForcesYellowDeferral() {
    // When policyDeferred=true, YELLOW should always defer P3 (no probabilistic gate).
    assertEquals(
        SchedulerStoreHelpers.DEFER_DELAY_MS,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.YELLOW, true));
  }

  // ---------------------------------------------------------------------------
  // agingThresholdMs
  // ---------------------------------------------------------------------------

  @Test
  void agingThresholdsArePositiveForP2AndP3() {
    assertTrue(SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P3_BACKGROUND) > 0);
    assertTrue(SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P2_REPAIR) > 0);
  }

  @Test
  void p3AgingThresholdExceedsP2Threshold() {
    // P3 jobs should age longer before promotion than P2 jobs.
    assertTrue(
        SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P3_BACKGROUND)
            > SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P2_REPAIR));
  }

  @Test
  void p0AndP1NeverAge() {
    assertEquals(
        Long.MAX_VALUE, SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P0_SYNC));
    assertEquals(
        Long.MAX_VALUE, SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P1_FRESHNESS));
  }

  // ---------------------------------------------------------------------------
  // SchedulerBandState
  // ---------------------------------------------------------------------------

  @Test
  void bandStartsAtGreen() {
    assertEquals(SchedulerHealthBand.GREEN, new SchedulerBandState().current());
  }

  @Test
  void computeAndSetEscalatesToYellowAtP3Threshold() {
    var state = new SchedulerBandState();
    Map<StatsPriorityClass, Long> depths = new EnumMap<>(StatsPriorityClass.class);
    depths.put(StatsPriorityClass.P3_BACKGROUND, SchedulerBandState.P3_YELLOW_THRESHOLD + 1);
    state.computeAndSet(depths, 0L);
    assertEquals(SchedulerHealthBand.YELLOW, state.current());
  }

  @Test
  void computeAndSetEscalatesToOrangeAtP2Threshold() {
    var state = new SchedulerBandState();
    Map<StatsPriorityClass, Long> depths = new EnumMap<>(StatsPriorityClass.class);
    depths.put(StatsPriorityClass.P2_REPAIR, SchedulerBandState.P2_ORANGE_THRESHOLD + 1);
    state.computeAndSet(depths, 0L);
    assertEquals(SchedulerHealthBand.ORANGE, state.current());
  }

  @Test
  void computeAndSetEscalatesToRedWhenP0Stalled() {
    var state = new SchedulerBandState();
    state.computeAndSet(Map.of(), SchedulerBandState.P0_RED_BUDGET_MS + 1);
    assertEquals(SchedulerHealthBand.RED, state.current());
  }

  @Test
  void computeAndSetDowngradesToGreenWhenQueuesEmpty() {
    var state = new SchedulerBandState();
    // First escalate to RED
    state.computeAndSet(Map.of(), SchedulerBandState.P0_RED_BUDGET_MS + 1);
    assertEquals(SchedulerHealthBand.RED, state.current());
    // Then clear
    state.computeAndSet(Map.of(), 0L);
    assertEquals(SchedulerHealthBand.GREEN, state.current());
  }

  @Test
  void maybeClearRedOnP0DrainClearsRedToOrangeWhenP0Empty() {
    var state = new SchedulerBandState();
    state.computeAndSet(Map.of(), SchedulerBandState.P0_RED_BUDGET_MS + 1);
    assertEquals(SchedulerHealthBand.RED, state.current());

    state.maybeClearRedOnP0Drain(Map.of()); // empty P0
    assertEquals(SchedulerHealthBand.ORANGE, state.current());
  }

  // ---------------------------------------------------------------------------
  // AgingPromotionTracker
  // ---------------------------------------------------------------------------

  @Test
  void trackerRecordsFirstEligiblePromotion() {
    var tracker = new AgingPromotionTracker();
    long now = System.currentTimeMillis();
    // Job waited longer than P3 threshold
    boolean promoted =
        tracker.recordIfEligible(
            "job1",
            SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS + 1,
            StatsPriorityClass.P3_BACKGROUND,
            now);
    assertTrue(promoted);
    assertEquals(1L, tracker.totalPromotions());
  }

  @Test
  void trackerRejectsBelowThreshold() {
    var tracker = new AgingPromotionTracker();
    boolean promoted =
        tracker.recordIfEligible(
            "job1",
            SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS - 1,
            StatsPriorityClass.P3_BACKGROUND,
            System.currentTimeMillis());
    assertEquals(false, promoted);
    assertEquals(0L, tracker.totalPromotions());
  }

  @Test
  void trackerEnforcesPerJobCooldown() {
    var tracker = new AgingPromotionTracker();
    long now = System.currentTimeMillis();
    tracker.recordIfEligible(
        "job1",
        SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS + 1,
        StatsPriorityClass.P3_BACKGROUND,
        now);
    // Immediate second attempt within cooldown should be rejected
    boolean second =
        tracker.recordIfEligible(
            "job1",
            SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS + 1,
            StatsPriorityClass.P3_BACKGROUND,
            now + 1);
    assertEquals(false, second);
    assertEquals(1L, tracker.totalPromotions()); // still only 1
  }
}
