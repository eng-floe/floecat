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
import org.junit.jupiter.api.Test;

/**
 * Verifies the correctness of {@link SchedulerStoreHelpers} static methods for all (priority class,
 * health band) combinations.
 *
 * <p>These helpers are shared by all job-store implementations; a regression here would silently
 * diverge admission and aging behavior across stores.
 */
class SchedulerStoreHelpersTest {

  // ---------------------------------------------------------------------------
  // admissionDeferMs — P0_SYNC and P1_FRESHNESS always admit
  // ---------------------------------------------------------------------------

  @Test
  void p0SyncAlwaysAdmittedInAllBands() {
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          0L,
          SchedulerStoreHelpers.admissionDeferMs(StatsPriorityClass.P0_SYNC, band),
          "P0_SYNC must always return 0 in band " + band);
    }
  }

  @Test
  void p1FreshnessAlwaysAdmittedInAllBands() {
    for (SchedulerHealthBand band : SchedulerHealthBand.values()) {
      assertEquals(
          0L,
          SchedulerStoreHelpers.admissionDeferMs(StatsPriorityClass.P1_FRESHNESS, band),
          "P1_FRESHNESS must always return 0 in band " + band);
    }
  }

  // ---------------------------------------------------------------------------
  // admissionDeferMs — P2_REPAIR
  // ---------------------------------------------------------------------------

  @Test
  void p2RepairAdmittedUnderGreenYellowOrange() {
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
  }

  @Test
  void p2RepairDeferredUnderRed() {
    assertEquals(
        SchedulerStoreHelpers.DEFER_DELAY_MS,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P2_REPAIR, SchedulerHealthBand.RED));
  }

  // ---------------------------------------------------------------------------
  // admissionDeferMs — P3_BACKGROUND
  // ---------------------------------------------------------------------------

  @Test
  void p3BackgroundAdmittedUnderGreen() {
    assertEquals(
        0L,
        SchedulerStoreHelpers.admissionDeferMs(
            StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.GREEN));
  }

  @Test
  void p3BackgroundDeferredUnderOrangeAndRed() {
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
  void p3BackgroundYellowIsProbabilistic() {
    // Under YELLOW the result is 50/50 — run enough samples to observe both outcomes with high
    // probability (false-negative rate ≈ 2^-20 for 20 samples of a fair coin).
    boolean sawZero = false;
    boolean sawDelay = false;
    for (int i = 0; i < 200 && !(sawZero && sawDelay); i++) {
      long result =
          SchedulerStoreHelpers.admissionDeferMs(
              StatsPriorityClass.P3_BACKGROUND, SchedulerHealthBand.YELLOW);
      if (result == 0L) sawZero = true;
      else if (result == SchedulerStoreHelpers.DEFER_DELAY_MS) sawDelay = true;
    }
    assertTrue(sawZero, "Expected YELLOW P3 to occasionally admit (return 0)");
    assertTrue(sawDelay, "Expected YELLOW P3 to occasionally defer (return DEFER_DELAY_MS)");
  }

  // ---------------------------------------------------------------------------
  // agingThresholdMs
  // ---------------------------------------------------------------------------

  @Test
  void agingThresholdsAreStrictlyDecreasingByUrgency() {
    long p3 = SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P3_BACKGROUND);
    long p2 = SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P2_REPAIR);
    long p1 = SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P1_FRESHNESS);
    long p0 = SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P0_SYNC);

    assertTrue(p3 > p2, "P3 threshold must be higher than P2 (P3 waits longer before aging)");
    assertTrue(p1 > p3, "P1 threshold should be effectively infinite (never ages)");
    assertEquals(Long.MAX_VALUE, p0, "P0_SYNC must never age");
    assertEquals(Long.MAX_VALUE, p1, "P1_FRESHNESS must never age");
  }

  @Test
  void agingThresholdsMatchPublishedConstants() {
    assertEquals(
        SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS,
        SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P3_BACKGROUND));
    assertEquals(
        SchedulerStoreHelpers.P2_AGING_THRESHOLD_MS,
        SchedulerStoreHelpers.agingThresholdMs(StatsPriorityClass.P2_REPAIR));
  }
}
