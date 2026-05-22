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

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchedulerBandStateTest {

  @Test
  void computeAndSetEscalatesToYellowFromFullScan() {
    SchedulerBandState state = new SchedulerBandState();
    SchedulerHealthBand band =
        state.computeAndSet(depths(0L, 0L, SchedulerBandState.P3_YELLOW_THRESHOLD + 1L), 0L);
    assertEquals(SchedulerHealthBand.YELLOW, band);
    assertEquals(SchedulerHealthBand.YELLOW, state.current());
  }

  @Test
  void computeAndSetEscalatesToRedFromFullScan() {
    SchedulerBandState state = new SchedulerBandState();
    SchedulerHealthBand band =
        state.computeAndSet(depths(1L, 0L, 0L), SchedulerBandState.P0_RED_BUDGET_MS + 1L);
    assertEquals(SchedulerHealthBand.RED, band);
    assertEquals(SchedulerHealthBand.RED, state.current());
  }

  @Test
  void computeAndSetDowngradesFromRedToGreen() {
    SchedulerBandState state = new SchedulerBandState();
    state.setForTest(SchedulerHealthBand.RED);

    SchedulerHealthBand band = state.computeAndSet(depths(0L, 0L, 0L), 0L);
    assertEquals(SchedulerHealthBand.GREEN, band);
    assertEquals(SchedulerHealthBand.GREEN, state.current());
  }

  private static Map<StatsPriorityClass, Long> depths(long p0, long p2, long p3) {
    EnumMap<StatsPriorityClass, Long> out = new EnumMap<>(StatsPriorityClass.class);
    out.put(StatsPriorityClass.P0_SYNC, p0);
    out.put(StatsPriorityClass.P2_REPAIR, p2);
    out.put(StatsPriorityClass.P3_BACKGROUND, p3);
    return out;
  }
}
