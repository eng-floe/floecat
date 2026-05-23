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

package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SchedulerHealthBandTest {

  // ---------------------------------------------------------------------------
  // escalate() transitions
  // ---------------------------------------------------------------------------

  @Test
  void escalateGreenToYellow() {
    assertEquals(SchedulerHealthBand.YELLOW, SchedulerHealthBand.GREEN.escalate());
  }

  @Test
  void escalateYellowToOrange() {
    assertEquals(SchedulerHealthBand.ORANGE, SchedulerHealthBand.YELLOW.escalate());
  }

  @Test
  void escalateOrangeToRed() {
    assertEquals(SchedulerHealthBand.RED, SchedulerHealthBand.ORANGE.escalate());
  }

  @Test
  void escalateRedStaysRed() {
    assertEquals(SchedulerHealthBand.RED, SchedulerHealthBand.RED.escalate());
  }

  // ---------------------------------------------------------------------------
  // relax() transitions
  // ---------------------------------------------------------------------------

  @Test
  void relaxGreenStaysGreen() {
    assertEquals(SchedulerHealthBand.GREEN, SchedulerHealthBand.GREEN.relax());
  }

  @Test
  void relaxYellowToGreen() {
    assertEquals(SchedulerHealthBand.GREEN, SchedulerHealthBand.YELLOW.relax());
  }

  @Test
  void relaxOrangeToYellow() {
    assertEquals(SchedulerHealthBand.YELLOW, SchedulerHealthBand.ORANGE.relax());
  }

  @Test
  void relaxRedToOrange() {
    assertEquals(SchedulerHealthBand.ORANGE, SchedulerHealthBand.RED.relax());
  }

  // ---------------------------------------------------------------------------
  // Round-trip: escalate then relax returns the original band
  // ---------------------------------------------------------------------------

  @Test
  void escalateThenRelaxReturnsSameBandForGreen() {
    assertEquals(SchedulerHealthBand.GREEN, SchedulerHealthBand.GREEN.escalate().relax());
  }

  @Test
  void escalateThenRelaxReturnsSameBandForYellow() {
    assertEquals(SchedulerHealthBand.YELLOW, SchedulerHealthBand.YELLOW.escalate().relax());
  }

  @Test
  void escalateThenRelaxReturnsSameBandForOrange() {
    assertEquals(SchedulerHealthBand.ORANGE, SchedulerHealthBand.ORANGE.escalate().relax());
  }

  // ---------------------------------------------------------------------------
  // Boundary: double-escalate from ORANGE stays RED
  // ---------------------------------------------------------------------------

  @Test
  void doubleEscalateFromOrangeStaysRed() {
    assertEquals(SchedulerHealthBand.RED, SchedulerHealthBand.ORANGE.escalate().escalate());
  }

  // ---------------------------------------------------------------------------
  // Boundary: double-relax from YELLOW stays GREEN
  // ---------------------------------------------------------------------------

  @Test
  void doubleRelaxFromYellowStaysGreen() {
    assertEquals(SchedulerHealthBand.GREEN, SchedulerHealthBand.YELLOW.relax().relax());
  }
}
