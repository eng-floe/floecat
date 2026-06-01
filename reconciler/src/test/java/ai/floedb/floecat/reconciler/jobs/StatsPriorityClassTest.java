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

class StatsPriorityClassTest {

  @Test
  void orderIsMonotonicallyIncreasingLowToHigh() {
    // P0 < P1 < P2 < P3 by order value
    assertEquals(0, StatsPriorityClass.P0_SYNC.order);
    assertEquals(1, StatsPriorityClass.P1_FRESHNESS.order);
    assertEquals(2, StatsPriorityClass.P2_REPAIR.order);
    assertEquals(3, StatsPriorityClass.P3_BACKGROUND.order);
  }

  @Test
  void promoteFromP3AdvancesToP2() {
    assertEquals(StatsPriorityClass.P2_REPAIR, StatsPriorityClass.P3_BACKGROUND.promote());
  }

  @Test
  void promoteFromP2AdvancesToP1() {
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.P2_REPAIR.promote());
  }

  @Test
  void promoteFromP1StaysP1() {
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.P1_FRESHNESS.promote());
  }

  @Test
  void promoteFromP0StaysP0() {
    assertEquals(StatsPriorityClass.P0_SYNC, StatsPriorityClass.P0_SYNC.promote());
  }

  @Test
  void fromStringParsesKnownValues() {
    assertEquals(StatsPriorityClass.P0_SYNC, StatsPriorityClass.fromString("P0_SYNC"));
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.fromString("P1_FRESHNESS"));
    assertEquals(
        StatsPriorityClass.P2_REPAIR,
        StatsPriorityClass.fromString("p2_repair")); // case-insensitive
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString("P3_BACKGROUND"));
  }

  @Test
  void fromStringDefaultsToP3ForNullOrUnknown() {
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString(null));
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString(""));
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString("UNKNOWN_CLASS"));
  }
}
