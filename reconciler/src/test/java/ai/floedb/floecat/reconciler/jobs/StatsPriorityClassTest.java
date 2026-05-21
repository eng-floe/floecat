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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.stats.spi.JobCostHint;
import org.junit.jupiter.api.Test;

class StatsPriorityClassTest {

  // ---- order values -------------------------------------------------------

  @Test
  void orderValuesAreStrictlyAscendingFromP0toP3() {
    assertTrue(StatsPriorityClass.P0_SYNC.order < StatsPriorityClass.P1_FRESHNESS.order);
    assertTrue(StatsPriorityClass.P1_FRESHNESS.order < StatsPriorityClass.P2_REPAIR.order);
    assertTrue(StatsPriorityClass.P2_REPAIR.order < StatsPriorityClass.P3_BACKGROUND.order);
  }

  // ---- promote() ----------------------------------------------------------

  @Test
  void promoteP3ReturnsP2() {
    assertEquals(StatsPriorityClass.P2_REPAIR, StatsPriorityClass.P3_BACKGROUND.promote());
  }

  @Test
  void promoteP2ReturnsP1() {
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.P2_REPAIR.promote());
  }

  @Test
  void promoteP1ReturnsSelf_ceilingAtP1() {
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.P1_FRESHNESS.promote());
  }

  @Test
  void promoteP0ReturnsSelf_unreachableFromPolicy() {
    assertEquals(StatsPriorityClass.P0_SYNC, StatsPriorityClass.P0_SYNC.promote());
  }

  // ---- fromString() -------------------------------------------------------

  @Test
  void fromStringParsesExactNames() {
    assertEquals(StatsPriorityClass.P0_SYNC, StatsPriorityClass.fromString("P0_SYNC"));
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.fromString("P1_FRESHNESS"));
    assertEquals(StatsPriorityClass.P2_REPAIR, StatsPriorityClass.fromString("P2_REPAIR"));
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString("P3_BACKGROUND"));
  }

  @Test
  void fromStringIsCaseInsensitive() {
    assertEquals(StatsPriorityClass.P1_FRESHNESS, StatsPriorityClass.fromString("p1_freshness"));
  }

  @Test
  void fromStringReturnsP3BackgroundForNull() {
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString(null));
  }

  @Test
  void fromStringReturnsP3BackgroundForBlank() {
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString("   "));
  }

  @Test
  void fromStringReturnsP3BackgroundForUnknown() {
    assertEquals(StatsPriorityClass.P3_BACKGROUND, StatsPriorityClass.fromString("NOT_A_CLASS"));
  }

  // ---- JobCostHint.fitsIn() -----------------------------------------------

  @Test
  void cheapFitsInAnyBudget() {
    assertTrue(JobCostHint.CHEAP.fitsIn(JobCostHint.CHEAP));
    assertTrue(JobCostHint.CHEAP.fitsIn(JobCostHint.MEDIUM));
    assertTrue(JobCostHint.CHEAP.fitsIn(JobCostHint.EXPENSIVE));
  }

  @Test
  void mediumFitsInMediumAndExpensiveButNotCheap() {
    assertFalse(JobCostHint.MEDIUM.fitsIn(JobCostHint.CHEAP));
    assertTrue(JobCostHint.MEDIUM.fitsIn(JobCostHint.MEDIUM));
    assertTrue(JobCostHint.MEDIUM.fitsIn(JobCostHint.EXPENSIVE));
  }

  @Test
  void expensiveOnlyFitsInExpensive() {
    assertFalse(JobCostHint.EXPENSIVE.fitsIn(JobCostHint.CHEAP));
    assertFalse(JobCostHint.EXPENSIVE.fitsIn(JobCostHint.MEDIUM));
    assertTrue(JobCostHint.EXPENSIVE.fitsIn(JobCostHint.EXPENSIVE));
  }
}
