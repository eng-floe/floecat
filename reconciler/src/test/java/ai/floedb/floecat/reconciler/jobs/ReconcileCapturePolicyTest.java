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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.stats.spi.JobCostHint;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcileCapturePolicyTest {

  // ---------------------------------------------------------------------------
  // maxCost defaults
  // ---------------------------------------------------------------------------

  @Test
  void emptyPolicyHasExpensiveMaxCost() {
    assertEquals(JobCostHint.EXPENSIVE, ReconcileCapturePolicy.empty().maxCost());
  }

  @Test
  void twoArgOfDefaultsToExpensive() {
    var policy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS));
    assertEquals(JobCostHint.EXPENSIVE, policy.maxCost());
  }

  @Test
  void threeArgOfPreservesExplicitMaxCost() {
    var policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS), JobCostHint.CHEAP);
    assertEquals(JobCostHint.CHEAP, policy.maxCost());
  }

  @Test
  void threeArgOfMediumPreservesMedium() {
    var policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS), JobCostHint.MEDIUM);
    assertEquals(JobCostHint.MEDIUM, policy.maxCost());
  }

  // ---------------------------------------------------------------------------
  // empty() sentinel reuse
  // ---------------------------------------------------------------------------

  @Test
  void emptyColumnsAndOutputsWithExpensiveReturnsEmptySentinel() {
    // Calling the 3-arg factory with empty content + EXPENSIVE should return the singleton.
    assertSame(
        ReconcileCapturePolicy.empty(),
        ReconcileCapturePolicy.of(List.of(), Set.of(), JobCostHint.EXPENSIVE));
  }

  @Test
  void emptyColumnsAndOutputsWithNullMaxCostReturnsEmptySentinel() {
    // null maxCost normalises to EXPENSIVE, so the empty sentinel is returned.
    assertSame(
        ReconcileCapturePolicy.empty(), ReconcileCapturePolicy.of(List.of(), Set.of(), null));
  }

  @Test
  void nonExpensiveMaxCostWithEmptyContentIsNotEmptySentinel() {
    // CHEAP maxCost on an otherwise empty policy must not collapse to the EXPENSIVE sentinel,
    // because the two carry different cost budgets.
    var policy = ReconcileCapturePolicy.of(List.of(), Set.of(), JobCostHint.CHEAP);
    // isEmpty() reflects columns/outputs only, not maxCost — so this is still "empty" in that
    // sense.
    assertTrue(policy.isEmpty());
    // But it is a distinct instance from the EXPENSIVE sentinel.
    assertNotSame(
        ReconcileCapturePolicy.empty(), policy, "must not be the EXPENSIVE empty sentinel");
    assertEquals(JobCostHint.CHEAP, policy.maxCost());
  }

  // ---------------------------------------------------------------------------
  // fitsIn interaction
  // ---------------------------------------------------------------------------

  @Test
  void cheapCostFitsInCheapBudget() {
    assertTrue(JobCostHint.CHEAP.fitsIn(JobCostHint.CHEAP));
  }

  @Test
  void mediumCostDoesNotFitInCheapBudget() {
    assertFalse(JobCostHint.MEDIUM.fitsIn(JobCostHint.CHEAP));
  }

  @Test
  void expensiveCostDoesNotFitInMediumBudget() {
    assertFalse(JobCostHint.EXPENSIVE.fitsIn(JobCostHint.MEDIUM));
  }

  @Test
  void expensiveCostFitsInExpensiveBudget() {
    assertTrue(JobCostHint.EXPENSIVE.fitsIn(JobCostHint.EXPENSIVE));
  }

  // ---------------------------------------------------------------------------
  // Existing behaviour preserved: outputs and columns still work correctly
  // ---------------------------------------------------------------------------

  @Test
  void outputsAndColumnsUnaffectedByMaxCost() {
    var col = new ReconcileCapturePolicy.Column("col1", true, false);
    var policy =
        ReconcileCapturePolicy.of(
            List.of(col), Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS), JobCostHint.MEDIUM);
    assertEquals(Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS), policy.outputs());
    assertEquals(List.of(col), policy.columns());
    assertTrue(policy.requestsStats());
    assertFalse(policy.requestsIndexes());
  }
}
