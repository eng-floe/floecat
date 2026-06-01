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

package ai.floedb.floecat.service.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.jobs.JobCostHint;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcileCapturePolicyTest {

  @Test
  void emptyPolicyHasExpensiveDefaultMaxCost() {
    assertEquals(JobCostHint.EXPENSIVE, ReconcileCapturePolicy.empty().maxCost());
  }

  @Test
  void ofFactoryHasExpensiveDefaultMaxCost() {
    var policy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS));
    assertEquals(JobCostHint.EXPENSIVE, policy.maxCost());
  }

  @Test
  void withMaxCostReturnsCopyWithNewCost() {
    var base =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS));
    var medium = base.withMaxCost(JobCostHint.MEDIUM);
    assertEquals(JobCostHint.MEDIUM, medium.maxCost());
    // Original unchanged
    assertEquals(JobCostHint.EXPENSIVE, base.maxCost());
    // Other fields preserved
    assertEquals(base.outputs(), medium.outputs());
  }

  @Test
  void withMaxCostCheap() {
    var policy = ReconcileCapturePolicy.empty().withMaxCost(JobCostHint.CHEAP);
    assertEquals(JobCostHint.CHEAP, policy.maxCost());
  }
}
