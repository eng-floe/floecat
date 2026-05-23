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

package ai.floedb.floecat.service.statistics.scheduler.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TestSchedulerPriorityPolicy}.
 *
 * <p>Verifies that the fixed-class and fixed-score semantics work correctly for all factory
 * methods, and that the lane key is derived correctly from the request's table ID.
 */
class TestSchedulerPriorityPolicyTest {

  // ---------------------------------------------------------------------------
  // Factory methods
  // ---------------------------------------------------------------------------

  @Test
  void alwaysP3ReturnsP3Background() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP3();
    PriorityAssignment assignment = policy.assign(req("acct1", "tbl1"), null);
    assertEquals(StatsPriorityClass.P3_BACKGROUND, assignment.priorityClass());
    assertEquals(0L, assignment.score());
  }

  @Test
  void alwaysP1ReturnsP1Freshness() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP1();
    PriorityAssignment assignment = policy.assign(req("acct2", "tbl2"), null);
    assertEquals(StatsPriorityClass.P1_FRESHNESS, assignment.priorityClass());
    assertEquals(0L, assignment.score());
  }

  @Test
  void alwaysP2ReturnsP2Repair() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP2();
    PriorityAssignment assignment = policy.assign(req("acct3", "tbl3"), null);
    assertEquals(StatsPriorityClass.P2_REPAIR, assignment.priorityClass());
    assertEquals(0L, assignment.score());
  }

  @Test
  void ofFactoryReturnsSpecifiedClassAndScore() {
    TestSchedulerPriorityPolicy policy =
        TestSchedulerPriorityPolicy.of(StatsPriorityClass.P1_FRESHNESS, 42L);
    PriorityAssignment assignment = policy.assign(req("acct4", "tbl4"), null);
    assertEquals(StatsPriorityClass.P1_FRESHNESS, assignment.priorityClass());
    assertEquals(42L, assignment.score());
  }

  // ---------------------------------------------------------------------------
  // Lane key derivation
  // ---------------------------------------------------------------------------

  @Test
  void laneKeyIsAccountIdColonTableId() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP3();
    PriorityAssignment assignment = policy.assign(req("myaccount", "mytable"), null);
    assertEquals("myaccount:mytable", assignment.laneKey());
  }

  @Test
  void laneKeyDiffersAcrossRequests() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP3();
    PriorityAssignment a1 = policy.assign(req("acct", "tbl1"), null);
    PriorityAssignment a2 = policy.assign(req("acct", "tbl2"), null);
    assertNotNull(a1.laneKey());
    assertNotNull(a2.laneKey());
    assertEquals("acct:tbl1", a1.laneKey());
    assertEquals("acct:tbl2", a2.laneKey());
  }

  // ---------------------------------------------------------------------------
  // assignForReconcileJob — uses interface default
  // ---------------------------------------------------------------------------

  @Test
  void assignForReconcileJobNewSnapshotReturnsP1Freshness() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP3();
    PriorityAssignment assignment =
        policy.assignForReconcileJob(
            ReconcileJobKind.PLAN_SNAPSHOT, "acct:tbl", 1L, /* isNewSnapshot= */ true, null);
    assertEquals(StatsPriorityClass.P1_FRESHNESS, assignment.priorityClass());
  }

  @Test
  void assignForReconcileJobExistingSnapshotReturnsP3Background() {
    TestSchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP1();
    PriorityAssignment assignment =
        policy.assignForReconcileJob(
            ReconcileJobKind.EXEC_FILE_GROUP, "acct:tbl", 2L, /* isNewSnapshot= */ false, null);
    // default implementation is used regardless of fixedClass
    assertEquals(StatsPriorityClass.P3_BACKGROUND, assignment.priorityClass());
  }

  // ---------------------------------------------------------------------------
  // Constructor accessors
  // ---------------------------------------------------------------------------

  @Test
  void fixedClassAndScoreAccessors() {
    TestSchedulerPriorityPolicy policy =
        new TestSchedulerPriorityPolicy(StatsPriorityClass.P2_REPAIR, 99L);
    assertEquals(StatsPriorityClass.P2_REPAIR, policy.fixedClass());
    assertEquals(99L, policy.fixedScore());
  }

  @Test
  void negativeScoreClampedToZero() {
    TestSchedulerPriorityPolicy policy =
        new TestSchedulerPriorityPolicy(StatsPriorityClass.P3_BACKGROUND, -5L);
    assertEquals(0L, policy.fixedScore());
    assertEquals(0L, policy.assign(req("a", "b"), null).score());
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private static StatsCaptureRequest req(String accountId, String tableId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId(accountId).setId(tableId).build(),
            1L,
            StatsTarget.getDefaultInstance())
        .build();
  }
}
