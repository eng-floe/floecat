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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for priority-aware dispatch in {@link InMemoryReconcileJobStore}.
 *
 * <p>Verifies that the {@link ai.floedb.floecat.reconciler.jobs.impl.PriorityReadyQueue} correctly
 * orders jobs by class (P0 before P3) and by score (higher score first within a class).
 */
class InMemoryStorePriorityDispatchTest {

  private static ReconcileExecutionPolicy policyFor(StatsPriorityClass cls, long score) {
    return ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", Map.of(), cls, score);
  }

  private static String enqueueJob(
      InMemoryReconcileJobStore store, String connId, ReconcileExecutionPolicy policy) {
    return enqueueJob(store, connId, "tbl", policy);
  }

  private static String enqueueJob(
      InMemoryReconcileJobStore store,
      String connId,
      String destinationTableId,
      ReconcileExecutionPolicy policy) {
    return store.enqueue(
        "acct",
        connId,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), destinationTableId),
        ReconcileJobKind.PLAN_CONNECTOR,
        null,
        null,
        null,
        null,
        policy,
        "",
        "");
  }

  private static LeasedJob leaseAndComplete(InMemoryReconcileJobStore store) {
    LeasedJob lease = store.leaseNext(ReconcileJobStore.LeaseRequest.all()).orElseThrow();
    store.markSucceeded(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 0L);
    return lease;
  }

  @Test
  void p0IsDispatchedBeforeP3WhenP3EnqueuedFirst() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, 0L));
    String p0Id = enqueueJob(store, "conn-p0", policyFor(StatsPriorityClass.P0_SYNC, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent(), "First lease must succeed");
    assertEquals(
        p0Id, first.get().jobId, "P0 job must be dispatched first regardless of enqueue order");
    assertEquals(StatsPriorityClass.P0_SYNC, first.get().executionPolicy.priorityClass());
  }

  @Test
  void p1IsDispatchedBeforeP2AndP3() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, 0L));
    enqueueJob(store, "conn-p2", policyFor(StatsPriorityClass.P2_REPAIR, 0L));
    String p1Id = enqueueJob(store, "conn-p1", policyFor(StatsPriorityClass.P1_FRESHNESS, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(p1Id, first.get().jobId, "P1 must be dispatched before P2 and P3");
  }

  @Test
  void higherScoreIsDispatchedBeforeLowerScoreWithinSameClass() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-low", policyFor(StatsPriorityClass.P3_BACKGROUND, 10L));
    String highId =
        enqueueJob(store, "conn-high", policyFor(StatsPriorityClass.P3_BACKGROUND, 500L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(
        highId,
        first.get().jobId,
        "Higher-score P3 job (500) must be dispatched before lower-score P3 job (10)");
  }

  @Test
  void classOrderPrevalenceOverScore() {
    // P3 at maximum score must still lose to P0 at score 0.
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, Long.MAX_VALUE));
    String p0Id = enqueueJob(store, "conn-p0", policyFor(StatsPriorityClass.P0_SYNC, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(p0Id, first.get().jobId, "P0 at score=0 must beat P3 at max score");
  }

  // ---------------------------------------------------------------------------
  // Admission deferral + health band
  // ---------------------------------------------------------------------------

  @Test
  void p3JobDeferredWhenBandIsOrange() {
    var store = new InMemoryReconcileJobStore();
    var p2Policy = policyFor(StatsPriorityClass.P2_REPAIR, 0L);
    // Enqueue enough P2 jobs to push the band to ORANGE.
    for (int i = 0; i < (int) SchedulerBandState.P2_ORANGE_THRESHOLD + 5; i++) {
      enqueueJob(store, "conn-" + i, p2Policy);
    }
    // Trigger authoritative band computation.
    var stats = store.queueStats();
    assertEquals(
        ai.floedb.floecat.stats.spi.SchedulerHealthBand.ORANGE,
        stats.healthBand,
        "Band must be ORANGE when P2 depth exceeds threshold");

    // Enqueue a P3 job under ORANGE — it should be deferred.
    String p3Id = enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, 0L));

    // The first leased job must be a P2 job, not the deferred P3 job.
    var lease = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(lease.isPresent(), "A P2 job should be leasable");
    assertNotEquals(
        p3Id, lease.get().jobId, "P3 job must not be dispatched before deferral expires");
  }

  @Test
  void bandEscalatesToYellowAtP3Threshold() {
    var store = new InMemoryReconcileJobStore();
    var p3Policy = policyFor(StatsPriorityClass.P3_BACKGROUND, 0L);
    for (int i = 0; i < (int) SchedulerBandState.P3_YELLOW_THRESHOLD + 5; i++) {
      enqueueJob(store, "conn-" + i, p3Policy);
    }
    var stats = store.queueStats();
    assertEquals(
        ai.floedb.floecat.stats.spi.SchedulerHealthBand.YELLOW,
        stats.healthBand,
        "Band must be YELLOW when P3 depth exceeds threshold");
  }

  @Test
  void equalWeightLanesAlternateDispatchWithinClass() {
    var store = new InMemoryReconcileJobStore();
    for (int i = 0; i < 10; i++) {
      enqueueJob(
          store, "conn-a-" + i, "table-a", policyFor(StatsPriorityClass.P3_BACKGROUND, 500L));
      enqueueJob(
          store, "conn-b-" + i, "table-b", policyFor(StatsPriorityClass.P3_BACKGROUND, 499L));
    }

    List<String> seen = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      var lease = leaseAndComplete(store);
      seen.add(lease.scope.destinationTableId());
    }

    for (int i = 1; i < seen.size(); i++) {
      assertNotEquals(seen.get(i - 1), seen.get(i), "equal-weight lanes should alternate dispatch");
    }
  }

  @Test
  void weightedLanesDispatchProportionallyTwoToOne() {
    String laneAWeightKey = "floecat.stats.scheduler.lane-weight.*|table-a";
    String laneBWeightKey = "floecat.stats.scheduler.lane-weight.*|table-b";
    System.setProperty(laneAWeightKey, "2");
    System.setProperty(laneBWeightKey, "1");
    try {
      var store = new InMemoryReconcileJobStore();
      for (int i = 0; i < 30; i++) {
        enqueueJob(
            store, "conn-a-" + i, "table-a", policyFor(StatsPriorityClass.P3_BACKGROUND, 500L));
        enqueueJob(
            store, "conn-b-" + i, "table-b", policyFor(StatsPriorityClass.P3_BACKGROUND, 499L));
      }

      int aDispatches = 0;
      int bDispatches = 0;
      for (int i = 0; i < 30; i++) {
        var lease = leaseAndComplete(store);
        if ("table-a".equals(lease.scope.destinationTableId())) {
          aDispatches++;
        } else if ("table-b".equals(lease.scope.destinationTableId())) {
          bDispatches++;
        }
      }
      assertEquals(20, aDispatches, "2x-weight lane should get two-thirds of dispatches");
      assertEquals(10, bDispatches, "1x-weight lane should get one-third of dispatches");
    } finally {
      System.clearProperty(laneAWeightKey);
      System.clearProperty(laneBWeightKey);
    }
  }

  @Test
  void missingLaneIsSkippedWithoutAffectingActiveLane() {
    String inactiveLaneWeightKey = "floecat.stats.scheduler.lane-weight.*|table-missing";
    System.setProperty(inactiveLaneWeightKey, "10");
    try {
      var store = new InMemoryReconcileJobStore();
      for (int i = 0; i < 6; i++) {
        enqueueJob(
            store,
            "conn-active-" + i,
            "table-active",
            policyFor(StatsPriorityClass.P3_BACKGROUND, 1L));
      }
      for (int i = 0; i < 6; i++) {
        var lease = leaseAndComplete(store);
        assertEquals(
            "table-active",
            lease.scope.destinationTableId(),
            "active lane should dispatch normally when weighted lane has no jobs");
      }
    } finally {
      System.clearProperty(inactiveLaneWeightKey);
    }
  }
}
