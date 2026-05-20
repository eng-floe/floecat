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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for weighted-round-robin (WRR) lane dispatch fairness in {@link InMemoryReconcileJobStore}.
 *
 * <p>WRR is driven by {@code laneServiceCounts}: the job from the lane with the smallest virtual
 * service count wins the tournament within each priority class. Equal-count ties are broken by
 * which job was polled first from the skip-list (i.e. highest priority score, then FIFO).
 */
class WrrFairnessTest {

  // ---------------------------------------------------------------------------
  // Two-lane interleaving
  // ---------------------------------------------------------------------------

  /**
   * Enqueue two jobs per lane (lane-a and lane-b) all at the same P3 score. WRR should interleave
   * them: first dispatch from one lane, second from the other, and so on.
   *
   * <p>The exact lane for the very first call is unspecified (both have virtual time 0), but the
   * subsequent calls must alternate.
   */
  @Test
  void twoLanesInterleaveWhenEqualVirtualTime() {
    var store = new InMemoryReconcileJobStore();

    // Scope per job so lane keys are distinct
    ReconcileExecutionPolicy laneA =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "lane-a", java.util.Map.of());
    ReconcileExecutionPolicy laneB =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "lane-b", java.util.Map.of());

    String a1 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-a1"),
            laneA,
            "");
    String a2 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-a2"),
            laneA,
            "");
    String b1 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-b1"),
            laneB,
            "");
    String b2 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-b2"),
            laneB,
            "");

    List<String> dispatchedLanes = new ArrayList<>();
    List<String> dispatchedJobIds = new ArrayList<>();

    // Drain all 4 jobs; mark each succeeded so the lane is freed for the next job.
    for (int i = 0; i < 4; i++) {
      var leased = store.leaseNext();
      assertTrue(leased.isPresent(), "call " + i + " should find a job");
      String jobId = leased.get().jobId;
      dispatchedJobIds.add(jobId);
      String lane = leased.get().executionPolicy.lane();
      dispatchedLanes.add(lane);
      store.markSucceeded(jobId, leased.get().leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);
    }

    assertEquals(4, dispatchedJobIds.size());
    // All four job IDs must be dispatched
    assertTrue(dispatchedJobIds.containsAll(Set.of(a1, a2, b1, b2)));

    // Verify interleaving: adjacent dispatches must be from different lanes
    for (int i = 1; i < dispatchedLanes.size(); i++) {
      assertNotNull(dispatchedLanes.get(i - 1));
      assertNotNull(dispatchedLanes.get(i));
      String prev = dispatchedLanes.get(i - 1);
      String curr = dispatchedLanes.get(i);
      assertTrue(
          !prev.equals(curr),
          "Expected lanes to alternate but got consecutive: "
              + prev
              + " then "
              + curr
              + " (all dispatched: "
              + dispatchedLanes
              + ")");
    }
  }

  // ---------------------------------------------------------------------------
  // Single-lane FIFO preserved
  // ---------------------------------------------------------------------------

  /**
   * When all jobs share the same computed lane key (same table scope across different connectors),
   * they should be serialised by the lane mutex: only one job runs at a time, and they are
   * dispatched in enqueue order (FIFO within equal score).
   */
  @Test
  void singleLaneFifoPreserved() {
    var store = new InMemoryReconcileJobStore();

    // Same table scope → same computed laneKey "table|tbl-serial".
    // Different connectorIds avoid deduplication.
    ReconcileScope sharedScope = ReconcileScope.of(List.of(), "tbl-serial");
    ReconcileExecutionPolicy policy = ReconcileExecutionPolicy.defaults();

    String first =
        store.enqueue(
            "acct", "conn-s1", false, CaptureMode.METADATA_AND_CAPTURE, sharedScope, policy, "");
    String second =
        store.enqueue(
            "acct", "conn-s2", false, CaptureMode.METADATA_AND_CAPTURE, sharedScope, policy, "");
    String third =
        store.enqueue(
            "acct", "conn-s3", false, CaptureMode.METADATA_AND_CAPTURE, sharedScope, policy, "");

    // First leaseNext() should succeed and return the first-enqueued job.
    var l1 = store.leaseNext().orElseThrow();
    assertEquals(first, l1.jobId, "first job dispatched should be the first enqueued");

    // While first is running, the lane mutex blocks the other two.
    assertTrue(
        store.leaseNext().isEmpty(), "lane mutex should block second job while first is running");

    store.markSucceeded(first, l1.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);
    var l2 = store.leaseNext().orElseThrow();
    assertEquals(second, l2.jobId);

    store.markSucceeded(second, l2.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);
    var l3 = store.leaseNext().orElseThrow();
    assertEquals(third, l3.jobId);
  }

  // ---------------------------------------------------------------------------
  // Verify laneServiceCounts increments per dispatch
  // ---------------------------------------------------------------------------

  @Test
  void laneServiceCountIncrementedAfterEachDispatch() {
    var store = new InMemoryReconcileJobStore();

    ReconcileExecutionPolicy laneA =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "svc-lane-a", java.util.Map.of());
    ReconcileExecutionPolicy laneB =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "svc-lane-b", java.util.Map.of());

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "svc-tbl-a1"),
        laneA,
        "");
    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "svc-tbl-b1"),
        laneB,
        "");

    // First dispatch: one of the lanes gets count=1, the other remains at 0
    var l1 = store.leaseNext().orElseThrow();
    String firstLane = l1.executionPolicy.lane();
    store.markSucceeded(l1.jobId, l1.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    // Second dispatch must go to the other lane (virtual time 0 < 1)
    var l2 = store.leaseNext().orElseThrow();
    String secondLane = l2.executionPolicy.lane();

    assertTrue(
        !firstLane.equals(secondLane),
        "second dispatch should go to the other lane; got " + firstLane + " then " + secondLane);
  }

  // ---------------------------------------------------------------------------
  // Higher-priority class is never starved by WRR scan
  // ---------------------------------------------------------------------------

  @Test
  void p0JobDispatchedBeforeP3RegardlessOfWrrOrder() {
    var store = new InMemoryReconcileJobStore();

    // Enqueue P3 job first, then P0 job
    String p3Job =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3-wrr"),
            ReconcileExecutionPolicy.of(
                StatsPriorityClass.P3_BACKGROUND, "lane-p3", java.util.Map.of()),
            "");
    String p0Job =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0-wrr"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", java.util.Map.of()),
            "");

    var l1 = store.leaseNext().orElseThrow();
    assertEquals(p0Job, l1.jobId, "P0 job must be dispatched before P3");
  }
}
