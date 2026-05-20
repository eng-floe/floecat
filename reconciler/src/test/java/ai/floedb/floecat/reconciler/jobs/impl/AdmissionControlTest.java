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

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for band-based admission control in {@link InMemoryReconcileJobStore#enqueue}.
 *
 * <p>The logic is: P0/P1 are always admitted immediately. P2 is deferred only when the band is RED.
 * P3 is deferred when ORANGE or RED, probabilistically when YELLOW, and never when GREEN.
 *
 * <p>Tests that need deterministic YELLOW behavior avoid the probabilistic branch by checking the
 * deferred counter across multiple enqueues or by using GREEN/ORANGE/RED bands which are
 * deterministic.
 */
class AdmissionControlTest {

  // Reference the store constant directly so a change in the default defer delay is caught here.
  private static final long DEFER_DELAY_MS = InMemoryReconcileJobStore.DEFER_DELAY_MS;

  // ---------------------------------------------------------------------------
  // P3 + GREEN → immediate admission
  // ---------------------------------------------------------------------------

  @Test
  void p3JobAdmittedImmediatelyWhenGreen() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.GREEN);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3-green"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    // Should be dispatchable immediately
    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "P3 job should be dispatched immediately in GREEN band");
    assertEquals(jobId, leased.get().jobId);
    assertEquals(
        0L,
        store
            .queueStats()
            .admissionDeferredByClass
            .getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L));
  }

  // ---------------------------------------------------------------------------
  // P3 + RED → deferred by 5 s
  // ---------------------------------------------------------------------------

  @Test
  void p3JobDeferredWhenRed() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3-red"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    // Job should not be dispatched immediately (nextAttemptAtMs is in the future)
    assertTrue(store.leaseNext().isEmpty(), "P3 job should be deferred in RED band");
    assertEquals(
        1L,
        store
            .queueStats()
            .admissionDeferredByClass
            .getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L));
  }

  // ---------------------------------------------------------------------------
  // P0 + RED → always admitted
  // ---------------------------------------------------------------------------

  @Test
  void p0JobAlwaysAdmittedEvenWhenRed() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0-red"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", java.util.Map.of()),
            "");

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "P0 job must be dispatched immediately even in RED band");
    assertEquals(jobId, leased.get().jobId);
    assertEquals(
        0L,
        store.queueStats().admissionDeferredByClass.getOrDefault(StatsPriorityClass.P0_SYNC, 0L));
  }

  // ---------------------------------------------------------------------------
  // P1 + RED → always admitted
  // ---------------------------------------------------------------------------

  @Test
  void p1JobAlwaysAdmittedEvenWhenRed() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p1-red"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P1_FRESHNESS, "", java.util.Map.of()),
            "");

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "P1 job must be dispatched immediately even in RED band");
    assertEquals(jobId, leased.get().jobId);
  }

  // ---------------------------------------------------------------------------
  // P2 + ORANGE → not deferred
  // ---------------------------------------------------------------------------

  @Test
  void p2JobNotDeferredWhenOrange() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.ORANGE);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p2-orange"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P2_REPAIR, "", java.util.Map.of()),
            "");

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "P2 job should not be deferred in ORANGE band");
    assertEquals(jobId, leased.get().jobId);
    assertEquals(
        0L,
        store.queueStats().admissionDeferredByClass.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L));
  }

  // ---------------------------------------------------------------------------
  // P2 + RED → deferred
  // ---------------------------------------------------------------------------

  @Test
  void p2JobDeferredWhenRed() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-p2-red"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P2_REPAIR, "", java.util.Map.of()),
        "");

    assertTrue(store.leaseNext().isEmpty(), "P2 job should be deferred in RED band");
    assertEquals(
        1L,
        store.queueStats().admissionDeferredByClass.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L));
  }

  // ---------------------------------------------------------------------------
  // P3 + ORANGE → always deferred
  // ---------------------------------------------------------------------------

  @Test
  void p3JobAlwaysDeferredWhenOrange() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.ORANGE);

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-p3-orange"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
        "");

    assertTrue(store.leaseNext().isEmpty(), "P3 job should be deferred in ORANGE band");
    assertEquals(
        1L,
        store
            .queueStats()
            .admissionDeferredByClass
            .getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L));
  }

  // ---------------------------------------------------------------------------
  // admissionDeferredByClass counter accumulates across multiple deferrals
  // ---------------------------------------------------------------------------

  @Test
  void admissionDeferredCounterAccumulatesAcrossMultipleDeferrals() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    for (int i = 0; i < 3; i++) {
      store.enqueue(
          "acct",
          "conn-" + i,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-acc-" + i),
          ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
          "");
    }

    assertEquals(
        3L,
        store
            .queueStats()
            .admissionDeferredByClass
            .getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L));
  }

  // ---------------------------------------------------------------------------
  // P0 timeout path: stale P0 triggers RED escalation in maybeRefreshBand
  // ---------------------------------------------------------------------------

  /**
   * When a P0 job has been waiting longer than the P0 sync budget (1 s), the next {@code enqueue()}
   * call must escalate the band to RED via {@code maybeRefreshBand}, causing subsequently enqueued
   * P3 jobs to be deferred.
   *
   * <p>The test backdates the P0 job's creation timestamp to simulate a 2-second wait, then forces
   * a band-refresh by resetting {@link InMemoryReconcileJobStore#lastBandRefreshMs} via the
   * package-private test helper before the P3 enqueue.
   */
  @Test
  void p0TimeoutEscalatesToRedDeferringSubsequentP3() {
    var store = new InMemoryReconcileJobStore();

    // Enqueue a P0 job and immediately backdate it so it appears to have been waiting 2s.
    String p0JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0-timeout"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", java.util.Map.of()),
            "");
    store.backdateCreatedAtForTest(p0JobId, System.currentTimeMillis() - 2_000L);

    // Reset the refresh TTL so the next enqueue() will run maybeRefreshBand.
    store.resetBandRefreshForTest();

    // Now enqueue a P3 job — maybeRefreshBand should detect the stale P0 and escalate to RED.
    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-p3-after-p0-timeout"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
        "");

    var stats = store.queueStats();
    assertEquals(
        SchedulerHealthBand.RED, stats.healthBand, "band should be RED due to stale P0 job");
    assertEquals(
        1L,
        stats.admissionDeferredByClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L),
        "P3 job should have been deferred by RED-band admission control");
  }

  // ---------------------------------------------------------------------------
  // RED clears immediately when P0 queue empties (spec: "RED drops as soon as P0 queue clears")
  // ---------------------------------------------------------------------------

  /**
   * After a P0 job is dispatched and the P0 ready queue becomes empty, {@code leaseNext()} calls
   * {@code maybeClearP0RedBand()}, which must downgrade the band from RED to the depth-based
   * required band (GREEN when there is no P2/P3 backlog). Subsequent P3 admissions must not be
   * deferred.
   */
  @Test
  void redBandClearsImmediatelyAfterP0QueueEmpties() {
    var store = new InMemoryReconcileJobStore();

    // Force RED externally (simulates a state set by queueStats() due to P0 timeout).
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    // Enqueue a P0 job. maybeRefreshBand is escalation-only — RED stays.
    String p0Id =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0-red-clear"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", java.util.Map.of()),
            "");

    // P0 must dispatch even in RED.
    var p0Lease =
        store.leaseNext().orElseThrow(() -> new AssertionError("P0 must dispatch in RED"));
    assertEquals(p0Id, p0Lease.jobId);
    // After dispatch, maybeClearP0RedBand() fires: P0 queue empty → band drops to GREEN.

    // Enqueue a P3 job within the 1s maybeRefreshBand TTL window — so the band visible at
    // admission time is whatever maybeClearP0RedBand() set, not a re-escalation from the scan.
    String p3Id =
        store.enqueue(
            "acct",
            "conn2",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3-after-p0-clear"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    var p3Lease = store.leaseNext();
    assertTrue(p3Lease.isPresent(), "P3 must be admitted after RED clears");
    assertEquals(p3Id, p3Lease.get().jobId);
    var stats = store.queueStats();
    assertEquals(
        0L,
        stats.admissionDeferredByClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L),
        "P3 should not have been deferred — band cleared before its enqueue");
  }
}
