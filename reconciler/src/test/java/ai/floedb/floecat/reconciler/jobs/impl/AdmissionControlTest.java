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

  private static final long DEFER_DELAY_MS = 5_000L;

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
}
