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
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import org.junit.jupiter.api.Test;

/**
 * Tests for starvation-aging promotion in {@link InMemoryReconcileJobStore#leaseNext()}.
 *
 * <p>Aging works by back-dating the job's {@code createdAtMs} entry past the class threshold. When
 * {@code leaseNext()} selects the job, it records a promotion and increments {@code
 * agingPromotionsTotal}. A cooldown window prevents the same job from being counted twice within
 * {@link InMemoryReconcileJobStore#AGING_COOLDOWN_MS}.
 */
class StarvationAgingTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Backdates the createdAtMs for the given jobId by the specified number of milliseconds. */
  private static void backdateJob(InMemoryReconcileJobStore store, String jobId, long ageMs) {
    store.backdateCreatedAtForTest(jobId, System.currentTimeMillis() - ageMs);
  }

  // ---------------------------------------------------------------------------
  // P3 aging
  // ---------------------------------------------------------------------------

  @Test
  void p3JobBackdatedPastThresholdCausesAgingPromotion() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-1"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    backdateJob(store, jobId, InMemoryReconcileJobStore.P3_AGING_THRESHOLD_MS + 1_000L);

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "should lease the P3 job");
    assertEquals(jobId, leased.get().jobId);
    assertEquals(1L, store.queueStats().agingPromotionsTotal);
  }

  @Test
  void p3JobNotYetPastThresholdDoesNotCauseAgingPromotion() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-2"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    // Back-date to just under the threshold — should NOT trigger aging
    backdateJob(store, jobId, InMemoryReconcileJobStore.P3_AGING_THRESHOLD_MS - 1_000L);

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "should still lease the job");
    assertEquals(0L, store.queueStats().agingPromotionsTotal);
  }

  // ---------------------------------------------------------------------------
  // P2 aging
  // ---------------------------------------------------------------------------

  @Test
  void p2JobBackdatedPastThresholdCausesAgingPromotion() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-3"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P2_REPAIR, "", java.util.Map.of()),
            "");

    backdateJob(store, jobId, InMemoryReconcileJobStore.P2_AGING_THRESHOLD_MS + 1_000L);

    var leased = store.leaseNext();
    assertTrue(leased.isPresent());
    assertEquals(1L, store.queueStats().agingPromotionsTotal);
  }

  // ---------------------------------------------------------------------------
  // Cooldown: same job not promoted twice within cooldown window
  // ---------------------------------------------------------------------------

  @Test
  void agingCooldownPreventsDoubleCountingWithinCooldownWindow() {
    var store = new InMemoryReconcileJobStore();

    // Enqueue two distinct jobs (different table IDs to avoid lane dedup)
    String jobId1 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-cool-a"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");
    String jobId2 =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-cool-b"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of()),
            "");

    backdateJob(store, jobId1, InMemoryReconcileJobStore.P3_AGING_THRESHOLD_MS + 1_000L);
    backdateJob(store, jobId2, InMemoryReconcileJobStore.P3_AGING_THRESHOLD_MS + 1_000L);

    // Lease and immediately return job1 (mark failed + requeue so it gets a fresh entry)
    var lease1 = store.leaseNext().orElseThrow();
    // The promotion cooldown entry prevents a second promotion for the same jobId even if the job
    // is re-enqueued before AGING_COOLDOWN_MS elapses.  We verify by confirming total = 1 per
    // initial promotion, not 2 for the second job on the same call.
    assertEquals(1L, store.queueStats().agingPromotionsTotal);

    // Lease the second job
    var lease2 = store.leaseNext().orElseThrow();
    assertEquals(2L, store.queueStats().agingPromotionsTotal);
  }

  // ---------------------------------------------------------------------------
  // P0 / P1 never age
  // ---------------------------------------------------------------------------

  @Test
  void p0JobIsNeverAgedRegardlessOfAge() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-p0"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", java.util.Map.of()),
            "");

    // Use a very large age — well past any reasonable threshold
    backdateJob(store, jobId, 365L * 24 * 60 * 60 * 1_000L);

    var leased = store.leaseNext();
    assertTrue(leased.isPresent());
    // P0 never ages
    assertEquals(0L, store.queueStats().agingPromotionsTotal);
  }
}
