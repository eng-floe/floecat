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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for two hardening fixes in {@link InMemoryReconcileJobStore#leaseNext}:
 *
 * <ul>
 *   <li><b>F-01/F-08</b>: When {@code leased.add()} returns {@code false} (job already held by
 *       another lease, e.g. a CANCELLING job re-enqueued by {@link
 *       InMemoryReconcileJobStore#cancel}), the job must be requeued rather than silently dropped
 *       from the skip-list.
 *   <li><b>F-06</b>: Jobs without a lane key (blank lane) must not accumulate a {@code
 *       laneServiceCounts} entry; a blank key in the counter map would cause unbounded growth and
 *       corrupt WRR virtual-time for all future blank-lane jobs.
 * </ul>
 */
class LeasedAddFallbackTest {

  // ---------------------------------------------------------------------------
  // F-01 / F-08: CANCELLING job survives leased.add() == false
  // ---------------------------------------------------------------------------

  /**
   * Sequence:
   *
   * <ol>
   *   <li>Enqueue a P3 job and lease it (JS_RUNNING).
   *   <li>Call {@code cancel()} — sets job to JS_CANCELLING, re-enqueues it in the skip-list, and
   *       shortens the lease to 1 s.
   *   <li>Call {@code leaseNext()} immediately — the CANCELLING job is at the head of the
   *       skip-list, but {@code leased.add()} returns {@code false} because the original lease is
   *       still active. Without the fix the job vanishes from the skip-list; with the fix it is
   *       requeued.
   *   <li>Force-expire all leases (test helper) and call {@code leaseNext()} again. The job must be
   *       re-dispatched as JS_CANCELLING — proof that it was not permanently dropped.
   * </ol>
   */
  @Test
  void cancellingJobRequeuedWhenLeasedAddFails() {
    var store = new InMemoryReconcileJobStore();

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-cancel-requeue"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "lane-x", Map.of()),
            "");

    // Step 1: Lease the job (it enters the `leased` set)
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent(), "job should be leased");
    assertEquals(jobId, firstLease.get().jobId);

    // Step 2: Cancel — sets JS_CANCELLING and re-enqueues into skip-list
    store.cancel("acct", jobId, "test cancel");

    // Step 3: leaseNext() polls the CANCELLING job but leased.add() returns false; the fix
    // ensures the job is requeued rather than dropped.
    var immediateAttempt = store.leaseNext();
    assertTrue(
        immediateAttempt.isEmpty(),
        "leaseNext() must return empty while original lease is still active");

    // Step 4: Force-expire all leases; reclaimExpiredLeasesIfDue will run on the next leaseNext().
    store.forceExpireAllLeasesForTest();

    var afterExpiry = store.leaseNext();
    assertTrue(
        afterExpiry.isPresent(),
        "CANCELLING job must be re-dispatched after lease expires — it must NOT have been silently"
            + " dropped from the skip-list (F-01/F-08)");
    assertEquals(jobId, afterExpiry.get().jobId);
  }

  // ---------------------------------------------------------------------------
  // F-06: blank-lane job does not pollute laneServiceCounts
  // ---------------------------------------------------------------------------

  /**
   * A job enqueued without an explicit lane key (blank lane) must leave {@code laneServiceCounts}
   * untouched after dispatch. Accumulating a blank-key entry would corrupt WRR virtual-time for all
   * future jobs that also have no lane key.
   */
  @Test
  void blankLaneJobDoesNotIncrementLaneServiceCount() {
    var store = new InMemoryReconcileJobStore();

    // Enqueue a job with the default policy (blank lane)
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-blank-lane"),
            ReconcileExecutionPolicy.defaults(),
            "");

    var leased = store.leaseNext();
    assertTrue(leased.isPresent(), "job should be dispatched");
    assertEquals(jobId, leased.get().jobId);

    // The blank lane key ("") must have no entry in laneServiceCounts (F-06).
    assertEquals(
        0L,
        store.laneServiceCountForTest(""),
        "blank lane key must not accumulate in laneServiceCounts");
  }

  /**
   * Contrasting test: an explicit lane key IS incremented, confirming the guard applies only to
   * blank keys and not to all lanes.
   */
  @Test
  void explicitLaneJobIncrementsLaneServiceCount() {
    var store = new InMemoryReconcileJobStore();

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-explicit-lane"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "my-lane", Map.of()),
        "");

    var leased = store.leaseNext();
    assertTrue(leased.isPresent());

    assertEquals(
        1L,
        store.laneServiceCountForTest("my-lane"),
        "explicit lane must be incremented after dispatch");
    assertEquals(
        0L,
        store.laneServiceCountForTest(""),
        "blank key must never be incremented regardless of other lanes");
  }
}
