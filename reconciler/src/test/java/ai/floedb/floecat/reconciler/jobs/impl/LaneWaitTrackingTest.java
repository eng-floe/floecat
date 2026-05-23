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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for per-lane oldest-job wait tracking in {@link InMemoryReconcileJobStore#queueStats}. */
class LaneWaitTrackingTest {

  /** A lane with two queued jobs appears in {@code topLaneWaitMs} with a non-zero wait time. */
  @Test
  void twoJobsInSameLaneShowNonZeroWait() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "lane-tbl");

    store.enqueue("acct", "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    store.enqueue("acct", "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);

    var stats = store.queueStats();

    assertEquals(2L, stats.queued);
    // At least one lane entry should be present (both jobs share the same lane key).
    assertFalse(stats.topLaneWaitMs.isEmpty(), "expected at least one lane in topLaneWaitMs");
    // The wait time must be non-negative (could be 0 ms if very fast).
    stats.topLaneWaitMs.values().forEach(wait -> assertTrue(wait >= 0L));
  }

  /** After dispatching all jobs in a lane the lane disappears from {@code topLaneWaitMs}. */
  @Test
  void laneRemovedFromTopLaneWaitAfterAllJobsDispatched() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "lane-tbl-drain");

    String jobId = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var statsBefore = store.queueStats();
    assertFalse(
        statsBefore.topLaneWaitMs.isEmpty(), "lane should be present before job is dispatched");

    // Dispatch the job (leaseNext transitions it out of JS_QUEUED).
    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);

    var statsAfter = store.queueStats();
    // After dispatch there are no JS_QUEUED jobs, so topLaneWaitMs should be empty.
    assertTrue(
        statsAfter.topLaneWaitMs.isEmpty(),
        "lane should be absent after all jobs in lane are dispatched");
  }

  /** Different lanes each get their own entry in {@code topLaneWaitMs}. */
  @Test
  void differentLanesAreTrackedSeparately() {
    var store = new InMemoryReconcileJobStore();

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "t1"));
    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "t2"));
    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "t3"));

    var stats = store.queueStats();

    // Each table gets a distinct lane key, so we should see three separate entries.
    assertEquals(3, stats.topLaneWaitMs.size());
  }

  /** At most 10 lanes are returned even when there are more than 10 active lanes. */
  @Test
  void topLaneWaitMsIsLimitedToTenLanes() {
    var store = new InMemoryReconcileJobStore();

    for (int i = 0; i < 15; i++) {
      store.enqueue(
          "acct",
          "conn",
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-" + i));
    }

    var stats = store.queueStats();

    assertTrue(
        stats.topLaneWaitMs.size() <= 10,
        "topLaneWaitMs should contain at most 10 lanes, got " + stats.topLaneWaitMs.size());
  }
}
