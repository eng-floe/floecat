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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import org.junit.jupiter.api.Test;

class InMemoryReconcileJobStoreTest {

  @Test
  void markFailedPreservesViewTaskContext() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty(),
            ReconcileJobKind.EXEC_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of("src_ns", "src_view", "dst_ns", "dst_view"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    var lease = store.leaseNext().orElseThrow();

    store.markFailed(jobId, lease.leaseEpoch, System.currentTimeMillis(), "boom", 0, 0, 1, 0, 1);

    var failed = store.get("acct", jobId).orElseThrow();
    assertEquals("JS_FAILED", failed.state);
    assertEquals("src_ns", failed.viewTask.sourceNamespace());
    assertEquals("src_view", failed.viewTask.sourceView());
    assertEquals("dst_ns", failed.viewTask.destinationNamespace());
    assertEquals("dst_view", failed.viewTask.destinationViewDisplayName());
  }

  @Test
  void cancelIsIdempotentForCancellingJobs() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct", "conn", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();

    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel("acct", jobId, "first stop");
    var cancelling = store.get("acct", jobId).orElseThrow();

    assertEquals("JS_CANCELLING", cancelling.state);

    store.cancel("acct", jobId, "second stop");
    var stillCancelling = store.get("acct", jobId).orElseThrow();

    assertEquals("JS_CANCELLING", stillCancelling.state);
    assertEquals("first stop", stillCancelling.message);
  }

  @Test
  void leaseNextReclaimsExpiredRunningJobs() throws Exception {
    String leaseMsKey = "floecat.reconciler.job-store.lease-ms";
    String reclaimMsKey = "floecat.reconciler.job-store.reclaim-interval-ms";
    String previousLeaseMs = System.getProperty(leaseMsKey);
    String previousReclaimMs = System.getProperty(reclaimMsKey);
    try {
      System.setProperty(leaseMsKey, "1000");
      System.setProperty(reclaimMsKey, "1000");

      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueue(
              "acct", "conn", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
      var lease = store.leaseNext().orElseThrow();
      store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

      Thread.sleep(1150L);

      var reclaimed = store.leaseNext().orElseThrow();
      assertEquals(jobId, reclaimed.jobId);
      var job = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_QUEUED", job.state);
      assertEquals("Lease expired; requeued", job.message);
    } finally {
      restoreProperty(leaseMsKey, previousLeaseMs);
      restoreProperty(reclaimMsKey, previousReclaimMs);
    }
  }

  @Test
  void leaseNextReclaimsExpiredCancellingJobs() throws Exception {
    String leaseMsKey = "floecat.reconciler.job-store.lease-ms";
    String reclaimMsKey = "floecat.reconciler.job-store.reclaim-interval-ms";
    String previousLeaseMs = System.getProperty(leaseMsKey);
    String previousReclaimMs = System.getProperty(reclaimMsKey);
    try {
      System.setProperty(leaseMsKey, "5000");
      System.setProperty(reclaimMsKey, "1000");

      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueue(
              "acct", "conn", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
      var lease = store.leaseNext().orElseThrow();
      store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
      store.cancel("acct", jobId, "stop");

      Thread.sleep(1150L);

      var reclaimed = store.leaseNext().orElseThrow();
      assertEquals(jobId, reclaimed.jobId);
      store.markRunning(
          reclaimed.jobId, reclaimed.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
      var job = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_CANCELLING", job.state);
      assertEquals("Lease expired while cancelling", job.message);
      assertTrue(store.isCancellationRequested(jobId));
    } finally {
      restoreProperty(leaseMsKey, previousLeaseMs);
      restoreProperty(reclaimMsKey, previousReclaimMs);
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
      return;
    }
    System.setProperty(key, value);
  }
}
