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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryReconcileJobStoreTest {

  @Test
  void enqueueDedupesWhileJobIsActive() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(first, second);
  }

  @Test
  void enqueueDoesNotDedupeAcrossDifferentExecutionPolicies() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", java.util.Map.of()),
            "");
    String second =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
            "");

    assertNotEquals(first, second);
  }

  @Test
  void enqueueExecViewRejectsMismatchedDestinationNamespaceIds() {
    var store = new InMemoryReconcileJobStore();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.enqueue(
                    "acct",
                    "conn",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of("analytics-namespace-id"), null),
                    ReconcileJobKind.PLAN_VIEW,
                    ReconcileTableTask.empty(),
                    ReconcileViewTask.of(
                        "db", "events_summary", "other-namespace-id", "events-summary-id"),
                    ReconcileExecutionPolicy.defaults(),
                    "",
                    ""));

    assertEquals(
        "view task destinationNamespaceId does not match scope destinationNamespaceIds",
        error.getMessage());
  }

  @Test
  void enqueueExecTableDedupesOnDestinationTableIdNotDisplayName() {
    var store = new InMemoryReconcileJobStore();

    String first =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders_v1"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String second =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders_v2"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    assertEquals(first, second);
  }

  @Test
  void persistSnapshotPlanUpdatesStoredJobPayload() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    store.persistSnapshotPlan(
        jobId,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(
                ReconcileFileGroupTask.of(
                    jobId,
                    "snapshot-55-group-0",
                    "table-1",
                    55L,
                    List.of("s3://bucket/data/file-1.parquet"))),
            true));

    var job = store.get("acct", jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
  }

  @Test
  void persistFileGroupResultUpdatesStoredJobPayload() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    store.persistFileGroupResult(
        jobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(
                ai.floedb.floecat.reconciler.jobs.ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet",
                    2L,
                    ReconcileIndexArtifactResult.of(
                        "s3://bucket/index/file-1.parquet.index", "parquet", 1)))));

    var job = store.get("acct", jobId).orElseThrow();
    assertEquals(1, job.fileGroupTask.fileResults().size());
    assertEquals(2L, job.fileGroupTask.fileResults().getFirst().statsProcessed());
    assertEquals(
        "s3://bucket/index/file-1.parquet.index",
        job.fileGroupTask.fileResults().getFirst().indexArtifact().artifactUri());
  }

  @Test
  void directEnqueueRejectsImplicitSnapshotCoverageForFinalization() {
    var store = new InMemoryReconcileJobStore();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.enqueue(
                    "acct",
                    "conn",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ReconcileTableTask.empty(),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "parent-1",
                    ""));

    assertTrue(error.getMessage().contains("FINALIZE_SNAPSHOT_CAPTURE"));
  }

  @Test
  void persistSnapshotPlanRejectsImplicitCoverageForPlanSnapshotJobs() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.persistSnapshotPlan(
                    jobId,
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(
                            ReconcileFileGroupTask.of(
                                jobId,
                                "snapshot-55-group-0",
                                "table-1",
                                55L,
                                List.of("s3://bucket/data/file-1.parquet"))))));

    assertTrue(error.getMessage().contains("persistSnapshotPlan"));
  }

  @Test
  void persistSnapshotPlanRejectsEmptyCoverageForPlanSnapshotJobs() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> store.persistSnapshotPlan(jobId, ReconcileSnapshotTask.empty()));

    assertTrue(error.getMessage().contains("persistSnapshotPlan"));
  }

  @Test
  void leaseNextSerializesSnapshotFinalizersPerSnapshot() {
    var store = new InMemoryReconcileJobStore();
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true);

    store.enqueueSnapshotFinalization(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        "parent-1",
        "");
    store.enqueueSnapshotFinalization(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        "parent-2",
        "");

    var firstLease = store.leaseNext().orElseThrow();

    assertEquals(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, firstLease.jobKind);
    assertTrue(store.leaseNext().isEmpty());
  }

  @Test
  void leaseNextAllowsConcurrentExecFileGroupsForDifferentGroups() {
    var store = new InMemoryReconcileJobStore();

    String firstJobId =
        store.enqueueFileGroupExecution(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-1",
            "");
    String secondJobId =
        store.enqueueFileGroupExecution(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-1",
            "");

    var firstLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    var secondLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();

    assertTrue(
        java.util.Set.of(firstLease.jobId, secondLease.jobId)
            .containsAll(java.util.Set.of(firstJobId, secondJobId)));
  }

  @Test
  void leaseNextAllowsOnlyOneRunningJobPerTableAcrossConnectors() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue("acct", "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob =
        store.enqueue("acct", "conn-b", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(firstJob, firstLease.jobId);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void leaseNextTreatsEquivalentMultiNamespaceScopesAsSameTableLane() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope firstScope = ReconcileScope.of(List.of("b", "a"), null);
    ReconcileScope secondScope = ReconcileScope.of(List.of("a", "b"), null);

    String firstJob =
        store.enqueue("acct", "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, firstScope);
    String secondJob =
        store.enqueue("acct", "conn-b", false, CaptureMode.CAPTURE_ONLY, secondScope);

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(firstJob, firstLease.jobId);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void leaseNextPreventsConcurrentSnapshotPlanningForSameTableSnapshot() {
    var store = new InMemoryReconcileJobStore();

    String firstJob =
        store.enqueueSnapshotPlan(
            "acct",
            "conn-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    String secondJob =
        store.enqueueSnapshotPlan(
            "acct",
            "conn-b",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-2",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(firstJob, firstLease.jobId);
    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void markFailedRequeuesAndEventuallyTransitionsToFailed() throws Exception {
    String maxAttemptsKey = "floecat.reconciler.job-store.max-attempts";
    String baseBackoffKey = "floecat.reconciler.job-store.base-backoff-ms";
    String maxBackoffKey = "floecat.reconciler.job-store.max-backoff-ms";
    String previousMaxAttempts = System.getProperty(maxAttemptsKey);
    String previousBaseBackoff = System.getProperty(baseBackoffKey);
    String previousMaxBackoff = System.getProperty(maxBackoffKey);
    try {
      System.setProperty(maxAttemptsKey, "2");
      System.setProperty(baseBackoffKey, "100");
      System.setProperty(maxBackoffKey, "100");

      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueue(
              "acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
      var firstLease = store.leaseNext().orElseThrow();

      store.markFailed(
          jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "transient", 1, 0, 1, 2, 3);
      var retried = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_QUEUED", retried.state);

      Thread.sleep(120L);
      var secondLease = store.leaseNext().orElseThrow();
      store.markFailed(
          jobId, secondLease.leaseEpoch, System.currentTimeMillis(), "terminal", 1, 0, 2, 2, 3);
      var failed = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_FAILED", failed.state);
    } finally {
      restoreProperty(maxAttemptsKey, previousMaxAttempts);
      restoreProperty(baseBackoffKey, previousBaseBackoff);
      restoreProperty(maxBackoffKey, previousMaxBackoff);
    }
  }

  @Test
  void markFailedPreservesViewTaskContext() {
    String maxAttemptsKey = "floecat.reconciler.job-store.max-attempts";
    String previousMaxAttempts = System.getProperty(maxAttemptsKey);
    try {
      System.setProperty(maxAttemptsKey, "1");
      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueue(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileJobKind.PLAN_VIEW,
              ReconcileTableTask.empty(),
              ReconcileViewTask.of("src_ns", "src_view", "dst-ns-id", "dst-view-id"),
              ReconcileExecutionPolicy.defaults(),
              "",
              "");
      var lease = store.leaseNext().orElseThrow();

      store.markFailed(jobId, lease.leaseEpoch, System.currentTimeMillis(), "boom", 0, 0, 1, 0, 1);

      var failed = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_FAILED", failed.state);
      assertEquals("src_ns", failed.viewTask.sourceNamespace());
      assertEquals("src_view", failed.viewTask.sourceView());
      assertEquals("dst-ns-id", failed.viewTask.destinationNamespaceId());
      assertEquals("dst-view-id", failed.viewTask.destinationViewId());
    } finally {
      restoreProperty(maxAttemptsKey, previousMaxAttempts);
    }
  }

  @Test
  void cancelIsIdempotentForCancellingJobs() {
    var store = new InMemoryReconcileJobStore();
    String jobId =
        store.enqueue(
            "acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
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
              "acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
      var lease = store.leaseNext().orElseThrow();
      store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

      Thread.sleep(1150L);

      var reclaimed = store.leaseNext().orElseThrow();
      assertEquals(jobId, reclaimed.jobId);
      var job = store.get("acct", jobId).orElseThrow();
      assertEquals("JS_RUNNING", job.state);
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
              "acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
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
