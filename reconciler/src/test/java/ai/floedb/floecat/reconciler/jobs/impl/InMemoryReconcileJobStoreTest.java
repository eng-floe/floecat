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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
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
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.JobCostHint;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
  void enqueueDoesNotDedupeAcrossDifferentCapturePolicyMaxCost() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope cheapScope =
        ReconcileScope.of(
            List.of(),
            "tbl",
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS),
                JobCostHint.CHEAP));
    ReconcileScope expensiveScope =
        ReconcileScope.of(
            List.of(),
            "tbl",
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS),
                JobCostHint.EXPENSIVE));

    String cheapId =
        store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, cheapScope);
    String expensiveId =
        store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, expensiveScope);

    assertNotEquals(cheapId, expensiveId);
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

  // ---------------------------------------------------------------------------
  // Scheduler: priority-class dispatch ordering
  // ---------------------------------------------------------------------------

  @Test
  void leaseNextDispatchesP0BeforeP3() {
    var store = new InMemoryReconcileJobStore();

    // Enqueue P3 first, then P0 — dispatch should still return P0 first.
    String p3JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
            "");
    String p0JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", Map.of()),
            "");

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(p0JobId, firstLease.jobId, "P0_SYNC must be dispatched before P3_BACKGROUND");
    assertNotEquals(p3JobId, firstLease.jobId);
  }

  @Test
  void leaseNextDispatchesP1BeforeP3() {
    var store = new InMemoryReconcileJobStore();

    String p3JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p3b"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
            "");
    String p1JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p1"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P1_FRESHNESS, "", Map.of()),
            "");

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(p1JobId, firstLease.jobId, "P1_FRESHNESS must be dispatched before P3_BACKGROUND");
    assertNotEquals(p3JobId, firstLease.jobId);
  }

  @Test
  void enqueueDedupedQueuedJobPromotesPriorityClassAndScore() {
    var store = new InMemoryReconcileJobStore();
    String lane = "acct:tbl-dedupe";

    String initialJobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-dedupe"),
            new ReconcileExecutionPolicy(
                ReconcileExecutionClass.DEFAULT,
                lane,
                Map.of("reason", "initial"),
                StatsPriorityClass.P3_BACKGROUND,
                10L),
            "");

    String dedupedJobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-dedupe"),
            new ReconcileExecutionPolicy(
                ReconcileExecutionClass.DEFAULT,
                lane,
                Map.of("reason", "initial"),
                StatsPriorityClass.P1_FRESHNESS,
                90L),
            "");

    assertEquals(initialJobId, dedupedJobId);

    var promoted = store.get("acct", initialJobId).orElseThrow();
    assertEquals(StatsPriorityClass.P1_FRESHNESS, promoted.executionPolicy.priorityClass());
    assertEquals(90L, promoted.executionPolicy.priorityScore());

    var lease = store.leaseNext().orElseThrow();
    assertEquals(initialJobId, lease.jobId);
  }

  // ---------------------------------------------------------------------------
  // Scheduler: health band escalation
  // ---------------------------------------------------------------------------

  @Test
  void healthBandEscalatesToYellowWhenP3DepthExceedsThreshold() {
    // Threshold is SchedulerBandState.P3_YELLOW_THRESHOLD = 500.
    // Force the band-refresh TTL so each enqueue triggers an escalation check.
    var store = new InMemoryReconcileJobStore();
    store.resetBandRefreshForTest();

    // Enqueue P3_YELLOW_THRESHOLD + 1 = 501 P3 jobs on distinct tables so none are deduped.
    long threshold = SchedulerBandState.P3_YELLOW_THRESHOLD;
    for (int i = 0; i <= threshold; i++) {
      store.enqueue(
          "acct",
          "conn",
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-band-" + i),
          ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
          "");
      // Reset cooldown so each enqueue can re-evaluate the band.
      store.resetBandRefreshForTest();
    }

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertTrue(
        stats.healthBand.ordinal() >= SchedulerHealthBand.YELLOW.ordinal(),
        "Band should be at least YELLOW when P3 depth exceeds threshold, got: " + stats.healthBand);
  }

  @Test
  void healthBandEscalatesToRedOnP0Starvation() {
    // Force band to GREEN, then enqueue a P0 job whose creation time is backdated far into the
    // past to simulate a stale P0 starvation scenario.
    var store = new InMemoryReconcileJobStore();

    String p0JobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p0-stale"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", Map.of()),
            "");

    // Backdate creation to simulate P0 starvation well past the RED timeout.
    store.backdateCreatedAtForTest(p0JobId, System.currentTimeMillis() - 10_000L);
    store.resetBandRefreshForTest();

    // Trigger another enqueue to run the escalation check.
    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-trigger"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
        "");

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertEquals(
        SchedulerHealthBand.RED,
        stats.healthBand,
        "Band must escalate to RED when P0 job is stale");
  }

  // ---------------------------------------------------------------------------
  // Scheduler: admission deferral
  // ---------------------------------------------------------------------------

  @Test
  void admissionDeferredCounterIncrementsWhenP3EnqueuedUnderOrangeBand() {
    var store = new InMemoryReconcileJobStore();
    // Force band to ORANGE so P3 admission is deferred.
    store.setCurrentBandForTest(SchedulerHealthBand.ORANGE);

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-deferred"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
        "");

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertTrue(
        stats.admissionDeferredByClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L) >= 1L,
        "P3 admission deferred counter should be at least 1 under ORANGE band");
  }

  @Test
  void p0AdmissionNeverDeferredUnderRedBand() {
    var store = new InMemoryReconcileJobStore();
    store.setCurrentBandForTest(SchedulerHealthBand.RED);

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-p0-red"),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P0_SYNC, "", Map.of()),
        "");

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertEquals(
        0L,
        stats.admissionDeferredByClass.getOrDefault(StatsPriorityClass.P0_SYNC, 0L),
        "P0_SYNC must never be deferred regardless of band");
  }

  // ---------------------------------------------------------------------------
  // Scheduler: aging promotions
  // ---------------------------------------------------------------------------

  @Test
  void agingPromotionCounterIncrementsWhenJobExceedsAgingThreshold() {
    var store = new InMemoryReconcileJobStore();

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-aging"),
            ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "", Map.of()),
            "");

    // Backdate creation past the P3 aging threshold (300 s default).
    store.backdateCreatedAtForTest(jobId, System.currentTimeMillis() - 400_000L);

    // leaseNext() applies lazy aging promotion at lease time.
    store.leaseNext().orElseThrow();

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertTrue(
        stats.agingPromotionsTotal >= 1L,
        "Aging promotion counter should be at least 1 after a job exceeds its threshold");
  }

  // ---------------------------------------------------------------------------
  // Scheduler: execution-class filter fall-through
  // ---------------------------------------------------------------------------

  @Test
  void leaseNextAllowsFallThroughWhenP0JobDoesNotMatchLeaseRequestFilter() {
    var store = new InMemoryReconcileJobStore();
    store.enqueue(
        "acct-1",
        "conn-heavy",
        false,
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(java.util.List.of(), "tbl-p0"),
        new ReconcileExecutionPolicy(
            ReconcileExecutionClass.HEAVY, "", java.util.Map.of(), StatsPriorityClass.P0_SYNC, 10L),
        "");

    String p3JobId =
        store.enqueue(
            "acct-1",
            "conn-default",
            false,
            ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(java.util.List.of(), "tbl-p3"),
            ReconcileExecutionPolicy.of(
                StatsPriorityClass.P3_BACKGROUND, "", java.util.Map.of(), 1L),
            "");

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT), java.util.Set.of()))
            .orElse(null);

    assertNotNull(lease, "DEFAULT executor should dispatch P3 job even when a P0 HEAVY job exists");
    assertEquals(p3JobId, lease.jobId);
  }

  // ---------------------------------------------------------------------------
  // Scheduler: aging promotion tracker cleanup boundary
  // ---------------------------------------------------------------------------

  @Test
  void agingTrackerAllowsRePromotionAfterCooldownExpires() {
    var tracker = new AgingPromotionTracker();
    long now = 1_000_000L;
    long cooldown = SchedulerStoreHelpers.AGING_COOLDOWN_MS;
    long threshold = SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS;

    // First promotion: age exceeds threshold
    boolean first =
        tracker.recordIfEligible("job-1", threshold + 1, StatsPriorityClass.P3_BACKGROUND, now);
    assertTrue(first, "Should record first promotion");

    // Not eligible again within cooldown window
    boolean second =
        tracker.recordIfEligible(
            "job-1", threshold + 1, StatsPriorityClass.P3_BACKGROUND, now + cooldown - 1);
    assertFalse(second, "Should not re-promote within cooldown");

    // Cleanup at exactly expiry time (now + cooldown) removes the entry
    tracker.cleanupExpired(now + cooldown);

    // Now eligible again
    boolean third =
        tracker.recordIfEligible(
            "job-1", threshold + 1, StatsPriorityClass.P3_BACKGROUND, now + cooldown + 1);
    assertTrue(third, "Should allow re-promotion after cooldown expires");
    assertEquals(2L, tracker.totalPromotions());
  }

  // ---------------------------------------------------------------------------
  // Scheduler: policy-layer DEFER enforcement
  // ---------------------------------------------------------------------------

  @Test
  void policyDeferredAttributeDelaysP1JobInGreenBand() {
    // P1_FRESHNESS is always admitted by the store's band logic (admissionDeferMs returns 0).
    // When the ATTR_POLICY_DEFERRED attribute is set, the store must still apply DEFER_DELAY_MS
    // so that custom profile DEFER decisions are honoured even for high-priority classes.
    var store = new InMemoryReconcileJobStore();

    ReconcileExecutionPolicy deferredP1Policy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS,
            "lane-p1-test",
            Map.of(SchedulerStoreHelpers.ATTR_POLICY_DEFERRED, "true"),
            0L);

    store.enqueue(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-p1-deferred"),
        deferredP1Policy,
        "");

    assertTrue(
        store.leaseNext().isEmpty(),
        "P1 job with ATTR_POLICY_DEFERRED=true must not be immediately leasable");

    ReconcileJobStore.QueueStats stats = store.queueStats();
    assertTrue(
        stats.admissionDeferredByClass.getOrDefault(StatsPriorityClass.P1_FRESHNESS, 0L) >= 1L,
        "admissionDeferredByClass[P1_FRESHNESS] must be >= 1 when ATTR_POLICY_DEFERRED is set");
  }

  @Test
  void policyDeferredAttributeAbsentDoesNotAffectNormalAdmission() {
    var store = new InMemoryReconcileJobStore();

    ReconcileExecutionPolicy normalP1Policy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS,
            "lane-p1-normal",
            Map.of("enqueue_reason", "test"),
            0L);

    String jobId =
        store.enqueue(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-p1-normal"),
            normalP1Policy,
            "");

    var lease = store.leaseNext();
    assertTrue(lease.isPresent(), "P1 without ATTR_POLICY_DEFERRED must be immediately leasable");
    assertEquals(jobId, lease.get().jobId);
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
      return;
    }
    System.setProperty(key, value);
  }
}
