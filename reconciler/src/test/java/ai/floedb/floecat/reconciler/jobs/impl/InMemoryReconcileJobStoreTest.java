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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileWorkerAffinity;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  void enqueueDedupesOnceMatchingJobIsRunning() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    var lease = store.leaseNext().orElseThrow();

    String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(first, second);
    assertEquals(first, lease.jobId);
  }

  @Test
  void enqueueDoesNotDedupeAfterMatchingJobSucceeds() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    var lease = store.leaseNext().orElseThrow();
    store.markSucceeded(first, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertNotEquals(first, second);
  }

  @Test
  void enqueueDoesNotDedupeWhileMatchingJobIsCancelling() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(first, lease.leaseEpoch, System.currentTimeMillis(), "executor-1");
    store.cancel("acct", first, "stop");

    String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertNotEquals(first, second);
    assertEquals("JS_CANCELLING", store.get(first).orElseThrow().state);
  }

  @Test
  void enqueueDoesNotDedupeAfterMatchingJobFails() {
    String maxAttemptsKey = "floecat.reconciler.job-store.max-attempts";
    String previousMaxAttempts = System.getProperty(maxAttemptsKey);
    try {
      System.setProperty(maxAttemptsKey, "1");
      var store = new InMemoryReconcileJobStore();
      ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

      String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
      var lease = store.leaseNext().orElseThrow();
      store.markFailed(first, lease.leaseEpoch, System.currentTimeMillis(), "boom", 1, 0, 1, 0, 1);

      String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

      assertNotEquals(first, second);
    } finally {
      restoreProperty(maxAttemptsKey, previousMaxAttempts);
    }
  }

  @Test
  void enqueueDoesNotDedupeAfterMatchingJobIsCancelled() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    store.cancel("acct", first, "stop");

    String second = store.enqueue("acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertNotEquals(first, second);
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
  void storeStampsItsOwnCohortOnEnqueueAndLease() {
    // The store owns both stamps, so a caller cannot enqueue work its own deployment cannot lease.
    System.setProperty("floecat.reconciler.worker-affinity", "ci-branch");
    try {
      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueuePlan(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileExecutionPolicy.defaults(),
              "");

      var lease = store.leaseNext().orElseThrow();

      assertEquals(jobId, lease.jobId);
      assertEquals(
          ReconcileWorkerAffinity.of("ci-branch"),
          ReconcileWorkerAffinity.fromPolicy(lease.executionPolicy));
      // The cohort is not an executor pin.
      assertEquals("", lease.pinnedExecutorId);
    } finally {
      System.clearProperty("floecat.reconciler.worker-affinity");
    }
  }

  @Test
  void presentButEmptyAffinityPropertyLeavesAffinityDisabled() {
    // The property ships as ${FLOECAT_RECONCILER_WORKER_AFFINITY:}, so the default deployment
    // always presents it as an empty value. That must read as "disabled", not fail or stamp "".
    System.setProperty("floecat.reconciler.worker-affinity", "   ");
    try {
      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueuePlan(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileExecutionPolicy.defaults(),
              "");

      var lease = store.leaseNext().orElseThrow();

      assertEquals(jobId, lease.jobId);
      assertEquals(
          ReconcileWorkerAffinity.DISABLED,
          ReconcileWorkerAffinity.fromPolicy(lease.executionPolicy));
      assertFalse(
          lease.executionPolicy.attributes().containsKey(ReconcileWorkerAffinity.ATTRIBUTE));
    } finally {
      System.clearProperty("floecat.reconciler.worker-affinity");
    }
  }

  @Test
  void storeOverridesCallerSuppliedAffinity() {
    // A caller must not be able to place work into, or steal work from, another cohort by passing
    // the attribute itself. The store's own configuration always wins.
    System.setProperty("floecat.reconciler.worker-affinity", "ci-branch");
    try {
      var store = new InMemoryReconcileJobStore();
      String jobId =
          store.enqueuePlan(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileWorkerAffinity.of("steady-state")
                  .applyTo(ReconcileExecutionPolicy.defaults()),
              "");

      var lease = store.leaseNext().orElseThrow();

      assertEquals(jobId, lease.jobId);
      assertEquals(
          ReconcileWorkerAffinity.of("ci-branch"),
          ReconcileWorkerAffinity.fromPolicy(lease.executionPolicy));
    } finally {
      System.clearProperty("floecat.reconciler.worker-affinity");
    }
  }

  @Test
  void leaseRequestCohortMustMatchTheJobExactly() {
    ReconcileExecutionPolicy branch =
        ReconcileWorkerAffinity.of("ci-branch").applyTo(ReconcileExecutionPolicy.defaults());
    ReconcileExecutionPolicy legacy = ReconcileExecutionPolicy.defaults();
    ReconcileJobStore.LeaseRequest request =
        new ReconcileJobStore.LeaseRequest(
            null, null, Set.of(), EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR));

    assertTrue(
        request.withWorkerAffinity(ReconcileWorkerAffinity.of("ci-branch")).cohortMatches(branch));
    assertFalse(
        request
            .withWorkerAffinity(ReconcileWorkerAffinity.of("steady-state"))
            .cohortMatches(branch));
    // Neither direction of the legacy boundary may cross.
    assertFalse(request.cohortMatches(branch));
    assertFalse(
        request.withWorkerAffinity(ReconcileWorkerAffinity.of("ci-branch")).cohortMatches(legacy));
    assertTrue(request.cohortMatches(legacy));
  }

  @Test
  void deploymentLaneIsAnIndependentServerOwnedLeaseConstraint() {
    ReconcileJobStore.LeaseRequest request =
        new ReconcileJobStore.LeaseRequest(
            null,
            Set.of(ReconcileJobStore.LeaseRequest.anyLaneToken()),
            Set.of(),
            EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP));
    ReconcileExecutionPolicy configuredLane =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "ci-run", Map.of());
    ReconcileExecutionPolicy foreignLane =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "other-run", Map.of());

    ReconcileJobStore.LeaseRequest constrained = request.withDeploymentLane("ci-run");

    assertTrue(
        constrained.matches(configuredLane, "", ReconcileJobKind.EXEC_FILE_GROUP, "group-1"));
    assertFalse(constrained.matches(foreignLane, "", ReconcileJobKind.EXEC_FILE_GROUP, "group-1"));
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
  void adoptSnapshotPlanManifestUpdatesStoredJobPayload() {
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

    ReconcileSnapshotTask task =
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
                true)
            .withIndexPredecessor(
                new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
                    "generation-1", 7L, "/capture.pb", 9L));
    String manifestUri = store.persistSnapshotPlanManifest("acct", jobId, task);
    var lease = store.leaseNext().orElseThrow();
    assertTrue(store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, task, manifestUri, true));

    var job = store.get("acct", jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
    assertEquals(task.indexPredecessor(), job.snapshotTask.indexPredecessor());
  }

  @Test
  void snapshotOwnershipSurvivesWaitingUntilOwnerTerminates() {
    var store = new InMemoryReconcileJobStore();
    ReconcileSnapshotTask task = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    String first =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            task,
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    String second =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            task,
            ReconcileExecutionPolicy.defaults(),
            "parent-2",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(first, firstLease.jobId);
    store.markWaiting(
        first,
        firstLease.leaseEpoch,
        System.currentTimeMillis(),
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    assertTrue(store.leaseNext().isEmpty());

    store.cancel("acct", first, "cancel owner");
    assertEquals(second, store.leaseNext().orElseThrow().jobId);
  }

  @Test
  void snapshotOwnershipSurvivesRetryableFailure() {
    var store = new InMemoryReconcileJobStore();
    ReconcileSnapshotTask task = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    String first =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            task,
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    String second =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            task,
            ReconcileExecutionPolicy.defaults(),
            "parent-2",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    store.markFailed(
        first,
        firstLease.leaseEpoch,
        System.currentTimeMillis(),
        "retry owner",
        0L,
        0L,
        0L,
        0L,
        1L,
        0L,
        0L);

    assertTrue(store.leaseNext().isEmpty());
    assertEquals("JS_QUEUED", store.get("acct", second).orElseThrow().state);

    store.cancel("acct", first, "cancel owner");
    assertEquals(second, store.leaseNext().orElseThrow().jobId);
  }

  @Test
  void leasedSnapshotPinsIndexPredecessorOnlyOnce() {
    var store = new InMemoryReconcileJobStore();
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(),
            ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.of(
                List.of(),
                Set.of(
                    ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.Output
                        .PARQUET_PAGE_INDEX)));
    String jobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    var lease = store.leaseNext().orElseThrow();
    var firstPredecessor =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture-1.pb", 9L);
    var laterPredecessor =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-2", 8L, "/capture-2.pb", 10L);

    assertEquals(
        firstPredecessor,
        store
            .pinSnapshotIndexPredecessor(jobId, lease.leaseEpoch, firstPredecessor)
            .orElseThrow()
            .indexPredecessor());
    assertEquals(
        firstPredecessor,
        store
            .pinSnapshotIndexPredecessor(jobId, lease.leaseEpoch, laterPredecessor)
            .orElseThrow()
            .indexPredecessor());
  }

  @Test
  void completeFileGroupSuccessPublishesCompactChildResult() {
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

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "executor-1");
    assertTrue(
        store.completeFileGroupSuccess(
            jobId,
            lease.leaseEpoch,
            new ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor(
                1,
                "acct",
                "conn",
                "parent-1",
                jobId,
                "plan-1",
                "group-1",
                "table-1",
                55L,
                lease.leaseEpoch,
                "result-1",
                "/result.pb",
                100L,
                "sha256",
                1,
                1,
                0,
                0,
                2,
                1,
                "/stats/",
                1,
                "0".repeat(64),
                null,
                System.currentTimeMillis()),
            System.currentTimeMillis(),
            "done"));

    var page = store.childFileGroupResultDescriptorsPage("acct", "parent-1", 10, "");
    assertEquals(1, page.descriptors.size());
    assertEquals("/result.pb", page.descriptors.getFirst().payloadUri());
  }

  @Test
  void snapshotRollupUsesFinalizedManifestArtifactCounts() {
    var store = new InMemoryReconcileJobStore();
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-job",
            "");
    store.enqueueFileGroupExecution(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileFileGroupTask.of(
            snapshotJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");
    store.enqueueFileGroupExecution(
        "acct",
        "conn",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileFileGroupTask.of(
            snapshotJobId, "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet")),
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");
    String finalizerJobId =
        store.enqueueSnapshotFinalization(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");

    for (long statsProcessed : List.of(6L, 8L)) {
      var lease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
              .orElseThrow();
      assertTrue(
          store.applyLeaseOutcome(
              lease.jobId,
              lease.leaseEpoch,
              ReconcileJobStore.CompletionKind.SUCCEEDED,
              100L,
              "Succeeded",
              0L,
              0L,
              0L,
              0L,
              0L,
              0L,
              statsProcessed));
    }
    var finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();
    assertEquals(finalizerJobId, finalizerLease.jobId);
    assertTrue(store.beginSnapshotFinalizeCommit(finalizerJobId, finalizerLease.leaseEpoch));
    assertTrue(
        store.completeSnapshotFinalizeSuccess(
            finalizerJobId,
            finalizerLease.leaseEpoch,
            "result-1",
            "/capture-manifest.pb",
            100L,
            "sha256",
            2,
            2,
            9L,
            7L,
            List.of(),
            200L,
            "Succeeded"));
    assertTrue(
        store.completeSnapshotFinalizeSuccess(
            finalizerJobId,
            finalizerLease.leaseEpoch,
            "result-1",
            "/capture-manifest.pb",
            100L,
            "sha256",
            2,
            2,
            9L,
            7L,
            List.of(),
            300L,
            "Replayed"));
    assertFalse(
        store.completeSnapshotFinalizeSuccess(
            finalizerJobId,
            finalizerLease.leaseEpoch,
            "result-1",
            "/different-capture-manifest.pb",
            100L,
            "sha256",
            2,
            2,
            9L,
            7L,
            List.of(),
            300L,
            "Conflicting replay"));

    var snapshot = store.get("acct", snapshotJobId).orElseThrow();
    assertEquals(9L, snapshot.statsProcessed);
    assertEquals(7L, snapshot.indexesProcessed);
  }

  @Test
  void retryableSnapshotFinalizeFailureClearsAcceptedPublicationIntent() {
    var store = new InMemoryReconcileJobStore();
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "table-job",
            "");
    String finalizerJobId =
        store.enqueueSnapshotFinalization(
            "acct",
            "conn",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    var finalizerLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
            .orElseThrow();
    var intent =
        new ReconcileJobStore.SnapshotFinalizeCommitIntent(
            finalizerJobId,
            finalizerLease.leaseEpoch,
            "result-1",
            "/capture-manifest.pb",
            100L,
            "sha256",
            0,
            0,
            0L,
            0L);

    assertTrue(
        store.beginSnapshotFinalizeCommit(finalizerJobId, finalizerLease.leaseEpoch, intent));
    assertEquals(intent, store.snapshotFinalizeCommitIntent(finalizerJobId).orElseThrow());

    store.markFailed(
        finalizerJobId,
        finalizerLease.leaseEpoch,
        System.currentTimeMillis(),
        "publication failed",
        0L,
        0L,
        0L,
        0L,
        1L,
        0L,
        0L);

    assertTrue(store.snapshotFinalizeCommitIntent(finalizerJobId).isEmpty());
    assertTrue(store.pendingSnapshotFinalizeCommits(100, "").intents().isEmpty());
  }

  @Test
  void acceptedSnapshotFinalizePublishesAfterWorkerLeaseAndGraceExpire() throws Exception {
    String leaseKey = "floecat.reconciler.job-store.lease-ms";
    String reclaimKey = "floecat.reconciler.job-store.reclaim-interval-ms";
    String laneKey = "floecat.reconciler.execution-lane";
    String previousLease = System.getProperty(leaseKey);
    String previousReclaim = System.getProperty(reclaimKey);
    String previousLane = System.getProperty(laneKey);
    try {
      System.setProperty(leaseKey, "1000");
      System.setProperty(reclaimKey, "1000");
      var store = new InMemoryReconcileJobStore();
      String finalizerJobId =
          store.enqueueSnapshotFinalization(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
              ReconcileExecutionPolicy.defaults(),
              "snapshot-job",
              "");
      var lease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
              .orElseThrow();
      var intent =
          new ReconcileJobStore.SnapshotFinalizeCommitIntent(
              finalizerJobId,
              lease.leaseEpoch,
              "result-1",
              "/capture-manifest.pb",
              100L,
              "sha256",
              0,
              0,
              0L,
              0L);
      assertTrue(store.beginSnapshotFinalizeCommit(finalizerJobId, lease.leaseEpoch, intent));
      assertEquals(List.of(intent), store.pendingSnapshotFinalizeCommits(100, "").intents());

      System.setProperty(laneKey, "other-lane");
      store.init();
      assertTrue(store.pendingSnapshotFinalizeCommits(100, "").intents().isEmpty());
      restoreProperty(laneKey, previousLane);
      store.init();
      assertEquals(List.of(intent), store.pendingSnapshotFinalizeCommits(100, "").intents());

      Thread.sleep(1150L);

      assertTrue(store.getCompletionLeaseView(finalizerJobId, lease.leaseEpoch, true).isPresent());
      assertTrue(
          store.completeSnapshotFinalizeSuccess(
              finalizerJobId,
              lease.leaseEpoch,
              "result-1",
              "/capture-manifest.pb",
              100L,
              "sha256",
              0,
              0,
              0L,
              0L,
              List.of(),
              System.currentTimeMillis(),
              "published"));
      assertEquals("JS_SUCCEEDED", store.get(finalizerJobId).orElseThrow().state);
    } finally {
      restoreProperty(leaseKey, previousLease);
      restoreProperty(reclaimKey, previousReclaim);
      restoreProperty(laneKey, previousLane);
    }
  }

  @Test
  void snapshotFinalizeOwnershipWithoutIntentIsReclaimedAfterLeaseExpiry() throws Exception {
    String leaseKey = "floecat.reconciler.job-store.lease-ms";
    String reclaimKey = "floecat.reconciler.job-store.reclaim-interval-ms";
    String previousLease = System.getProperty(leaseKey);
    String previousReclaim = System.getProperty(reclaimKey);
    try {
      System.setProperty(leaseKey, "1000");
      System.setProperty(reclaimKey, "1000");
      var store = new InMemoryReconcileJobStore();
      String finalizerJobId =
          store.enqueueSnapshotFinalization(
              "acct",
              "conn",
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
              ReconcileExecutionPolicy.defaults(),
              "snapshot-job",
              "");
      var lease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
              .orElseThrow();
      assertTrue(store.beginSnapshotFinalizeCommit(finalizerJobId, lease.leaseEpoch));
      assertTrue(store.snapshotFinalizeCommitIntent(finalizerJobId).isEmpty());

      Thread.sleep(1150L);

      assertTrue(
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
              .isEmpty());
      assertEquals("JS_QUEUED", store.get(finalizerJobId).orElseThrow().state);
      var recovered =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)))
              .orElseThrow();
      assertEquals(finalizerJobId, recovered.jobId);
      assertNotEquals(lease.leaseEpoch, recovered.leaseEpoch);
    } finally {
      restoreProperty(leaseKey, previousLease);
      restoreProperty(reclaimKey, previousReclaim);
    }
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
  void adoptSnapshotPlanManifestRejectsImplicitCoverageForPlanSnapshotJobs() {
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
                store.adoptSnapshotPlanManifest(
                    jobId,
                    store.leaseNext().orElseThrow().leaseEpoch,
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
                                List.of("s3://bucket/data/file-1.parquet")))),
                    "",
                    true));

    assertFalse(error.getMessage().isBlank());
  }

  @Test
  void adoptSnapshotPlanManifestRejectsEmptyCoverageForPlanSnapshotJobs() {
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
                store.adoptSnapshotPlanManifest(
                    jobId,
                    store.leaseNext().orElseThrow().leaseEpoch,
                    ReconcileSnapshotTask.empty(),
                    "",
                    true));

    assertFalse(error.getMessage().isBlank());
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
