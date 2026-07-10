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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobDetailLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class ReconcileJobProjectorTest {
  @Test
  void projectSelfPublicJobDoesNotLoadSnapshotPayloadForParentCapableJobs() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-1";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.PLAN_SNAPSHOT.name();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.snapshotTaskTableId = "table-1";
    stored.snapshotTaskSnapshotId = 42L;
    stored.snapshotTaskFileGroupPlanRecorded = true;
    stored.snapshotPlanBlobUri =
        "/accounts/acct-1/reconcile/jobs/job-1/snapshot-plan/"
            + "snapshot-plan-47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU.json";
    stored.plannedFileGroups = 3L;
    stored.plannedFiles = 9L;
    stored.state = "JS_WAITING";
    stored.message = "Waiting on child work";

    ReconcileJobProjector.ProjectedPublicJob projected =
        assertDoesNotThrow(() -> projector.projectSelfPublicJob(stored, true));

    assertEquals("JS_WAITING", projected.state());
    assertEquals(3L, projected.projection().plannedFileGroups());
    assertEquals(9L, projected.projection().plannedFiles());
  }

  @Test
  void projectSelfPublicJobForRollupDoesNotLoadMissingExecFileGroupPayloads() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-2";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.EXEC_FILE_GROUP.name();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.fileGroupFileCount = 2;
    stored.fileGroupResultBlobUri =
        "/accounts/acct-1/reconcile/jobs/job-2/result-file-group-result-missing.json";
    stored.indexesProcessed = 1L;
    stored.plannedFileGroups = 1L;
    stored.plannedFiles = 2L;
    stored.completedFileGroups = 1L;
    stored.completedFiles = 1L;
    stored.failedFiles = 1L;
    stored.state = "JS_SUCCEEDED";
    stored.message = "Succeeded";

    ReconcileJobProjector.ProjectedPublicJob projected =
        assertDoesNotThrow(() -> projector.projectSelfPublicJobForRollup(stored));

    assertEquals("JS_SUCCEEDED", projected.state());
    assertEquals(1L, projected.projection().indexesProcessed());
    assertEquals(1L, projected.projection().completedFileGroups());
    assertEquals(1L, projected.projection().completedFiles());
    assertEquals(1L, projected.projection().failedFiles());
  }

  @Test
  void toPublicJobDegradesGracefullyWhenSnapshotManifestBlobIsMissing() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-3";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.PLAN_SNAPSHOT.name();
    stored.captureMode = CaptureMode.METADATA_AND_CAPTURE.name();
    stored.executionClass = ReconcileExecutionPolicy.defaults().executionClass().name();
    stored.executionLane = ReconcileExecutionPolicy.defaults().lane();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.snapshotTaskTableId = "table-1";
    stored.snapshotTaskSnapshotId = 42L;
    stored.snapshotTaskSourceNamespace = "db";
    stored.snapshotTaskSourceTable = "orders";
    stored.snapshotTaskFileGroupPlanRecorded = true;
    stored.snapshotPlanBlobUri =
        "/accounts/acct-1/reconcile/jobs/job-3/snapshot-plan/"
            + "snapshot-plan-47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU.json";
    stored.plannedFileGroups = 3L;
    stored.state = "JS_QUEUED";
    stored.message = "Queued";

    ReconcileJob job = assertDoesNotThrow(() -> projector.toPublicJob(stored, true));

    assertEquals(stored.snapshotPlanBlobUri, job.snapshotTask.fileGroupPlanBlobUri());
    assertEquals(3, job.snapshotTask.fileGroupCount());
    assertTrue(job.snapshotTask.fileGroups().isEmpty());
  }

  @Test
  void toPublicJobDegradesGracefullyWhenFileGroupResultBlobIsMissing() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-4";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.EXEC_FILE_GROUP.name();
    stored.captureMode = CaptureMode.METADATA_AND_CAPTURE.name();
    stored.executionClass = ReconcileExecutionPolicy.defaults().executionClass().name();
    stored.executionLane = ReconcileExecutionPolicy.defaults().lane();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.fileGroupPlanId = "plan-1";
    stored.fileGroupGroupId = "group-1";
    stored.fileGroupTableId = "table-1";
    stored.fileGroupSnapshotId = 42L;
    stored.fileGroupFileCount = 2;
    stored.fileGroupResultBlobUri =
        "/accounts/acct-1/reconcile/jobs/job-4/result-file-group-result-missing.json";
    stored.plannedFileGroups = 1L;
    stored.plannedFiles = 2L;
    stored.completedFiles = 1L;
    stored.failedFiles = 1L;
    stored.state = "JS_SUCCEEDED";
    stored.message = "Succeeded";

    ReconcileJob job = assertDoesNotThrow(() -> projector.toPublicJob(stored, true));

    assertEquals("plan-1", job.fileGroupTask.planId());
    assertEquals("group-1", job.fileGroupTask.groupId());
    assertEquals(2, job.fileGroupTask.fileCount());
    assertTrue(job.fileGroupTask.filePaths().isEmpty());
    assertTrue(job.fileGroupTask.fileResults().isEmpty());
  }

  @Test
  void toPublicJobSummaryUsesProjectedAggregatesForParentSummaries() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-5";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.PLAN_CONNECTOR.name();
    stored.captureMode = CaptureMode.METADATA_AND_CAPTURE.name();
    stored.executionClass = ReconcileExecutionPolicy.defaults().executionClass().name();
    stored.executionLane = ReconcileExecutionPolicy.defaults().lane();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.state = "JS_WAITING";
    stored.message = "Waiting on child work";
    stored.tablesScanned = 19L;
    stored.tablesChanged = 19L;
    stored.snapshotsProcessed = 19L;

    StoredReconcileJobProjection projection =
        new StoredReconcileJobProjection(
            "acct-1",
            "job-5",
            1L,
            "JS_WAITING",
            "Waiting on child work",
            100L,
            0L,
            38L,
            38L,
            0L,
            0L,
            0L,
            38L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            "",
            true);

    ReconcileJob job = projector.toPublicJobSummary(stored, projection);

    assertEquals("JS_WAITING", job.state);
    assertEquals(38L, job.tablesScanned);
    assertEquals(38L, job.tablesChanged);
    assertEquals(38L, job.snapshotsProcessed);
  }

  @Test
  void toPublicJobSummaryUsesFreshProjectionOverInflatedCanonicalAggregates() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());
    ReconcileJobDetailLoader detailLoader = new ReconcileJobDetailLoader();
    detailLoader.bind(payloadStore);

    ReconcileJobProjector projector = new ReconcileJobProjector();
    projector.bind(detailLoader);

    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = "job-7";
    stored.accountId = "acct-1";
    stored.connectorId = "conn-1";
    stored.jobKind = ReconcileJobKind.PLAN_SNAPSHOT.name();
    stored.captureMode = CaptureMode.METADATA_AND_CAPTURE.name();
    stored.executionClass = ReconcileExecutionPolicy.defaults().executionClass().name();
    stored.executionLane = ReconcileExecutionPolicy.defaults().lane();
    stored.definition = StoredJobDefinition.of(null, null, null);
    stored.state = "JS_SUCCEEDED";
    stored.message = "Succeeded";
    stored.snapshotsProcessed = 2L;
    stored.statsProcessed = 54L;
    stored.indexesProcessed = 54L;

    StoredReconcileJobProjection projection =
        new StoredReconcileJobProjection(
            "acct-1",
            "job-7",
            1L,
            "JS_SUCCEEDED",
            "Succeeded",
            100L,
            200L,
            0L,
            0L,
            0L,
            0L,
            0L,
            1L,
            27L,
            27L,
            1L,
            18L,
            1L,
            0L,
            18L,
            0L,
            "",
            true);

    ReconcileJob job = projector.toPublicJobSummary(stored, projection);

    assertEquals(1L, job.snapshotsProcessed);
    assertEquals(27L, job.statsProcessed);
    assertEquals(27L, job.indexesProcessed);
  }
}
