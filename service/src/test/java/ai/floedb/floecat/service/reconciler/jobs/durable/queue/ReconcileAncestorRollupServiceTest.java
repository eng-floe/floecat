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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcileAncestorRollupServiceTest {
  @Test
  void snapshotRollupUsesFinalizedManifestArtifactCounts() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob parent =
        job("snapshot-plan", ReconcileJobKind.PLAN_SNAPSHOT, "", "JS_WAITING");
    parent.childrenFinalized = true;
    parent.expectedDirectChildren = 2L;
    parent.snapshotsProcessed = 2L;
    parent.statsProcessed = 54L;
    parent.indexesProcessed = 54L;

    StoredReconcileJob fileGroup =
        job("file-group", ReconcileJobKind.EXEC_FILE_GROUP, parent.jobId, "JS_SUCCEEDED");
    fileGroup.statsProcessed = 18L;
    fileGroup.indexesProcessed = 18L;
    fileGroup.plannedFileGroups = 1L;
    fileGroup.plannedFiles = 18L;
    fileGroup.completedFileGroups = 1L;
    fileGroup.completedFiles = 18L;

    StoredReconcileJob finalizer =
        job("finalizer", ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, parent.jobId, "JS_SUCCEEDED");
    finalizer.snapshotsProcessed = 1L;
    finalizer.statsProcessed = 9L;
    finalizer.indexesProcessed = 7L;

    var projection = rollups.recomputeParentProjection(parent, List.of(fileGroup, finalizer));

    assertEquals("JS_SUCCEEDED", projection.state());
    assertEquals(1L, projection.snapshotsProcessed());
    assertEquals(9L, projection.statsProcessed());
    assertEquals(7L, projection.indexesProcessed());
  }

  @Test
  void snapshotRollupShowsFileGroupProgressUntilFinalizerSucceeds() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob parent =
        job("snapshot-plan", ReconcileJobKind.PLAN_SNAPSHOT, "", "JS_WAITING");
    parent.childrenFinalized = true;
    parent.expectedDirectChildren = 3L;

    StoredReconcileJob fileGroupOne =
        job("file-group-1", ReconcileJobKind.EXEC_FILE_GROUP, parent.jobId, "JS_SUCCEEDED");
    fileGroupOne.statsProcessed = 6L;
    fileGroupOne.indexesProcessed = 2L;
    StoredReconcileJob fileGroupTwo =
        job("file-group-2", ReconcileJobKind.EXEC_FILE_GROUP, parent.jobId, "JS_SUCCEEDED");
    fileGroupTwo.statsProcessed = 8L;
    fileGroupTwo.indexesProcessed = 3L;
    StoredReconcileJob finalizer =
        job("finalizer", ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, parent.jobId, "JS_RUNNING");

    var projection =
        rollups.recomputeParentProjection(parent, List.of(fileGroupOne, fileGroupTwo, finalizer));

    assertEquals(14L, projection.statsProcessed());
    assertEquals(5L, projection.indexesProcessed());
  }

  @Test
  void connectorRollupCountsOneTablePerPlanTableChild() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob connector =
        job("connector", ReconcileJobKind.PLAN_CONNECTOR, "", "JS_WAITING");
    connector.childrenFinalized = true;
    connector.expectedDirectChildren = 1L;

    StoredReconcileJob table =
        job("table", ReconcileJobKind.PLAN_TABLE, connector.jobId, "JS_WAITING");
    table.tablesScanned = 30L;
    table.tablesChanged = 30L;
    table.snapshotsProcessed = 100L;
    table.statsProcessed = 100L;
    table.indexesProcessed = 100L;

    var projection = rollups.recomputeParentProjection(connector, List.of(table));

    assertEquals(1L, projection.tablesScanned());
    assertEquals(1L, projection.tablesChanged());
    assertEquals(100L, projection.snapshotsProcessed());
    assertEquals(100L, projection.statsProcessed());
    assertEquals(100L, projection.indexesProcessed());
  }

  @Test
  void tableRollupUsesExpectedSnapshotChildrenInsteadOfChildSnapshotPayloads() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob table = job("table", ReconcileJobKind.PLAN_TABLE, "connector", "JS_WAITING");
    table.childrenFinalized = true;
    table.expectedDirectChildren = 474L;
    table.snapshotsProcessed = 474L;
    table.statsProcessed = 474L;
    table.indexesProcessed = 474L;

    StoredReconcileJob snapshotOne =
        job("snapshot-1", ReconcileJobKind.PLAN_SNAPSHOT, table.jobId, "JS_SUCCEEDED");
    snapshotOne.snapshotsProcessed = 300L;
    snapshotOne.statsProcessed = 300L;
    snapshotOne.indexesProcessed = 300L;
    StoredReconcileJob snapshotTwo =
        job("snapshot-2", ReconcileJobKind.PLAN_SNAPSHOT, table.jobId, "JS_SUCCEEDED");
    snapshotTwo.snapshotsProcessed = 300L;
    snapshotTwo.statsProcessed = 300L;
    snapshotTwo.indexesProcessed = 300L;

    var projection = rollups.recomputeParentProjection(table, List.of(snapshotOne, snapshotTwo));

    assertEquals(474L, projection.snapshotsProcessed());
    assertEquals(600L, projection.statsProcessed());
    assertEquals(600L, projection.indexesProcessed());
  }

  @Test
  void tableRollupCountsSnapshotChildrenOnceWhenNoExpectedCountExists() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob table = job("table", ReconcileJobKind.PLAN_TABLE, "connector", "JS_WAITING");

    StoredReconcileJob snapshotOne =
        job("snapshot-1", ReconcileJobKind.PLAN_SNAPSHOT, table.jobId, "JS_SUCCEEDED");
    snapshotOne.snapshotsProcessed = 300L;
    StoredReconcileJob snapshotTwo =
        job("snapshot-2", ReconcileJobKind.PLAN_SNAPSHOT, table.jobId, "JS_SUCCEEDED");
    snapshotTwo.snapshotsProcessed = 300L;

    var projection = rollups.recomputeParentProjection(table, List.of(snapshotOne, snapshotTwo));

    assertEquals(2L, projection.snapshotsProcessed());
  }

  @Test
  void cancellingParentBecomesCancelledWhenFinalizedChildrenAreTerminal() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob parent =
        job("connector", ReconcileJobKind.PLAN_CONNECTOR, "", "JS_CANCELLING");
    parent.childrenFinalized = true;
    parent.expectedDirectChildren = 2L;

    StoredReconcileJob cancelled =
        job("cancelled-table", ReconcileJobKind.PLAN_TABLE, parent.jobId, "JS_CANCELLED");
    StoredReconcileJob succeeded =
        job("succeeded-table", ReconcileJobKind.PLAN_TABLE, parent.jobId, "JS_SUCCEEDED");

    var projection = rollups.recomputeParentProjection(parent, List.of(cancelled, succeeded));

    assertEquals("JS_CANCELLED", projection.state());
  }

  @Test
  void cancellingParentStaysCancellingWhileChildIsActive() {
    ReconcileAncestorRollupService rollups = rollups();
    StoredReconcileJob parent =
        job("connector", ReconcileJobKind.PLAN_CONNECTOR, "", "JS_CANCELLING");
    parent.childrenFinalized = true;
    parent.expectedDirectChildren = 1L;

    StoredReconcileJob running =
        job("running-table", ReconcileJobKind.PLAN_TABLE, parent.jobId, "JS_RUNNING");

    var projection = rollups.recomputeParentProjection(parent, List.of(running));

    assertEquals("JS_CANCELLING", projection.state());
  }

  private static ReconcileAncestorRollupService rollups() {
    ReconcileAncestorRollupService rollups = new ReconcileAncestorRollupService();
    rollups.bind(null, new ReconcileJobProjector(), (record, tolerateDrift, nowMs) -> false);
    return rollups;
  }

  private static StoredReconcileJob job(
      String jobId, ReconcileJobKind jobKind, String parentJobId, String state) {
    StoredReconcileJob job = new StoredReconcileJob();
    job.accountId = "acct-1";
    job.connectorId = "conn-1";
    job.jobId = jobId;
    job.jobKind = jobKind.name();
    job.parentJobId = parentJobId;
    job.state = state;
    job.message = state;
    job.startedAtMs = 100L;
    job.finishedAtMs = "JS_SUCCEEDED".equals(state) ? 200L : 0L;
    return job;
  }
}
