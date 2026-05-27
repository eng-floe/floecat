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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileProjectionAccumulator;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import org.junit.jupiter.api.Test;

class ReconcileAncestorRollupServiceTest {
  @Test
  void foldChildSumsPlannedFileCountsAcrossChildren() {
    ReconcileJobProjector projector = mock(ReconcileJobProjector.class);
    when(projector.isParentCapable(ReconcileJobKind.PLAN_SNAPSHOT)).thenReturn(true);
    StoredReconcileJob firstChild = child("child-1", 1L, 4L);
    StoredReconcileJob secondChild = child("child-2", 2L, 7L);
    when(projector.projectSelfPublicJobForRollup(firstChild)).thenReturn(projectedChild(1L, 4L));
    when(projector.projectSelfPublicJobForRollup(secondChild)).thenReturn(projectedChild(2L, 7L));

    ReconcileAncestorRollupService service = new ReconcileAncestorRollupService();
    service.bind(null, projector, null);

    StoredReconcileJob parent = parent();
    StoredReconcileProjectionAccumulator accumulator = service.foldChild(parent, null, firstChild);
    accumulator = service.foldChild(parent, accumulator, secondChild);

    assertEquals(3L, accumulator.plannedFileGroups());
    assertEquals(11L, accumulator.plannedFiles());
  }

  private static StoredReconcileJob parent() {
    StoredReconcileJob parent = new StoredReconcileJob();
    parent.jobId = "parent";
    parent.jobKind = ReconcileJobKind.PLAN_SNAPSHOT.name();
    return parent;
  }

  private static StoredReconcileJob child(String jobId, long plannedFileGroups, long plannedFiles) {
    StoredReconcileJob child = new StoredReconcileJob();
    child.jobId = jobId;
    child.jobKind = ReconcileJobKind.EXEC_FILE_GROUP.name();
    child.state = "JS_WAITING";
    child.plannedFileGroups = plannedFileGroups;
    child.plannedFiles = plannedFiles;
    return child;
  }

  private static ReconcileJobProjector.ProjectedPublicJob projectedChild(
      long plannedFileGroups, long plannedFiles) {
    return new ReconcileJobProjector.ProjectedPublicJob(
        "JS_WAITING",
        "Waiting",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        "",
        new ReconcileJobProjector.JobProjection(
            0L, plannedFileGroups, plannedFiles, 0L, 0L, 0L, 0L));
  }
}
