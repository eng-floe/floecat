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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedSnapshotFinalizeInputServiceTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String FINALIZE_JOB_ID = "finalize-job";
  private static final String PARENT_JOB_ID = "parent-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  private LeasedSnapshotFinalizeInputService service;
  private SnapshotFinalizeChildStateService childStateService;
  private ReconcileJobStore jobs;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeInputService();
    childStateService = new SnapshotFinalizeChildStateService();
    jobs = mock(ReconcileJobStore.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.childStateService = childStateService;
    childStateService.jobs = jobs;
    when(principal.getCorrelationId()).thenReturn("corr");
  }

  @Test
  void resolveRejectsPendingChildren() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/file-1.parquet"));
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(FINALIZE_JOB_ID))
        .thenReturn(
            Optional.of(
                finalizeJob(
                    "JS_RUNNING",
                    true,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1))));
    when(jobs.childJobsPage(ACCOUNT_ID, PARENT_JOB_ID, 200, ""))
        .thenReturn(
            new ReconcileJobStore.ReconcileJobPage(
                List.of(childJob("JS_RUNNING", plannedGroup, ReconcileFileGroupTask.empty())), ""));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.resolve(principal, FINALIZE_JOB_ID, LEASE_EPOCH));

    assertEquals(
        "FAILED_PRECONDITION: snapshot finalization waiting for snapshot file groups 0/1"
            + " pending=[plan-1/group-1(JS_RUNNING)]",
        error.getMessage());
  }

  @Test
  void resolveAllowsIncrementalWhenChildrenAreReady() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/file-1.parquet"));
    ReconcileFileGroupTask persistedGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            TABLE_ID,
            SNAPSHOT_ID,
            1,
            "/accounts/acct/reconcile/jobs/group-1/file-group-stats/result.json",
            1,
            List.of("s3://bucket/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 1L)));
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(FINALIZE_JOB_ID))
        .thenReturn(
            Optional.of(
                finalizeJob(
                    "JS_RUNNING",
                    false,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1))));
    when(jobs.childJobsPage(ACCOUNT_ID, PARENT_JOB_ID, 200, ""))
        .thenReturn(
            new ReconcileJobStore.ReconcileJobPage(
                List.of(childJob("JS_SUCCEEDED", plannedGroup, persistedGroup)), ""));

    var payload = service.resolve(principal, FINALIZE_JOB_ID, LEASE_EPOCH);

    assertEquals(FINALIZE_JOB_ID, payload.jobId());
    assertEquals(1, payload.completedGroups().size());
    assertEquals(
        "/accounts/acct/reconcile/jobs/group-1/file-group-stats/result.json",
        payload.completedGroups().getFirst().fileStatsBlobUri());
    assertFalse(payload.completedGroups().getFirst().planId().isBlank());
  }

  private static ReconcileJobStore.ReconcileJob finalizeJob(
      String state, boolean fullRescan, ReconcileSnapshotTask snapshotTask) {
    return new ReconcileJobStore.ReconcileJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        state,
        "",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        fullRescan,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        PARENT_JOB_ID);
  }

  private static ReconcileJobStore.ReconcileJob childJob(
      String state, ReconcileFileGroupTask taskRef, ReconcileFileGroupTask persistedTask) {
    return new ReconcileJobStore.ReconcileJob(
        "child-job",
        ACCOUNT_ID,
        "connector",
        state,
        "",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        true,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        persistedTask.isEmpty() ? taskRef : persistedTask,
        PARENT_JOB_ID);
  }
}
