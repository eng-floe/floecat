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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.PlannedFileGroupPlanJob;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IdempotencyRepositoryImpl;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class LeasedPlannerWorkerServiceTest {
  private LeasedPlannerWorkerService service;
  private ReconcileJobStore jobs;
  private ConnectorRepository connectorRepo;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedPlannerWorkerService();
    jobs = mock(ReconcileJobStore.class);
    connectorRepo = mock(ConnectorRepository.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.idempotencyStore =
        new IdempotencyRepositoryImpl(new InMemoryPointerStore(), new InMemoryBlobStore());
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn("acct");
    when(jobs.adoptSnapshotPlanManifest(any(), any(), any(), any(), anyBoolean())).thenReturn(true);
    when(jobs.bulkEnqueue(any()))
        .thenAnswer(
            invocation -> {
              List<ReconcileJobStore.BulkEnqueueSpec> specs = invocation.getArgument(0);
              if (specs == null || specs.isEmpty()) {
                return new ReconcileJobStore.BulkEnqueueResult(List.of());
              }
              List<ReconcileJobStore.BulkEnqueueItemResult> items =
                  new java.util.ArrayList<>(specs.size());
              for (int index = 0; index < specs.size(); index++) {
                items.add(
                    new ReconcileJobStore.BulkEnqueueItemResult(index, "job-" + index, true, ""));
              }
              return new ReconcileJobStore.BulkEnqueueResult(items);
            });
  }

  @Test
  void resolvePlanConnectorPreservesPinnedExecutorId() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_RUNNING",
                    "",
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "",
                    ReconcileJobKind.PLAN_CONNECTOR,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    "")));

    var payload = service.resolvePlanConnector(principal, "job-1", "lease-1");

    assertEquals("remote-executor", payload.pinnedExecutorId());
  }

  @Test
  void resolvePlanConnectorUsesLeaseViewInsteadOfProjectedPublicState() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.get("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_QUEUED",
                    "Queued",
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_CONNECTOR,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));

    var payload = service.resolvePlanConnector(principal, "job-1", "lease-1");

    assertEquals("job-1", payload.jobId());
    verify(jobs).getLeaseView("job-1");
    verify(jobs, never()).get("job-1");
  }

  @Test
  void persistPlanSnapshotSuccessStoresExpandedSnapshotPlanBeforeEnqueueingReferences() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"));
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_RUNNING",
                    "",
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    ReconcileFileGroupTask staleDuplicateGroup =
        ReconcileFileGroupTask.of(
            "stale-plan",
            "stale-group",
            "table-1",
            55L,
            List.of("s3://bucket/data/stale-file.parquet"));
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    ReconcileSnapshotTask durableSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    ReconcileScope fileGroupScope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(new ReconcileCapturePolicy.Column("col_a", true, false)),
                java.util.Set.of(ReconcileCapturePolicy.Output.FILE_STATS)));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    stagePlanSnapshotChunk("job-1", "lease-1", 0, 1);
    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", submittedSnapshotTask, 1);

    assertEquals(true, accepted);
    InOrder inOrder = inOrder(jobs);
    inOrder.verify(jobs).getLeaseView("job-1");
    inOrder.verify(jobs).getCompletionLeaseView("job-1", "lease-1", true);
    inOrder
        .verify(jobs)
        .adoptSnapshotPlanManifest(
            eq("job-1"),
            eq("lease-1"),
            eq(durableSnapshotTask),
            eq(durableSnapshotTask.fileGroupPlanBlobUri()),
            eq(true));
    inOrder
        .verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
    verify(jobs, never())
        .enqueueFileGroupExecution(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    verify(jobs, never()).bulkEnqueue(any());
    verify(jobs, never())
        .enqueueSnapshotFinalization(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void persistPlanConnectorFailureMarksTerminalFailure() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_TERMINAL),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanConnectorFailure(
            principal,
            "job-1",
            "lease-1",
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .INTERNAL,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
                .TERMINAL,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass.NONE,
            "boom");

    assertTrue(accepted);
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.FAILED_TERMINAL),
            anyLong(),
            eq("boom"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableSuccessCarriesWorkerCountersIntoStoredJob() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_TABLE)));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED),
            anyLong(),
            eq("Planned 0 snapshot job(s)"),
            eq(1L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanTableSuccess(principal, "job-1", "lease-1", 1L, 1L, 0L, 0L, 0L, 0);

    assertTrue(accepted);
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED),
            anyLong(),
            eq("Planned 0 snapshot job(s)"),
            eq(1L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableSuccessCancelsWithoutEnqueueWhenCancellationRequested() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_TABLE)));
    when(jobs.isCancellationRequested("job-1")).thenReturn(true);
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            eq("Cancelled"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanTableSuccess(principal, "job-1", "lease-1", 1L, 1L, 0L, 1L, 0L, 0);

    assertTrue(accepted);
    verify(jobs, never())
        .enqueueSnapshotPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            eq("Cancelled"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableSuccessRejectsMissingDeclaredChunk() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_TABLE)));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistPlanTableSuccess(
                    principal, "job-1", "lease-1", 1L, 1L, 0L, 0L, 0L, 1));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("declared chunk index 0"));
    verify(jobs, never())
        .applyLeaseOutcome(
            any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong());
  }

  @Test
  void persistPlanSnapshotSuccessDoesNotCountPlannedSnapshotAsCompleted() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_RUNNING",
                    "",
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            eq("Snapshot plan recorded for db.events with 0 file group(s)"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", snapshotTask, 0);

    assertTrue(accepted);
    verify(jobs, never()).bulkEnqueue(any());
    verify(jobs)
        .enqueueSnapshotFinalization(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            eq(ReconcileScope.empty()),
            eq(snapshotTask),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            eq(""));
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            eq("Snapshot plan recorded for db.events with 0 file group(s)"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableFailureMarksWaitingForDependency() {
    when(jobs.renewLease("job-2", "lease-2")).thenReturn(true);
    when(jobs.getLeaseView("job-2"))
        .thenReturn(java.util.Optional.of(job("job-2", ReconcileJobKind.PLAN_TABLE)));
    when(jobs.applyLeaseOutcome(
            eq("job-2"),
            eq("lease-2"),
            eq(ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanTableFailure(
            principal,
            "job-2",
            "lease-2",
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .INTERNAL,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
                .RETRYABLE,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
                .DEPENDENCY_NOT_READY,
            "waiting");

    assertTrue(accepted);
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-2"),
            eq("lease-2"),
            eq(ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY),
            anyLong(),
            eq("waiting"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableFailureCancelsObsoleteMissingConnectorJob() {
    when(jobs.renewLease("job-2b", "lease-2b")).thenReturn(true);
    when(jobs.getLeaseView("job-2b"))
        .thenReturn(java.util.Optional.of(job("job-2b", ReconcileJobKind.PLAN_TABLE)));
    when(jobs.applyLeaseOutcome(
            eq("job-2b"),
            eq("lease-2b"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanTableFailure(
            principal,
            "job-2b",
            "lease-2b",
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .CONNECTOR_MISSING,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
                .RETRYABLE,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
                .TRANSIENT_ERROR,
            "getConnector failed: connector-1");

    assertTrue(accepted);
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-2b"),
            eq("lease-2b"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            eq("getConnector failed: connector-1"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableFailureReturnsFalseWhenObsoleteCancelDoesNotCommit() {
    when(jobs.renewLease("job-2c", "lease-2c")).thenReturn(true);
    when(jobs.getLeaseView("job-2c"))
        .thenReturn(java.util.Optional.of(job("job-2c", ReconcileJobKind.PLAN_TABLE)));
    when(jobs.applyLeaseOutcome(
            eq("job-2c"),
            eq("lease-2c"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(false);

    boolean accepted =
        service.persistPlanTableFailure(
            principal,
            "job-2c",
            "lease-2c",
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .CONNECTOR_MISSING,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
                .RETRYABLE,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
                .TRANSIENT_ERROR,
            "getConnector failed: connector-1");

    assertTrue(!accepted);
  }

  @Test
  void persistPlanSnapshotFailureMarksRetryableFailure() {
    when(jobs.renewLease("job-3", "lease-3")).thenReturn(true);
    when(jobs.getLeaseView("job-3"))
        .thenReturn(java.util.Optional.of(job("job-3", ReconcileJobKind.PLAN_SNAPSHOT)));
    when(jobs.applyLeaseOutcome(
            eq("job-3"),
            eq("lease-3"),
            eq(ReconcileJobStore.CompletionKind.FAILED_RETRYABLE),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanSnapshotFailure(
            principal,
            "job-3",
            "lease-3",
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .INTERNAL,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
                .RETRYABLE,
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
                .TRANSIENT_ERROR,
            "retry");

    assertTrue(accepted);
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-3"),
            eq("lease-3"),
            eq(ReconcileJobStore.CompletionKind.FAILED_RETRYABLE),
            anyLong(),
            eq("retry"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanSnapshotSuccessPreservesDirectStatsCompletionMode() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_RUNNING",
                    "",
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    ReconcileSnapshotTask directSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "",
            0,
            9,
            "/accounts/acct/reconcile/jobs/job-1/direct-stats/stats.json",
            7);

    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", directSnapshotTask, 0);

    assertTrue(accepted);
    InOrder inOrder = inOrder(jobs);
    inOrder
        .verify(jobs)
        .adoptSnapshotPlanManifest(
            eq("job-1"), eq("lease-1"), eq(directSnapshotTask), eq(""), eq(true));
    inOrder
        .verify(jobs)
        .enqueueSnapshotFinalization(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            eq(ReconcileScope.empty()),
            eq(directSnapshotTask),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            eq(""));
    inOrder
        .verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
    verify(jobs)
        .adoptSnapshotPlanManifest(
            eq("job-1"), eq("lease-1"), eq(directSnapshotTask), eq(""), eq(true));
  }

  @Test
  void persistPlanSnapshotSuccessRejectsInlineFileGroupsWithoutPersistedManifest() {
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(fullGroup),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "",
            1);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_SNAPSHOT)));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);
    io.grpc.StatusRuntimeException error =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
            () ->
                service.persistPlanSnapshotSuccess(
                    principal, "job-1", "lease-1", submittedSnapshotTask, 0));

    verify(jobs, never()).adoptSnapshotPlanManifest(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void persistPlanSnapshotSuccessDoesNotEnqueueDuplicateExistingFileGroupChild() {
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_SNAPSHOT)));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    submittedSnapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);

    stagePlanSnapshotChunk("job-1", "lease-1", 0, 1);
    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", submittedSnapshotTask, 1);

    assertTrue(accepted);
    verify(jobs, never())
        .enqueueFileGroupExecution(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    verify(jobs, never()).bulkEnqueue(any());
    verify(jobs, never())
        .enqueueSnapshotFinalization(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void persistPlanSnapshotFileGroupChunkFailsWhenRequiredFileGroupEnqueueFails() {
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_RUNNING",
                    "",
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    submittedSnapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.adoptSnapshotPlanManifest(
            eq("job-1"),
            eq("lease-1"),
            eq(submittedSnapshotTask),
            eq(submittedSnapshotTask.fileGroupPlanBlobUri()),
            eq(true)))
        .thenReturn(true);
    when(jobs.bulkEnqueue(any()))
        .thenReturn(
            new ReconcileJobStore.BulkEnqueueResult(
                List.of(new ReconcileJobStore.BulkEnqueueItemResult(0, "", false, "boom"))));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                service.persistPlanSnapshotFileGroupChunk(
                    principal,
                    "job-1",
                    "lease-1",
                    submittedSnapshotTask,
                    SubmitLeasedPlanSnapshotResultRequest.Chunk.newBuilder()
                        .setChunkIndex(0)
                        .addFileGroupJobs(PlannedFileGroupPlanJob.getDefaultInstance())
                        .build(),
                    List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup))));

    assertTrue(error.getMessage().contains("snapshot file-group child enqueue failed"));
    verify(jobs, never())
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            any(),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
    verify(jobs, never())
        .enqueueSnapshotFinalization(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void persistPlanSnapshotSuccessReturnsTrueWhenMatchingPlanWasAlreadyAdopted() {
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_SUCCEEDED",
                    "Succeeded",
                    1L,
                    2L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    submittedSnapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));

    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", submittedSnapshotTask, 0);

    assertTrue(accepted);
    verify(jobs, never()).getCompletionLeaseView(any(), any(), anyBoolean());
    verify(jobs, never()).adoptSnapshotPlanManifest(any(), any(), any(), any(), anyBoolean());
    verify(jobs, never()).bulkEnqueue(any());
  }

  @Test
  void persistPlanSnapshotSuccessTreatsPostCommitRetryAsIdempotent() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    ReconcileJobStore.ReconcileJob activeJob =
        new ReconcileJobStore.ReconcileJob(
            "job-1",
            "acct",
            "connector-1",
            "JS_RUNNING",
            "",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "remote-executor",
            "remote_snapshot_planner_worker",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            "parent-1");
    ReconcileJobStore.ReconcileJob succeededJob =
        new ReconcileJobStore.ReconcileJob(
            "job-1",
            "acct",
            "connector-1",
            "JS_SUCCEEDED",
            "Succeeded",
            1L,
            2L,
            0L,
            0L,
            0L,
            0L,
            0L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "remote-executor",
            "remote_snapshot_planner_worker",
            ReconcileJobKind.PLAN_SNAPSHOT,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            snapshotTask,
            ReconcileFileGroupTask.empty(),
            "parent-1");
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(activeJob), java.util.Optional.of(succeededJob));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(false);

    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", snapshotTask, 0);

    assertTrue(accepted);
    verify(jobs)
        .adoptSnapshotPlanManifest(eq("job-1"), eq("lease-1"), eq(snapshotTask), eq(""), eq(true));
    verify(jobs, never()).bulkEnqueue(any());
    verify(jobs)
        .enqueueSnapshotFinalization(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            eq(ReconcileScope.empty()),
            eq(snapshotTask),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            eq(""));
  }

  @Test
  void persistPlanConnectorSuccessReturnsFalseWhenLeaseOutcomeRejected() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.bulkEnqueueAndApplyLeaseOutcome(
            any(),
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(false);

    boolean accepted =
        service.persistPlanConnectorSuccess(
            principal,
            "job-1",
            "lease-1",
            List.of(
                new LeasedPlannerWorkerService.PlannedTableJob(
                    ReconcileScope.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                        "db", "orders", "orders-id", "orders"))),
            List.of());

    assertEquals(false, accepted);
    verify(jobs, never())
        .enqueueTablePlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void persistPlanConnectorSuccessBulkEnqueuesUnpinnedTableAndViewChildren() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.bulkEnqueueAndApplyLeaseOutcome(
            any(),
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            eq("Planned 1 table job(s) and 1 view job(s)"),
            eq(1L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L)))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanConnectorSuccess(
            principal,
            "job-1",
            "lease-1",
            List.of(
                new LeasedPlannerWorkerService.PlannedTableJob(
                    ReconcileScope.of(List.of(), "orders"),
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                        "db", "orders", "orders-id", "orders"))),
            List.of(
                new LeasedPlannerWorkerService.PlannedViewJob(
                    ReconcileScope.of(List.of(), "orders_view"),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.of(
                        "db", "orders_view", "orders-view-id", "orders_view"))));

    assertTrue(accepted);
    verify(jobs)
        .bulkEnqueueAndApplyLeaseOutcome(
            argThat(
                specs ->
                    specs != null
                        && specs.size() == 2
                        && specs.stream().allMatch(spec -> spec.parentJobId.equals("job-1"))
                        && specs.stream().allMatch(spec -> spec.pinnedExecutorId.isBlank())
                        && specs.stream()
                            .anyMatch(
                                spec ->
                                    spec.jobKind == ReconcileJobKind.PLAN_TABLE
                                        && spec.tableTask.destinationTableId().equals("orders-id"))
                        && specs.stream()
                            .anyMatch(
                                spec ->
                                    spec.jobKind == ReconcileJobKind.PLAN_VIEW
                                        && spec.viewTask
                                            .destinationViewId()
                                            .equals("orders_view"))),
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING),
            anyLong(),
            eq("Planned 1 table job(s) and 1 view job(s)"),
            eq(1L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
    verify(jobs, never())
        .enqueueTablePlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    verify(jobs, never())
        .enqueueViewPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void persistPlanConnectorSuccessCancelsDeletedConnectorWithoutChildFanout() {
    service.connectorRepo = connectorRepo;
    when(connectorRepo.existsById(any())).thenReturn(false);
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.applyLeaseOutcome(
            any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanConnectorSuccess(
            principal,
            "job-1",
            "lease-1",
            List.of(
                new LeasedPlannerWorkerService.PlannedTableJob(
                    ReconcileScope.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                        "db", "orders", "orders-id", "orders"))),
            List.of());

    assertTrue(accepted);
    verify(connectorRepo).existsById(activeConnectorId());
    verify(jobs, never())
        .bulkEnqueueAndApplyLeaseOutcome(
            any(), any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong(), anyLong());
    verify(jobs)
        .applyLeaseOutcome(
            eq("job-1"),
            eq("lease-1"),
            eq(ReconcileJobStore.CompletionKind.CANCELLED),
            anyLong(),
            eq("connector deleted: connector-1"),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanConnectorSuccessReturnsFalseWhenDeleteCancelDoesNotCommit() {
    service.connectorRepo = connectorRepo;
    when(connectorRepo.existsById(any())).thenReturn(false);
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));
    when(jobs.applyLeaseOutcome(
            any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong()))
        .thenReturn(false);

    boolean accepted =
        service.persistPlanConnectorSuccess(
            principal,
            "job-1",
            "lease-1",
            List.of(
                new LeasedPlannerWorkerService.PlannedTableJob(
                    ReconcileScope.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                        "db", "orders", "orders-id", "orders"))),
            List.of());

    assertTrue(!accepted);
    verify(connectorRepo).existsById(activeConnectorId());
    verify(jobs, never())
        .bulkEnqueueAndApplyLeaseOutcome(
            any(), any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong(), anyLong());
  }

  @Test
  void persistPlanSnapshotSuccessCancelsDeletedConnectorBeforeLoadingPlanManifest() {
    service.connectorRepo = connectorRepo;
    ReconcileSnapshotTask submittedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/job-1/snapshot-plan/plan.json",
            1);
    when(connectorRepo.existsById(any())).thenReturn(false);
    when(jobs.getLeaseView("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_SNAPSHOT)));
    when(jobs.getCompletionLeaseView("job-1", "lease-1", true))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "lease-1",
                    "remote-executor",
                    "remote_snapshot_planner_worker",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    submittedSnapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "parent-1")));
    when(jobs.applyLeaseOutcome(
            any(), any(), any(), anyLong(), any(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong()))
        .thenReturn(true);

    boolean accepted =
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", submittedSnapshotTask, 0);

    assertTrue(accepted);
    verify(connectorRepo).existsById(activeConnectorId());
    verify(jobs, never()).bulkEnqueue(any());
    verify(jobs, never())
        .enqueueSnapshotFinalization(
            any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  private static ReconcileJobStore.ReconcileJob job(String jobId, ReconcileJobKind jobKind) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        "connector-1",
        "JS_RUNNING",
        "",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "remote-executor",
        "remote_snapshot_planner_worker",
        jobKind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "parent-1");
  }

  private void stagePlanSnapshotChunk(
      String jobId, String leaseEpoch, int chunkIndex, int fileGroupJobCount) {
    SubmitLeasedPlanSnapshotResultRequest.Chunk.Builder chunk =
        SubmitLeasedPlanSnapshotResultRequest.Chunk.newBuilder()
            .setChunkIndex(Math.max(0, chunkIndex));
    for (int index = 0; index < fileGroupJobCount; index++) {
      chunk.addFileGroupJobs(PlannedFileGroupPlanJob.getDefaultInstance());
    }
    stageChunk(
        "SubmitLeasedPlanSnapshotResult",
        jobId + ":" + leaseEpoch,
        chunkIndex,
        tableId("table-1"),
        chunk.build().toByteArray());
  }

  private void stageChunk(
      String operationName, String jobId, int chunkIndex, ResourceId resourceId, byte[] payload) {
    String key =
        Keys.idempotencyKey("acct", operationName, jobId + ":chunk:" + Math.max(0, chunkIndex));
    Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
    String requestHash = "hash-" + operationName + "-" + jobId + "-" + Math.max(0, chunkIndex);
    service.idempotencyStore.createPending("acct", key, operationName, requestHash, now, now);
    service.idempotencyStore.finalizeSuccess(
        "acct",
        key,
        operationName,
        requestHash,
        resourceId,
        MutationMeta.getDefaultInstance(),
        payload,
        now,
        now);
  }

  private static ResourceId tableId(String tableId) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId(tableId)
        .build();
  }

  private static ResourceId activeConnectorId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }
}
