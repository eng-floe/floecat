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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerContext;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPolicyRegistry;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class LeasedPlannerWorkerServiceTest {
  private LeasedPlannerWorkerService service;
  private ReconcileJobStore jobs;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedPlannerWorkerService();
    jobs = mock(ReconcileJobStore.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    when(principal.getCorrelationId()).thenReturn("corr");
  }

  @Test
  void resolvePlanConnectorPreservesPinnedExecutorId() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.get("job-1"))
        .thenReturn(
            java.util.Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    "JS_QUEUED",
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
  void persistPlanSnapshotSuccessStoresExpandedSnapshotPlanBeforeEnqueueingReferences() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"));
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.get("job-1"))
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

    boolean accepted =
        service.persistPlanSnapshotSuccess(
            principal,
            "job-1",
            "lease-1",
            List.of(
                new LeasedPlannerWorkerService.PlannedFileGroupJob(
                    ReconcileScope.empty(), fullGroup)));

    assertEquals(true, accepted);
    InOrder inOrder = inOrder(jobs);
    inOrder.verify(jobs).renewLease("job-1", "lease-1");
    inOrder.verify(jobs).get("job-1");
    inOrder
        .verify(jobs)
        .persistSnapshotPlan(
            eq("job-1"),
            eq(ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(fullGroup), true)));
    // Parent job has P3_BACKGROUND policy (defaults) → isNewSnapshot=false → fallback
    // P3_BACKGROUND.
    // laneKey = "acct:table-1" since registry is absent (no CDI in unit test).
    ReconcileExecutionPolicy expectedFileGroupPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:table-1", java.util.Map.of(), 0L);
    inOrder
        .verify(jobs)
        .enqueueFileGroupExecution(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(),
            eq(fullGroup.asReference()),
            eq(expectedFileGroupPolicy),
            eq("job-1"),
            eq("remote-executor"));
    inOrder
        .verify(jobs)
        .enqueueSnapshotFinalization(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(),
            eq(ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(fullGroup), true)),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            eq("remote-executor"));
    verify(jobs).persistSnapshotPlan(eq("job-1"), any());
  }

  @Test
  void persistPlanConnectorFailureMarksTerminalFailure() {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.get("job-1"))
        .thenReturn(java.util.Optional.of(job("job-1", ReconcileJobKind.PLAN_CONNECTOR)));

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
        .markFailedTerminal(
            eq("job-1"),
            eq("lease-1"),
            anyLong(),
            eq("boom"),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanTableFailureMarksWaitingForDependency() {
    when(jobs.renewLease("job-2", "lease-2")).thenReturn(true);
    when(jobs.get("job-2"))
        .thenReturn(java.util.Optional.of(job("job-2", ReconcileJobKind.PLAN_TABLE)));

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
        .markWaiting(
            eq("job-2"),
            eq("lease-2"),
            anyLong(),
            eq("waiting"),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  @Test
  void persistPlanSnapshotFailureMarksRetryableFailure() {
    when(jobs.renewLease("job-3", "lease-3")).thenReturn(true);
    when(jobs.get("job-3"))
        .thenReturn(java.util.Optional.of(job("job-3", ReconcileJobKind.PLAN_SNAPSHOT)));

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
        .markFailed(
            eq("job-3"),
            eq("lease-3"),
            anyLong(),
            eq("retry"),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L));
  }

  // ---------------------------------------------------------------------------
  // Priority-aware enqueue: PLAN_SNAPSHOT from PLAN_TABLE completion
  // ---------------------------------------------------------------------------

  @Test
  void persistPlanTableSuccessEnqueuesPlanSnapshotAtP1FreshnessWhenRegistryAbsent() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-snap", 10L, "ns", "tbl");
    when(jobs.renewLease("job-pt", "lease-pt")).thenReturn(true);
    when(jobs.get("job-pt"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-pt", ReconcileJobKind.PLAN_TABLE, ReconcileExecutionPolicy.defaults())));

    service.persistPlanTableSuccess(
        principal,
        "job-pt",
        "lease-pt",
        List.of(
            new LeasedPlannerWorkerService.PlannedSnapshotJob(
                ReconcileScope.empty(), snapshotTask)));

    // PLAN_SNAPSHOT is always isNewSnapshot=true → fallback returns P1_FRESHNESS when no registry.
    ReconcileExecutionPolicy expectedSnapshotPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS, "acct:tbl-snap", java.util.Map.of(), 0L);
    verify(jobs)
        .enqueueSnapshotPlan(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(),
            eq(snapshotTask),
            eq(expectedSnapshotPolicy),
            eq("job-pt"),
            eq("remote-executor"));
  }

  @Test
  void persistPlanSnapshotSuccessInheritsP1ForExecFileGroupWhenParentIsP1Freshness() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-fg", 20L, "ns", "tbl");
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of("plan-2", "grp-2", "tbl-fg", 20L, List.of("s3://f/a.parquet"));
    ReconcileExecutionPolicy parentPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS, "acct:tbl-fg", java.util.Map.of(), 0L);
    when(jobs.renewLease("job-ps", "lease-ps")).thenReturn(true);
    when(jobs.get("job-ps"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-ps", ReconcileJobKind.PLAN_SNAPSHOT, parentPolicy, snapshotTask)));

    service.persistPlanSnapshotSuccess(
        principal,
        "job-ps",
        "lease-ps",
        List.of(
            new LeasedPlannerWorkerService.PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));

    // Parent is P1_FRESHNESS → isNewSnapshot=true → fallback returns P1_FRESHNESS.
    ReconcileExecutionPolicy expectedPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS, "acct:tbl-fg", java.util.Map.of(), 0L);
    verify(jobs)
        .enqueueFileGroupExecution(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(),
            eq(fullGroup.asReference()),
            eq(expectedPolicy),
            eq("job-ps"),
            eq("remote-executor"));
  }

  @Test
  void persistPlanSnapshotSuccessUsesP3ForExecFileGroupWhenParentIsP3Background() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-bg", 30L, "ns", "tbl");
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of("plan-3", "grp-3", "tbl-bg", 30L, List.of("s3://f/b.parquet"));
    ReconcileExecutionPolicy parentPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:tbl-bg", java.util.Map.of(), 0L);
    when(jobs.renewLease("job-bg", "lease-bg")).thenReturn(true);
    when(jobs.get("job-bg"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-bg", ReconcileJobKind.PLAN_SNAPSHOT, parentPolicy, snapshotTask)));

    service.persistPlanSnapshotSuccess(
        principal,
        "job-bg",
        "lease-bg",
        List.of(
            new LeasedPlannerWorkerService.PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));

    // Parent is P3_BACKGROUND → isNewSnapshot=false → fallback returns P3_BACKGROUND.
    ReconcileExecutionPolicy expectedPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:tbl-bg", java.util.Map.of(), 0L);
    verify(jobs)
        .enqueueFileGroupExecution(
            eq("acct"),
            eq("connector-1"),
            eq(false),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(),
            eq(fullGroup.asReference()),
            eq(expectedPolicy),
            eq("job-bg"),
            eq("remote-executor"));
  }

  @Test
  void persistPlanTableSuccessPassesRegistryContextToPriorityPolicy() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-ctx", 40L, "ns", "tbl");
    when(jobs.renewLease("job-ctx", "lease-ctx")).thenReturn(true);
    when(jobs.get("job-ctx"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-ctx", ReconcileJobKind.PLAN_TABLE, ReconcileExecutionPolicy.defaults())));

    @SuppressWarnings("unchecked")
    Instance<SchedulerPolicyRegistry> registryInstance = mock(Instance.class);
    SchedulerPolicyRegistry registry = mock(SchedulerPolicyRegistry.class);
    SchedulerPriorityPolicy policy = mock(SchedulerPriorityPolicy.class);
    SchedulerContext context = mock(SchedulerContext.class);
    when(registryInstance.isUnsatisfied()).thenReturn(false);
    when(registryInstance.get()).thenReturn(registry);
    when(registry.activePriorityPolicy()).thenReturn(policy);
    when(registry.activeContext()).thenReturn(context);
    when(policy.assignForReconcileJob(
            eq(ReconcileJobKind.PLAN_SNAPSHOT), any(), eq(40L), eq(true), eq(context)))
        .thenReturn(
            new SchedulerPriorityPolicy.PriorityAssignment(
                StatsPriorityClass.P1_FRESHNESS, 7L, "acct:tbl-ctx"));
    service.schedulerRegistryInstance = registryInstance;

    service.persistPlanTableSuccess(
        principal,
        "job-ctx",
        "lease-ctx",
        List.of(
            new LeasedPlannerWorkerService.PlannedSnapshotJob(
                ReconcileScope.empty(), snapshotTask)));

    verify(policy)
        .assignForReconcileJob(
            eq(ReconcileJobKind.PLAN_SNAPSHOT), any(), eq(40L), eq(true), eq(context));
  }

  private static ReconcileJobStore.ReconcileJob jobWithPolicy(
      String jobId, ReconcileJobKind jobKind, ReconcileExecutionPolicy policy) {
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
        policy,
        "remote-executor",
        "remote_snapshot_planner_worker",
        jobKind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "parent-1");
  }

  private static ReconcileJobStore.ReconcileJob jobWithPolicy(
      String jobId,
      ReconcileJobKind jobKind,
      ReconcileExecutionPolicy policy,
      ReconcileSnapshotTask snapshotTask) {
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
        policy,
        "remote-executor",
        "remote_snapshot_planner_worker",
        jobKind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        "parent-1");
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
}
