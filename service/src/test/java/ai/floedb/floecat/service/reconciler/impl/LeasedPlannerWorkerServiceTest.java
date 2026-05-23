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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
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
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class LeasedPlannerWorkerServiceTest {
  private LeasedPlannerWorkerService service;
  private ReconcileJobStore jobs;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedPlannerWorkerService();
    jobs = mock(ReconcileJobStore.class);
    principal = mock(PrincipalContext.class);
    service.snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    service.jobs = jobs;
    when(principal.getCorrelationId()).thenReturn("corr");
    when(service.snapshotPlanBlobStore.loadPlanJobs(any())).thenReturn(List.of());
    when(jobs.getLeaseView(anyString())).thenAnswer(inv -> jobs.get((String) inv.getArgument(0)));
    when(jobs.getCompletionLeaseView(anyString(), anyString(), anyBoolean()))
        .thenAnswer(
            inv -> {
              String jobId = inv.getArgument(0);
              String leaseEpoch = inv.getArgument(1);
              Optional<ReconcileJobStore.ReconcileJob> job = jobs.get(jobId);
              if (job.isEmpty()) {
                return Optional.empty();
              }
              ReconcileJobStore.ReconcileJob j = job.get();
              return Optional.of(
                  new ReconcileJobStore.LeasedJob(
                      j.jobId,
                      j.accountId,
                      j.connectorId,
                      j.fullRescan,
                      j.captureMode == null ? CaptureMode.METADATA_AND_CAPTURE : j.captureMode,
                      j.scope,
                      j.executionPolicy,
                      leaseEpoch,
                      j.pinnedExecutorId,
                      j.executorId,
                      j.jobKind,
                      j.tableTask,
                      j.viewTask,
                      j.snapshotTask,
                      j.fileGroupTask,
                      j.parentJobId));
            });
    when(jobs.adoptSnapshotPlanManifest(anyString(), anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(true);
    when(jobs.applyLeaseOutcome(
            anyString(),
            anyString(),
            any(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenCallRealMethod();
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
  void persistPlanSnapshotSuccessStoresExpandedSnapshotPlanBeforeEnqueueingReferences() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
    ReconcileSnapshotTask plannedSnapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "blob://plan",
            1);
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"));
    when(service.snapshotPlanBlobStore.loadPlanJobs(plannedSnapshotTask))
        .thenReturn(List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));
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
        service.persistPlanSnapshotSuccess(principal, "job-1", "lease-1", plannedSnapshotTask);

    assertEquals(true, accepted);
    // Parent job has P3_BACKGROUND policy (defaults) -> isNewSnapshot=false -> fallback
    // P3_BACKGROUND. laneKey = "acct:table-1" since registry is absent (no CDI in unit test).
    ReconcileExecutionPolicy expectedFileGroupPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:table-1", java.util.Map.of(), 0L);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    List<ReconcileJobStore.BulkEnqueueSpec> specs = specsCaptor.getValue();
    assertEquals(2, specs.size());
    ReconcileJobStore.BulkEnqueueSpec execSpec = specs.get(0);
    ReconcileJobStore.BulkEnqueueSpec finalizeSpec = specs.get(1);
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, execSpec.jobKind);
    assertEquals(fullGroup.asReference(), execSpec.fileGroupTask);
    assertEquals(expectedFileGroupPolicy, execSpec.executionPolicy);
    assertEquals(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, finalizeSpec.jobKind);
    assertEquals(plannedSnapshotTask, finalizeSpec.snapshotTask);
    assertEquals(ReconcileExecutionPolicy.defaults(), finalizeSpec.executionPolicy);
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
        .applyLeaseOutcome(
            eq("job-2"),
            eq("lease-2"),
            eq(ReconcileJobStore.CompletionKind.FAILED_WAITING),
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
        0L,
        0L,
        0L,
        0L,
        0L,
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
    ReconcileSnapshotTask plannedSnapshotTask =
        ReconcileSnapshotTask.of(
            "tbl-fg",
            20L,
            "ns",
            "tbl",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "blob://plan-fg",
            1);
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of("plan-2", "grp-2", "tbl-fg", 20L, List.of("s3://f/a.parquet"));
    ReconcileExecutionPolicy parentPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS, "acct:tbl-fg", java.util.Map.of(), 0L);
    when(service.snapshotPlanBlobStore.loadPlanJobs(plannedSnapshotTask))
        .thenReturn(List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));
    when(jobs.renewLease("job-ps", "lease-ps")).thenReturn(true);
    when(jobs.get("job-ps"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-ps", ReconcileJobKind.PLAN_SNAPSHOT, parentPolicy, snapshotTask)));

    service.persistPlanSnapshotSuccess(principal, "job-ps", "lease-ps", plannedSnapshotTask);

    // Parent is P1_FRESHNESS → isNewSnapshot=true → fallback returns P1_FRESHNESS.
    ReconcileExecutionPolicy expectedPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS, "acct:tbl-fg", java.util.Map.of(), 0L);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileJobStore.BulkEnqueueSpec execSpec =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .findFirst()
            .orElseThrow();
    assertEquals(fullGroup.asReference(), execSpec.fileGroupTask);
    assertEquals(expectedPolicy, execSpec.executionPolicy);
  }

  @Test
  void persistPlanSnapshotSuccessUsesP3ForExecFileGroupWhenParentIsP3Background() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-bg", 30L, "ns", "tbl");
    ReconcileSnapshotTask plannedSnapshotTask =
        ReconcileSnapshotTask.of(
            "tbl-bg",
            30L,
            "ns",
            "tbl",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "blob://plan-bg",
            1);
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of("plan-3", "grp-3", "tbl-bg", 30L, List.of("s3://f/b.parquet"));
    ReconcileExecutionPolicy parentPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:tbl-bg", java.util.Map.of(), 0L);
    when(service.snapshotPlanBlobStore.loadPlanJobs(plannedSnapshotTask))
        .thenReturn(List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));
    when(jobs.renewLease("job-bg", "lease-bg")).thenReturn(true);
    when(jobs.get("job-bg"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-bg", ReconcileJobKind.PLAN_SNAPSHOT, parentPolicy, snapshotTask)));

    service.persistPlanSnapshotSuccess(principal, "job-bg", "lease-bg", plannedSnapshotTask);

    // Parent is P3_BACKGROUND → isNewSnapshot=false → fallback returns P3_BACKGROUND.
    ReconcileExecutionPolicy expectedPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND, "acct:tbl-bg", java.util.Map.of(), 0L);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileJobStore.BulkEnqueueSpec execSpec =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .findFirst()
            .orElseThrow();
    assertEquals(fullGroup.asReference(), execSpec.fileGroupTask);
    assertEquals(expectedPolicy, execSpec.executionPolicy);
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
        0L,
        0L,
        0L,
        0L,
        0L,
        List.of(
            new LeasedPlannerWorkerService.PlannedSnapshotJob(
                ReconcileScope.empty(), snapshotTask)));

    verify(policy)
        .assignForReconcileJob(
            eq(ReconcileJobKind.PLAN_SNAPSHOT), any(), eq(40L), eq(true), eq(context));
  }

  @Test
  void persistPlanSnapshotSuccessPreservesParentExecutionClassOnChildJobs() {
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-heavy", 77L, "ns", "tbl");
    ReconcileSnapshotTask plannedSnapshotTask =
        ReconcileSnapshotTask.of(
            "tbl-heavy",
            77L,
            "ns",
            "tbl",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "blob://plan-heavy",
            1);
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-heavy", "group-heavy", "tbl-heavy", 77L, List.of("s3://bucket/heavy.parquet"));
    ReconcileExecutionPolicy parentPolicy =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "", java.util.Map.of());
    when(service.snapshotPlanBlobStore.loadPlanJobs(plannedSnapshotTask))
        .thenReturn(List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));

    when(jobs.renewLease("job-heavy", "lease-heavy")).thenReturn(true);
    when(jobs.get("job-heavy"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-heavy", ReconcileJobKind.PLAN_SNAPSHOT, parentPolicy, snapshotTask)));

    service.persistPlanSnapshotSuccess(principal, "job-heavy", "lease-heavy", plannedSnapshotTask);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileExecutionPolicy childPolicy =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .findFirst()
            .orElseThrow()
            .executionPolicy;

    assertEquals(ReconcileExecutionClass.HEAVY, childPolicy.executionClass());
    assertEquals(StatsPriorityClass.P3_BACKGROUND, childPolicy.priorityClass());
    assertEquals("acct:tbl-heavy", childPolicy.lane());
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

  @Test
  void policyDeferredAttributeIsNotPropagatedToChildFileGroupJobs() {
    // A parent PLAN_SNAPSHOT job may carry ATTR_POLICY_DEFERRED in its attributes when the
    // orchestrator-layer admission policy voted DEFER at parent enqueue time. That attribute
    // must NOT be copied into child EXEC_FILE_GROUP policies — child admission is re-evaluated
    // independently at each child's own enqueue time, and P1_FRESHNESS children must always ADMIT.
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("tbl-deferred", 42L, "db", "t");
    ReconcileSnapshotTask plannedSnapshotTask =
        ReconcileSnapshotTask.of(
            "tbl-deferred",
            42L,
            "db",
            "t",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "blob://plan-deferred",
            1);
    ReconcileFileGroupTask fullGroup =
        ReconcileFileGroupTask.of(
            "plan-deferred", "group-1", "tbl-deferred", 42L, List.of("s3://bucket/f.parquet"));
    ReconcileExecutionPolicy deferredParentPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS,
            "acct:tbl-deferred",
            java.util.Map.of(
                "enqueue_reason", "new_snapshot",
                "policy_deferred", "true"), // ATTR_POLICY_DEFERRED = "policy_deferred"
            0L);

    when(jobs.renewLease("job-deferred", "lease-deferred")).thenReturn(true);
    when(service.snapshotPlanBlobStore.loadPlanJobs(plannedSnapshotTask))
        .thenReturn(List.of(new PlannedFileGroupJob(ReconcileScope.empty(), fullGroup)));
    when(jobs.get("job-deferred"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicy(
                    "job-deferred",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    deferredParentPolicy,
                    snapshotTask)));

    service.persistPlanSnapshotSuccess(
        principal, "job-deferred", "lease-deferred", plannedSnapshotTask);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileExecutionPolicy childPolicy =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .findFirst()
            .orElseThrow()
            .executionPolicy;
    assertFalse(
        childPolicy.attributes().containsKey("policy_deferred"),
        "ATTR_POLICY_DEFERRED must not be propagated to child EXEC_FILE_GROUP jobs");
    // Other attributes (enqueue_reason) are still inherited.
    assertEquals("new_snapshot", childPolicy.attributes().get("enqueue_reason"));
  }

  @Test
  void policyDeferredAttributeIsNotPropagatedToChildTablePlanJobs() {
    // A parent PLAN_CONNECTOR job may carry ATTR_POLICY_DEFERRED in its attributes.
    // That attribute must NOT be copied into child PLAN_TABLE policies.
    ReconcileExecutionPolicy deferredConnectorPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND,
            "acct:conn",
            java.util.Map.of(
                "enqueue_reason", "batch_refresh",
                "policy_deferred", "true"),
            0L);

    when(jobs.renewLease("job-conn-deferred", "lease-conn-deferred")).thenReturn(true);
    when(jobs.get("job-conn-deferred"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicyAndKind(
                    "job-conn-deferred",
                    ReconcileJobKind.PLAN_CONNECTOR,
                    deferredConnectorPolicy)));

    service.persistPlanConnectorSuccess(
        principal,
        "job-conn-deferred",
        "lease-conn-deferred",
        List.of(
            new LeasedPlannerWorkerService.PlannedTableJob(
                ReconcileScope.empty(),
                ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of("ns", "t1"))),
        List.of());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileExecutionPolicy childPolicy =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.PLAN_TABLE)
            .findFirst()
            .orElseThrow()
            .executionPolicy;
    assertFalse(
        childPolicy.attributes().containsKey("policy_deferred"),
        "ATTR_POLICY_DEFERRED must not be propagated to child PLAN_TABLE jobs");
    assertEquals("batch_refresh", childPolicy.attributes().get("enqueue_reason"));
  }

  @Test
  void policyDeferredAttributeIsNotPropagatedToChildViewPlanJobs() {
    // Same invariant as the table case — PLAN_VIEW children must not inherit policy_deferred.
    ReconcileExecutionPolicy deferredConnectorPolicy =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P3_BACKGROUND,
            "acct:conn",
            java.util.Map.of(
                "enqueue_reason", "batch_refresh",
                "policy_deferred", "true"),
            0L);

    when(jobs.renewLease("job-conn-view-deferred", "lease-conn-view-deferred")).thenReturn(true);
    when(jobs.get("job-conn-view-deferred"))
        .thenReturn(
            java.util.Optional.of(
                jobWithPolicyAndKind(
                    "job-conn-view-deferred",
                    ReconcileJobKind.PLAN_CONNECTOR,
                    deferredConnectorPolicy)));

    service.persistPlanConnectorSuccess(
        principal,
        "job-conn-view-deferred",
        "lease-conn-view-deferred",
        List.of(),
        List.of(
            new LeasedPlannerWorkerService.PlannedViewJob(
                ReconcileScope.empty(),
                ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.of(
                    "ns", "v1", "ns-id", "view-id"))));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ReconcileJobStore.BulkEnqueueSpec>> specsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(jobs).bulkEnqueue(specsCaptor.capture());
    ReconcileExecutionPolicy childPolicy =
        specsCaptor.getValue().stream()
            .filter(spec -> spec.jobKind == ReconcileJobKind.PLAN_VIEW)
            .findFirst()
            .orElseThrow()
            .executionPolicy;
    assertFalse(
        childPolicy.attributes().containsKey("policy_deferred"),
        "ATTR_POLICY_DEFERRED must not be propagated to child PLAN_VIEW jobs");
    assertEquals("batch_refresh", childPolicy.attributes().get("enqueue_reason"));
  }

  private static ReconcileJobStore.ReconcileJob jobWithPolicyAndKind(
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
        ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        policy,
        "remote-executor",
        "remote_connector_planner_worker",
        jobKind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
        "parent-0");
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
