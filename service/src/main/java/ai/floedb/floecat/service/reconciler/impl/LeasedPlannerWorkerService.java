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

import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPolicyRegistry;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

@ApplicationScoped
public class LeasedPlannerWorkerService {
  private static final Logger LOG = Logger.getLogger(LeasedPlannerWorkerService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerBackend backend;

  /**
   * Optional injection — absent in test environments without a full CDI container. When unsatisfied
   * the planner falls back to safe priority defaults (P1_FRESHNESS for PLAN_SNAPSHOT, P3_BACKGROUND
   * for EXEC_FILE_GROUP) instead of failing.
   */
  @Inject Instance<SchedulerPolicyRegistry> schedulerRegistryInstance;

  record PlanConnectorPayload(
      String jobId,
      String leaseEpoch,
      ResourceId connectorId,
      ReconcilerService.CaptureMode captureMode,
      boolean fullRescan,
      ReconcileScope scope,
      ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy executionPolicy,
      String pinnedExecutorId) {}

  record PlanTablePayload(
      String jobId,
      String leaseEpoch,
      String parentJobId,
      ResourceId connectorId,
      ReconcilerService.CaptureMode captureMode,
      boolean fullRescan,
      ReconcileScope scope,
      ReconcileTableTask tableTask) {}

  record PlanViewPayload(
      String jobId,
      String leaseEpoch,
      String parentJobId,
      ResourceId connectorId,
      ReconcileScope scope,
      ReconcileViewTask viewTask) {}

  record PlannedViewMutation(
      ResourceId destinationViewId, ViewSpec viewSpec, String idempotencyKey) {}

  record PlanViewPersistResult(boolean accepted, long viewsChanged) {}

  record PlanSnapshotPayload(
      String jobId,
      String leaseEpoch,
      String parentJobId,
      ResourceId connectorId,
      ReconcilerService.CaptureMode captureMode,
      boolean fullRescan,
      ReconcileScope scope,
      ReconcileSnapshotTask snapshotTask) {}

  public PlanConnectorPayload resolvePlanConnector(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(),
            jobId,
            leaseEpoch,
            ReconcileJobKind.PLAN_CONNECTOR);
    return new PlanConnectorPayload(
        lease.jobId,
        lease.leaseEpoch,
        connectorId(lease),
        lease.captureMode,
        lease.fullRescan,
        effectiveScope(lease),
        effectiveExecutionPolicy(lease),
        blankToEmpty(lease.pinnedExecutorId));
  }

  public boolean persistPlanConnectorSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      List<PlannedTableJob> tableJobs,
      List<PlannedViewJob> viewJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(),
            jobId,
            leaseEpoch,
            ReconcileJobKind.PLAN_CONNECTOR);
    for (PlannedViewJob viewJob : nullToEmpty(viewJobs)) {
      if (viewJob == null || viewJob.viewTask().isEmpty()) {
        continue;
      }
      jobs.enqueueViewPlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          viewJob.scope(),
          viewJob.viewTask(),
          effectiveExecutionPolicy(lease),
          lease.jobId,
          lease.pinnedExecutorId);
    }
    for (PlannedTableJob tableJob : nullToEmpty(tableJobs)) {
      if (tableJob == null || tableJob.tableTask().isEmpty()) {
        continue;
      }
      jobs.enqueueTablePlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          tableJob.scope(),
          tableJob.tableTask(),
          effectiveExecutionPolicy(lease),
          lease.jobId,
          lease.pinnedExecutorId);
    }
    return true;
  }

  public boolean persistPlanConnectorFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    requireLeasedJob(
        principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_CONNECTOR);
    persistPlanFailure(jobId, leaseEpoch, retryDisposition, retryClass, message);
    return true;
  }

  public PlanTablePayload resolvePlanTable(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    return new PlanTablePayload(
        lease.jobId,
        lease.leaseEpoch,
        blankToEmpty(lease.parentJobId),
        connectorId(lease),
        lease.captureMode,
        lease.fullRescan,
        effectiveScope(lease),
        lease.tableTask == null ? ReconcileTableTask.empty() : lease.tableTask);
  }

  public boolean persistPlanTableSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      List<PlannedSnapshotJob> snapshotJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    for (PlannedSnapshotJob snapshotJob : nullToEmpty(snapshotJobs)) {
      if (snapshotJob == null || snapshotJob.snapshotTask().isEmpty()) {
        continue;
      }
      ReconcileSnapshotTask snapshotTask = snapshotJob.snapshotTask();
      // tableId() is a plain String; combine with lease.accountId for the WRR lane key.
      String laneKey = lease.accountId + ":" + snapshotTask.tableId();
      // PLAN_SNAPSHOT jobs always originate from fresh snapshot discovery → isNewSnapshot=true
      PriorityAssignment assignment =
          assignForReconcileJobSafe(
              ReconcileJobKind.PLAN_SNAPSHOT,
              laneKey,
              snapshotTask.snapshotId(),
              /* isNewSnapshot= */ true);
      ReconcileExecutionPolicy snapshotPolicy = policyFromAssignment(assignment, lease);
      jobs.enqueueSnapshotPlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          snapshotJob.scope(),
          snapshotTask,
          snapshotPolicy,
          lease.jobId,
          lease.pinnedExecutorId);
    }
    return true;
  }

  public boolean persistPlanTableFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    requireLeasedJob(
        principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    persistPlanFailure(jobId, leaseEpoch, retryDisposition, retryClass, message);
    return true;
  }

  public PlanViewPayload resolvePlanView(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_VIEW);
    return new PlanViewPayload(
        lease.jobId,
        lease.leaseEpoch,
        blankToEmpty(lease.parentJobId),
        connectorId(lease),
        effectiveScope(lease),
        lease.viewTask == null ? ReconcileViewTask.empty() : lease.viewTask);
  }

  public PlanViewPersistResult persistPlanViewSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      PlannedViewMutation mutation) {
    requireLeasedJob(
        principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_VIEW);
    if (mutation == null || mutation.viewSpec() == null) {
      return new PlanViewPersistResult(true, 0L);
    }
    ReconcileContext ctx =
        new ReconcileContext(
            principalContext.getCorrelationId(),
            principalContext,
            "reconcile_executor_control",
            Instant.now(),
            Optional.empty());
    boolean changed =
        mutation.destinationViewId() != null
                && mutation.destinationViewId().getId() != null
                && !mutation.destinationViewId().getId().isBlank()
            ? backend.updateViewById(ctx, mutation.destinationViewId(), mutation.viewSpec())
            : backend
                .ensureView(
                    ctx,
                    mutation.viewSpec(),
                    mutation.idempotencyKey() == null ? "" : mutation.idempotencyKey())
                .changed();
    return new PlanViewPersistResult(true, changed ? 1L : 0L);
  }

  public boolean persistPlanViewFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    requireLeasedJob(
        principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_VIEW);
    persistPlanFailure(jobId, leaseEpoch, retryDisposition, retryClass, message);
    return true;
  }

  public PlanSnapshotPayload resolvePlanSnapshot(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    return new PlanSnapshotPayload(
        lease.jobId,
        lease.leaseEpoch,
        blankToEmpty(lease.parentJobId),
        connectorId(lease),
        lease.captureMode,
        lease.fullRescan,
        effectiveScope(lease),
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask);
  }

  public boolean persistPlanSnapshotSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      List<PlannedFileGroupJob> fileGroupJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    List<ReconcileFileGroupTask> plannedGroups =
        nullToEmpty(fileGroupJobs).stream()
            .filter(fileGroupJob -> fileGroupJob != null && !fileGroupJob.fileGroupTask().isEmpty())
            .map(PlannedFileGroupJob::fileGroupTask)
            .toList();
    ReconcileSnapshotTask baseSnapshotTask =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    ReconcileSnapshotTask finalizedSnapshotTask =
        ReconcileSnapshotTask.of(
            baseSnapshotTask.tableId(),
            baseSnapshotTask.snapshotId(),
            baseSnapshotTask.sourceNamespace(),
            baseSnapshotTask.sourceTable(),
            plannedGroups,
            true);
    jobs.persistSnapshotPlan(lease.jobId, finalizedSnapshotTask);
    // Derive isNewSnapshot from the parent PLAN_SNAPSHOT's priority class.
    // If the parent ran at P1_FRESHNESS it was new-snapshot work; children inherit that judgment.
    boolean isNewSnapshot =
        lease.executionPolicy != null
            && lease.executionPolicy.priorityClass() == StatsPriorityClass.P1_FRESHNESS;
    for (PlannedFileGroupJob fileGroupJob : nullToEmpty(fileGroupJobs)) {
      if (fileGroupJob == null || fileGroupJob.fileGroupTask().isEmpty()) {
        continue;
      }
      ReconcileFileGroupTask fileGroupTask = fileGroupJob.fileGroupTask();
      // tableId() is a plain String; combine with lease.accountId for the WRR lane key.
      String laneKey =
          fileGroupTask.tableId() != null && !fileGroupTask.tableId().isBlank()
              ? lease.accountId + ":" + fileGroupTask.tableId()
              : lease.accountId;
      PriorityAssignment assignment =
          assignForReconcileJobSafe(
              ReconcileJobKind.EXEC_FILE_GROUP, laneKey, fileGroupTask.snapshotId(), isNewSnapshot);
      ReconcileExecutionPolicy fileGroupPolicy = policyFromAssignment(assignment, lease);
      jobs.enqueueFileGroupExecution(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          fileGroupJob.scope(),
          fileGroupTask.asReference(),
          fileGroupPolicy,
          lease.jobId,
          lease.pinnedExecutorId);
    }
    // FINALIZE_SNAPSHOT_CAPTURE is a snapshot-level housekeeping job — it is not scoped to a
    // per-table WRR lane. Strip the lane from the parent's execution policy so that
    // SnapshotFinalizeReconcileExecutor (which declares supportedLanes = {""}) can match it.
    // Priority class is preserved so P1_FRESHNESS snapshot work finalizes promptly.
    ReconcileExecutionPolicy finalizePolicy =
        ReconcileExecutionPolicy.of(
            effectiveExecutionPolicy(lease).priorityClass(),
            /* lane= */ "",
            effectiveExecutionPolicy(lease).attributes(),
            effectiveExecutionPolicy(lease).priorityScore());
    jobs.enqueueSnapshotFinalization(
        lease.accountId,
        lease.connectorId,
        lease.fullRescan,
        lease.captureMode,
        effectiveScope(lease),
        finalizedSnapshotTask,
        finalizePolicy,
        lease.jobId,
        lease.pinnedExecutorId);
    return true;
  }

  public boolean persistPlanSnapshotFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    requireLeasedJob(
        principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    persistPlanFailure(jobId, leaseEpoch, retryDisposition, retryClass, message);
    return true;
  }

  record PlannedTableJob(ReconcileScope scope, ReconcileTableTask tableTask) {}

  record PlannedViewJob(ReconcileScope scope, ReconcileViewTask viewTask) {}

  record PlannedSnapshotJob(ReconcileScope scope, ReconcileSnapshotTask snapshotTask) {}

  record PlannedFileGroupJob(ReconcileScope scope, ReconcileFileGroupTask fileGroupTask) {}

  private ReconcileJobStore.LeasedJob requireLeasedJob(
      String corr, String jobId, String leaseEpoch, ReconcileJobKind expectedKind) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.get(jobId)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("reconcile job not found: " + jobId)
                        .asRuntimeException());
    if (job.jobKind != expectedKind) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a " + expectedKind + " job")
          .asRuntimeException();
    }
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode == null
            ? ReconcilerService.CaptureMode.METADATA_AND_CAPTURE
            : job.captureMode,
        job.scope,
        job.executionPolicy,
        leaseEpoch,
        blankToEmpty(job.pinnedExecutorId),
        blankToEmpty(job.executorId),
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        blankToEmpty(job.parentJobId));
  }

  private void persistPlanFailure(
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    long finishedAtMs = System.currentTimeMillis();
    if (retryDisposition
        == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
            .TERMINAL) {
      jobs.markFailedTerminal(jobId, leaseEpoch, finishedAtMs, message, 0L, 0L, 1L, 0L, 0L);
      return;
    }
    if (retryClass
        == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
            .DEPENDENCY_NOT_READY) {
      jobs.markWaiting(jobId, leaseEpoch, finishedAtMs, message, 0L, 0L, 1L, 0L, 0L);
      return;
    }
    jobs.markFailed(jobId, leaseEpoch, finishedAtMs, message, 0L, 0L, 1L, 0L, 0L);
  }

  private static ResourceId connectorId(ReconcileJobStore.LeasedJob lease) {
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId)
        .setId(lease.connectorId)
        .setKind(ResourceKind.RK_CONNECTOR)
        .build();
  }

  private static ReconcileScope effectiveScope(ReconcileJobStore.LeasedJob lease) {
    return lease == null || lease.scope == null ? ReconcileScope.empty() : lease.scope;
  }

  private static ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy
      effectiveExecutionPolicy(ReconcileJobStore.LeasedJob lease) {
    return lease == null || lease.executionPolicy == null
        ? ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy.defaults()
        : lease.executionPolicy;
  }

  /**
   * Calls {@link SchedulerPolicyRegistry#activePriorityPolicy()}.assignForReconcileJob() wrapped in
   * a try/catch so that a misconfigured or absent registry never prevents the planner from making
   * progress.
   *
   * <p>Fallback priorities when the registry is unavailable or throws:
   *
   * <ul>
   *   <li>{@link ReconcileJobKind#PLAN_SNAPSHOT}: {@link StatsPriorityClass#P1_FRESHNESS} — new
   *       snapshot work must never silently degrade to background.
   *   <li>all other kinds (e.g. {@link ReconcileJobKind#EXEC_FILE_GROUP}): {@link
   *       StatsPriorityClass#P3_BACKGROUND}.
   * </ul>
   */
  private PriorityAssignment assignForReconcileJobSafe(
      ReconcileJobKind kind, String laneKey, long snapshotId, boolean isNewSnapshot) {
    SchedulerPolicyRegistry registry = resolveRegistry();
    if (registry != null) {
      try {
        return registry
            .activePriorityPolicy()
            .assignForReconcileJob(kind, laneKey, snapshotId, isNewSnapshot, null);
      } catch (RuntimeException e) {
        LOG.warnf(
            e,
            "scheduler policy threw for kind=%s laneKey=%s; falling back to safe default",
            kind,
            laneKey);
      }
    }
    // Safe fallback: mirrors the interface default — P1_FRESHNESS when isNewSnapshot is true
    // (PLAN_SNAPSHOT is always new work; EXEC_FILE_GROUP inherits from parent's judgment).
    StatsPriorityClass fallbackClass =
        isNewSnapshot ? StatsPriorityClass.P1_FRESHNESS : StatsPriorityClass.P3_BACKGROUND;
    return new PriorityAssignment(fallbackClass, 0L, laneKey);
  }

  private SchedulerPolicyRegistry resolveRegistry() {
    try {
      if (schedulerRegistryInstance == null || schedulerRegistryInstance.isUnsatisfied()) {
        return null;
      }
      return schedulerRegistryInstance.get();
    } catch (RuntimeException e) {
      LOG.debugf("scheduler registry not available: %s", e.getMessage());
      return null;
    }
  }

  /**
   * Builds a {@link ReconcileExecutionPolicy} from a {@link PriorityAssignment}, merging attributes
   * from the parent lease. Attributes from the lease are overridden by any attributes derived from
   * the assignment (none at present, reserved for future use).
   */
  private static ReconcileExecutionPolicy policyFromAssignment(
      PriorityAssignment assignment, ReconcileJobStore.LeasedJob lease) {
    Map<String, String> baseAttributes =
        lease.executionPolicy != null && lease.executionPolicy.attributes() != null
            ? lease.executionPolicy.attributes()
            : Map.of();
    return ReconcileExecutionPolicy.of(
        assignment.priorityClass(), assignment.laneKey(), baseAttributes, assignment.score());
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static <T> List<T> nullToEmpty(List<T> values) {
    return values == null ? List.of() : values;
  }
}
