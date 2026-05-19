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
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class LeasedPlannerWorkerService {
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerBackend backend;
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;

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
    long plannedTableJobs =
        nullToEmpty(tableJobs).stream()
            .filter(job -> job != null && !job.tableTask().isEmpty())
            .count();
    long plannedViewJobs =
        nullToEmpty(viewJobs).stream()
            .filter(job -> job != null && !job.viewTask().isEmpty())
            .count();
    boolean accepted =
        completePlanSuccess(
            jobId,
            leaseEpoch,
            "Planned "
                + plannedTableJobs
                + " table job(s)"
                + (plannedViewJobs == 0L ? "" : " and " + plannedViewJobs + " view job(s)"),
            plannedTableJobs,
            0L,
            plannedViewJobs,
            0L,
            0L,
            0L,
            0L);
    if (!accepted) {
      return false;
    }
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
    long plannedSnapshotJobs =
        nullToEmpty(snapshotJobs).stream()
            .filter(job -> job != null && !job.snapshotTask().isEmpty())
            .count();
    boolean accepted =
        completePlanSuccess(
            jobId,
            leaseEpoch,
            "Planned " + plannedSnapshotJobs + " snapshot job(s)",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L);
    if (!accepted) {
      return false;
    }
    for (PlannedSnapshotJob snapshotJob : nullToEmpty(snapshotJobs)) {
      if (snapshotJob == null || snapshotJob.snapshotTask().isEmpty()) {
        continue;
      }
      jobs.enqueueSnapshotPlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          snapshotJob.scope(),
          snapshotJob.snapshotTask(),
          effectiveExecutionPolicy(lease),
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
    boolean accepted =
        completePlanSuccess(
            jobId, leaseEpoch, "Planned view", 0L, 0L, 1L, changed ? 1L : 0L, 0L, 0L, 0L);
    return new PlanViewPersistResult(accepted, changed ? 1L : 0L);
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
      ReconcileSnapshotTask plannedSnapshotTask) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    ReconcileSnapshotTask baseSnapshotTask =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    ReconcileSnapshotTask finalizedSnapshotTask =
        plannedSnapshotTask == null || plannedSnapshotTask.isEmpty()
            ? ReconcileSnapshotTask.of(
                baseSnapshotTask.tableId(),
                baseSnapshotTask.snapshotId(),
                baseSnapshotTask.sourceNamespace(),
                baseSnapshotTask.sourceTable(),
                List.of(),
                true,
                baseSnapshotTask.completionMode(),
                baseSnapshotTask.fileGroupPlanBlobUri(),
                baseSnapshotTask.fileGroupCount())
            : plannedSnapshotTask;
    List<PlannedFileGroupJob> plannedJobs =
        snapshotPlanBlobStore.loadPlanJobs(finalizedSnapshotTask);
    long plannedFileGroupJobs =
        plannedJobs.stream().filter(job -> job != null && !job.fileGroupTask().isEmpty()).count();
    boolean accepted =
        completePlanSuccess(
            jobId,
            leaseEpoch,
            "Snapshot plan recorded for "
                + finalizedSnapshotTask.sourceNamespace()
                + "."
                + finalizedSnapshotTask.sourceTable()
                + " with "
                + plannedFileGroupJobs
                + " file group(s)",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L);
    if (!accepted) {
      return false;
    }
    jobs.persistSnapshotPlan(lease.jobId, finalizedSnapshotTask);
    for (PlannedFileGroupJob fileGroupJob : plannedJobs) {
      if (fileGroupJob == null || fileGroupJob.fileGroupTask().isEmpty()) {
        continue;
      }
      jobs.enqueueFileGroupExecution(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          fileGroupJob.scope(),
          fileGroupJob.fileGroupTask().asReference(),
          effectiveExecutionPolicy(lease),
          lease.jobId,
          lease.pinnedExecutorId);
    }
    jobs.enqueueSnapshotFinalization(
        lease.accountId,
        lease.connectorId,
        lease.fullRescan,
        lease.captureMode,
        effectiveScope(lease),
        finalizedSnapshotTask,
        effectiveExecutionPolicy(lease),
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
    if (!isActiveLeasedState(job.state)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "reconcile job is no longer active for lease "
                  + jobId
                  + " state="
                  + blankToEmpty(job.state))
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
      jobs.applyLeaseOutcome(
          jobId,
          leaseEpoch,
          ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
          finishedAtMs,
          message,
          0L,
          0L,
          0L,
          0L,
          1L,
          0L,
          0L);
      return;
    }
    if (retryClass
        == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
            .DEPENDENCY_NOT_READY) {
      jobs.applyLeaseOutcome(
          jobId,
          leaseEpoch,
          ReconcileJobStore.CompletionKind.FAILED_WAITING,
          finishedAtMs,
          message,
          0L,
          0L,
          0L,
          0L,
          1L,
          0L,
          0L);
      return;
    }
    jobs.applyLeaseOutcome(
        jobId,
        leaseEpoch,
        ReconcileJobStore.CompletionKind.FAILED_RETRYABLE,
        finishedAtMs,
        message,
        0L,
        0L,
        0L,
        0L,
        1L,
        0L,
        0L);
  }

  private boolean completePlanSuccess(
      String jobId,
      String leaseEpoch,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    return jobs.applyLeaseOutcome(
        jobId,
        leaseEpoch,
        ReconcileJobStore.CompletionKind.SUCCEEDED,
        System.currentTimeMillis(),
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
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

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }

  private static <T> List<T> nullToEmpty(List<T> values) {
    return values == null ? List.of() : values;
  }
}
