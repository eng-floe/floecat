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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
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
    java.util.ArrayList<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        new java.util.ArrayList<>((int) (plannedTableJobs + plannedViewJobs));
    for (PlannedViewJob viewJob : nullToEmpty(viewJobs)) {
      if (viewJob == null || viewJob.viewTask().isEmpty()) {
        continue;
      }
      childSpecs.add(
          ReconcileJobStore.BulkEnqueueSpec.of(
              lease.accountId,
              lease.connectorId,
              lease.fullRescan,
              lease.captureMode,
              viewJob.scope(),
              ReconcileJobKind.PLAN_VIEW,
              ReconcileTableTask.empty(),
              viewJob.viewTask(),
              ReconcileSnapshotTask.empty(),
              ReconcileFileGroupTask.empty(),
              effectiveExecutionPolicy(lease),
              lease.jobId,
              ""));
    }
    for (PlannedTableJob tableJob : nullToEmpty(tableJobs)) {
      if (tableJob == null || tableJob.tableTask().isEmpty()) {
        continue;
      }
      childSpecs.add(
          ReconcileJobStore.BulkEnqueueSpec.of(
              lease.accountId,
              lease.connectorId,
              lease.fullRescan,
              lease.captureMode,
              tableJob.scope(),
              ReconcileJobKind.PLAN_TABLE,
              tableJob.tableTask(),
              ReconcileViewTask.empty(),
              ReconcileSnapshotTask.empty(),
              ReconcileFileGroupTask.empty(),
              effectiveExecutionPolicy(lease),
              lease.jobId,
              ""));
    }
    jobs.bulkEnqueue(childSpecs);
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
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      List<PlannedSnapshotJob> snapshotJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    long plannedSnapshotJobs =
        nullToEmpty(snapshotJobs).stream()
            .filter(job -> job != null && !job.snapshotTask().isEmpty())
            .count();
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
    boolean accepted =
        completePlanSuccess(
            jobId,
            leaseEpoch,
            "Planned " + plannedSnapshotJobs + " snapshot job(s)",
            tablesScanned,
            tablesChanged,
            0L,
            0L,
            errors,
            snapshotsProcessed,
            statsProcessed);
    if (!accepted) {
      return false;
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
    List<PlannedFileGroupJob> plannedJobs = plannedFileGroupJobs(finalizedSnapshotTask);
    ReconcileSnapshotTask durableSnapshotTask =
        durableSnapshotTask(finalizedSnapshotTask, plannedJobs);
    long plannedFileGroupJobs =
        plannedJobs.stream().filter(job -> job != null && !job.fileGroupTask().isEmpty()).count();
    jobs.persistSnapshotPlan(lease.jobId, durableSnapshotTask);
    java.util.Set<String> existingFileGroupKeys =
        existingExecFileGroupKeys(lease.accountId, lease.jobId);
    java.util.ArrayList<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        new java.util.ArrayList<>(plannedJobs.size() + 1);
    for (PlannedFileGroupJob fileGroupJob : plannedJobs) {
      if (fileGroupJob == null || fileGroupJob.fileGroupTask().isEmpty()) {
        continue;
      }
      String fileGroupKey = execFileGroupKey(fileGroupJob.fileGroupTask());
      if (!fileGroupKey.isBlank() && existingFileGroupKeys.contains(fileGroupKey)) {
        continue;
      }
      childSpecs.add(
          ReconcileJobStore.BulkEnqueueSpec.of(
              lease.accountId,
              lease.connectorId,
              lease.fullRescan,
              lease.captureMode,
              fileGroupJob.scope(),
              ReconcileJobKind.EXEC_FILE_GROUP,
              ReconcileTableTask.empty(),
              ReconcileViewTask.empty(),
              ReconcileSnapshotTask.empty(),
              fileGroupJob.fileGroupTask().asReference(),
              effectiveExecutionPolicy(lease),
              lease.jobId,
              ""));
      if (!fileGroupKey.isBlank()) {
        existingFileGroupKeys.add(fileGroupKey);
      }
    }
    childSpecs.add(
        ReconcileJobStore.BulkEnqueueSpec.of(
            lease.accountId,
            lease.connectorId,
            lease.fullRescan,
            lease.captureMode,
            effectiveScope(lease),
            ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            durableSnapshotTask,
            ReconcileFileGroupTask.empty(),
            effectiveExecutionPolicy(lease),
            lease.jobId,
            ""));
    jobs.bulkEnqueue(childSpecs);
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
    return true;
  }

  private List<PlannedFileGroupJob> plannedFileGroupJobs(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()) {
      return List.of();
    }
    if (!effective.fileGroups().isEmpty()) {
      return effective.fileGroups().stream()
          .filter(group -> group != null && !group.isEmpty())
          .map(group -> new PlannedFileGroupJob(ReconcileScope.empty(), group))
          .toList();
    }
    return snapshotPlanBlobStore.loadPlanJobs(effective);
  }

  private java.util.Set<String> existingExecFileGroupKeys(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return java.util.Set.of();
    }
    java.util.Set<String> keys = new java.util.LinkedHashSet<>();
    String pageToken = "";
    do {
      ReconcileJobStore.ReconcileJobPage page =
          jobs.childJobsPage(accountId, parentJobId, 200, pageToken);
      if (page == null || page.jobs == null || page.jobs.isEmpty()) {
        break;
      }
      for (ReconcileJobStore.ReconcileJob child : page.jobs) {
        if (child == null
            || child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP
            || child.fileGroupTask == null
            || child.fileGroupTask.isEmpty()) {
          continue;
        }
        String key = execFileGroupKey(child.fileGroupTask);
        if (!key.isBlank()) {
          keys.add(key);
        }
      }
      pageToken = page.nextPageToken == null ? "" : page.nextPageToken;
    } while (!pageToken.isBlank());
    return keys;
  }

  private static String execFileGroupKey(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "";
    }
    String planId = blankToEmpty(fileGroupTask.planId());
    String groupId = blankToEmpty(fileGroupTask.groupId());
    if (planId.isBlank() || groupId.isBlank()) {
      return "";
    }
    return planId + "|" + groupId;
  }

  private static ReconcileSnapshotTask durableSnapshotTask(
      ReconcileSnapshotTask snapshotTask, List<PlannedFileGroupJob> plannedJobs) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()) {
      return effective;
    }
    List<ReconcileFileGroupTask> fileGroups =
        plannedJobs == null
            ? List.of()
            : plannedJobs.stream()
                .filter(job -> job != null && job.fileGroupTask() != null)
                .map(PlannedFileGroupJob::fileGroupTask)
                .filter(task -> !task.isEmpty())
                .toList();
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        fileGroups,
        true,
        effective.completionMode(),
        "",
        fileGroups.size());
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
        jobs.getLeaseView(jobId)
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
