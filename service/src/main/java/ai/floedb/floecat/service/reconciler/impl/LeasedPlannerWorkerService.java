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
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.reconciler.jobs.impl.SchedulerStoreHelpers;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPolicyRegistry;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy.PriorityAssignment;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerSignalIndex;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.jboss.logging.Logger;

@ApplicationScoped
public class LeasedPlannerWorkerService {
  private static final Logger LOG = Logger.getLogger(LeasedPlannerWorkerService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerBackend backend;
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;

  /**
   * Optional injection — absent in test environments without a full CDI container. When unsatisfied
   * the planner falls back to safe priority defaults (P1_FRESHNESS for PLAN_SNAPSHOT, P3_BACKGROUND
   * for EXEC_FILE_GROUP) instead of failing.
   */
  @Inject Instance<SchedulerPolicyRegistry> schedulerRegistryInstance;

  /** Optional — absent in lightweight test paths. Used only for delta computation. */
  @Inject Instance<SnapshotRepository> snapshotRepoInstance;

  /** Optional — absent in lightweight test paths. Silently skipped when unsatisfied. */
  @Inject Instance<SchedulerSignalIndex> signalIndexInstance;

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
    ReconcileJobStore.ReconcileJob currentJob =
        jobs.getLeaseView(jobId)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("reconcile job not found: " + jobId)
                        .asRuntimeException());
    if (currentJob.jobKind != ReconcileJobKind.PLAN_SNAPSHOT) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a " + ReconcileJobKind.PLAN_SNAPSHOT + " job")
          .asRuntimeException();
    }
    ReconcileSnapshotTask baseSnapshotTask =
        currentJob.snapshotTask == null ? ReconcileSnapshotTask.empty() : currentJob.snapshotTask;
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
    ReconcileSnapshotTask durableSnapshotTask = durableSnapshotTask(finalizedSnapshotTask);
    if ("JS_SUCCEEDED".equals(currentJob.state)) {
      if (durableSnapshotTask.equals(currentJob.snapshotTask)) {
        return true;
      }
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile snapshot plan was already adopted with a different result")
          .asRuntimeException();
    }
    ReconcileJobStore.LeasedJob lease =
        requireCompletionLease(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    long plannedFileGroupJobs =
        plannedJobs.stream().filter(job -> job != null && !job.fileGroupTask().isEmpty()).count();
    boolean adopted =
        jobs.adoptSnapshotPlanManifest(
            lease.jobId,
            lease.leaseEpoch,
            durableSnapshotTask,
            durableSnapshotTask.fileGroupPlanBlobUri(),
            true);
    if (!adopted) {
      return currentSnapshotPlanSuccess(jobId, durableSnapshotTask);
    }
    // Record the snapshot delta signal before enqueuing EXEC_FILE_GROUP jobs so the signal is
    // visible to the scheduler at assignment time. Best-effort: never aborts the planner path.
    recordSnapshotDeltaSafe(
        lease.accountId, baseSnapshotTask.tableId(), baseSnapshotTask.snapshotId());
    // Derive isNewSnapshot from the parent PLAN_SNAPSHOT's priority class.
    // If the parent ran at P1_FRESHNESS it was new-snapshot work; children inherit that judgment.
    boolean isNewSnapshot =
        lease.executionPolicy != null
            && lease.executionPolicy.priorityClass() == StatsPriorityClass.P1_FRESHNESS;
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
      ReconcileFileGroupTask fileGroupTask = fileGroupJob.fileGroupTask();
      String laneKey =
          fileGroupTask.tableId() != null && !fileGroupTask.tableId().isBlank()
              ? lease.accountId + ":" + fileGroupTask.tableId()
              : lease.accountId;
      PriorityAssignment assignment =
          assignForReconcileJobSafe(
              ReconcileJobKind.EXEC_FILE_GROUP, laneKey, fileGroupTask.snapshotId(), isNewSnapshot);
      ReconcileExecutionPolicy fileGroupPolicy = policyFromAssignment(assignment, lease);
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
              fileGroupTask.asReference(),
              fileGroupPolicy,
              lease.jobId,
              ""));
      if (!fileGroupKey.isBlank()) {
        existingFileGroupKeys.add(fileGroupKey);
      }
    }
    ReconcileExecutionPolicy finalizePolicy =
        ReconcileExecutionPolicy.of(
            effectiveExecutionPolicy(lease).priorityClass(),
            "",
            effectiveExecutionPolicy(lease).attributes(),
            effectiveExecutionPolicy(lease).priorityScore());
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
            finalizePolicy,
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
      return currentSnapshotPlanSuccess(jobId, durableSnapshotTask);
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

  private static ReconcileSnapshotTask durableSnapshotTask(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()) {
      return effective;
    }
    if (effective.fileGroups() != null && !effective.fileGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot plan success must use manifest-only file-group representation")
          .asRuntimeException();
    }
    if (effective.fileGroupCount() > 0
        && blankToEmpty(effective.fileGroupPlanBlobUri()).isBlank()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot plan success requires a persisted file-group manifest")
          .asRuntimeException();
    }
    return effective;
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

  private ReconcileJobStore.LeasedJob requireCompletionLease(
      String corr, String jobId, String leaseEpoch, ReconcileJobKind expectedKind) {
    ReconcileJobStore.LeasedJob lease =
        jobs.getCompletionLeaseView(jobId, leaseEpoch, true)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription("reconcile lease is no longer valid")
                        .asRuntimeException());
    if (lease.jobKind != expectedKind) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a " + expectedKind + " job")
          .asRuntimeException();
    }
    return lease;
  }

  private boolean currentSnapshotPlanSuccess(
      String jobId, ReconcileSnapshotTask durableSnapshotTask) {
    return jobs.getLeaseView(jobId)
        .filter(current -> current.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
        .filter(current -> "JS_SUCCEEDED".equals(current.state))
        .map(current -> durableSnapshotTask.equals(current.snapshotTask))
        .orElse(false);
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
    if (lease == null || lease.executionPolicy == null) {
      return ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy.defaults();
    }
    ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy policy = lease.executionPolicy;
    if (policy.attributes() == null
        || !policy.attributes().containsKey(SchedulerStoreHelpers.ATTR_POLICY_DEFERRED)) {
      return policy;
    }
    // Strip ATTR_POLICY_DEFERRED — it is scoped to the original enqueue decision for the parent
    // job and must never propagate to executors or child jobs (PLAN_TABLE, PLAN_VIEW, FINALIZE).
    Map<String, String> filtered =
        Map.copyOf(
            policy.attributes().entrySet().stream()
                .filter(e -> !SchedulerStoreHelpers.ATTR_POLICY_DEFERRED.equals(e.getKey()))
                .collect(
                    java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    return new ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy(
        policy.executionClass(),
        policy.lane(),
        filtered,
        policy.priorityClass(),
        policy.priorityScore());
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

  /**
   * Attempts to compute and record the snapshot delta row count in the {@link
   * SchedulerSignalIndex}. Fetches the current snapshot and its parent from {@link
   * SnapshotRepository} and computes the absolute difference in {@code total-records}. Silently
   * skips on any error so it never aborts the planner success path.
   */
  private void recordSnapshotDeltaSafe(String accountId, String tableId, long snapshotId) {
    if (signalIndexInstance == null || signalIndexInstance.isUnsatisfied()) return;
    if (snapshotRepoInstance == null || snapshotRepoInstance.isUnsatisfied()) return;
    try {
      SnapshotRepository snapshotRepo = snapshotRepoInstance.get();
      SchedulerSignalIndex signalIndex = signalIndexInstance.get();

      ResourceId tableResourceId =
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setKind(ResourceKind.RK_TABLE)
              .setId(tableId)
              .build();
      var currentOpt = snapshotRepo.getById(tableResourceId, snapshotId);
      if (currentOpt.isEmpty()) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      var current = currentOpt.get();
      String currentTotalStr = current.getSummaryMap().get("total-records");
      if (currentTotalStr == null || currentTotalStr.isBlank()) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      long currentTotal;
      try {
        currentTotal = Long.parseLong(currentTotalStr.trim());
      } catch (NumberFormatException e) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      if (!current.hasParentSnapshotId()) {
        // First snapshot — delta = all rows
        signalIndex.recordSnapshotDelta(
            accountId, tableId, snapshotId, OptionalLong.of(currentTotal));
        return;
      }
      var parentOpt = snapshotRepo.getById(tableResourceId, current.getParentSnapshotId());
      if (parentOpt.isEmpty()) {
        signalIndex.recordSnapshotDelta(
            accountId, tableId, snapshotId, OptionalLong.of(currentTotal));
        return;
      }
      String parentTotalStr = parentOpt.get().getSummaryMap().get("total-records");
      if (parentTotalStr == null || parentTotalStr.isBlank()) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      long parentTotal;
      try {
        parentTotal = Long.parseLong(parentTotalStr.trim());
      } catch (NumberFormatException e) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      signalIndex.recordSnapshotDelta(
          accountId, tableId, snapshotId, OptionalLong.of(Math.abs(currentTotal - parentTotal)));
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "scheduler_signal_index error computing snapshot delta table=%s snap=%d",
          tableId,
          snapshotId);
    }
  }

  private PriorityAssignment assignForReconcileJobSafe(
      ReconcileJobKind kind, String laneKey, long snapshotId, boolean isNewSnapshot) {
    SchedulerPolicyRegistry registry = resolveRegistry();
    if (registry != null) {
      try {
        return registry
            .activePriorityPolicy()
            .assignForReconcileJob(
                kind, laneKey, snapshotId, isNewSnapshot, registry.activeContext());
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
   *
   * <p>{@code ATTR_POLICY_DEFERRED} is explicitly stripped. That attribute is scoped to the parent
   * job's enqueue-time admission decision; child jobs (PLAN_SNAPSHOT, EXEC_FILE_GROUP, FINALIZE)
   * are enqueued independently and their admission is re-evaluated at their own enqueue time.
   * Propagating it would defer P1_FRESHNESS children even though the admission-policy invariant
   * requires P1 to always ADMIT.
   */
  private static ReconcileExecutionPolicy policyFromAssignment(
      PriorityAssignment assignment, ReconcileJobStore.LeasedJob lease) {
    Map<String, String> rawAttributes =
        lease.executionPolicy != null && lease.executionPolicy.attributes() != null
            ? lease.executionPolicy.attributes()
            : Map.of();
    // Strip ATTR_POLICY_DEFERRED so it is never inherited by child jobs.
    Map<String, String> baseAttributes =
        rawAttributes.containsKey(SchedulerStoreHelpers.ATTR_POLICY_DEFERRED)
            ? Map.copyOf(
                rawAttributes.entrySet().stream()
                    .filter(e -> !SchedulerStoreHelpers.ATTR_POLICY_DEFERRED.equals(e.getKey()))
                    .collect(
                        java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            : rawAttributes;
    var parentExecutionClass =
        lease.executionPolicy != null
            ? lease.executionPolicy.executionClass()
            : ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass.DEFAULT;
    return new ReconcileExecutionPolicy(
        parentExecutionClass,
        assignment.laneKey(),
        baseAttributes,
        assignment.priorityClass(),
        assignment.score());
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
