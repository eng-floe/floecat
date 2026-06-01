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
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPolicyRegistry;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.jboss.logging.Logger;

@ApplicationScoped
public class LeasedPlannerWorkerService {
  private static final Logger LOG = Logger.getLogger(LeasedPlannerWorkerService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerBackend backend;
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;
  @Inject Instance<SchedulerPolicyRegistry> schedulerRegistryInstance;

  @Inject
  Instance<ai.floedb.floecat.service.statistics.scheduler.SchedulerSignalIndex> signalIndexInstance;

  @Inject Instance<SnapshotRepository> snapshotRepoInstance;

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
    // Use childExecutionPolicy() — not effectiveExecutionPolicy() — so scheduler-internal
    // attributes like policy_deferred are stripped before they reach child jobs. Each child
    // must be evaluated against the admission policy independently at its own enqueue time.
    ReconcileExecutionPolicy childPolicy = childExecutionPolicy(lease);
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
              childPolicy,
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
              childPolicy,
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
            !childSpecs.isEmpty(),
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
      // PLAN_SNAPSHOT jobs always represent freshly discovered snapshots → P1_FRESHNESS.
      ReconcileExecutionPolicy snapshotPolicy =
          assignForReconcileJobSafe(
              ReconcileJobKind.PLAN_SNAPSHOT,
              snapshotJob.snapshotTask().tableId(),
              snapshotJob.snapshotTask().snapshotId(),
              /* isNewSnapshot= */ true,
              effectiveExecutionPolicy(lease));
      // Record delta signal for this snapshot (unknown at planning time; signals
      // the snapshot as discovered so the delta dimension can be updated later).
      recordSnapshotDeltaSafe(
          lease.accountId,
          snapshotJob.snapshotTask().tableId(),
          snapshotJob.snapshotTask().snapshotId());
      jobs.enqueueSnapshotPlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          snapshotJob.scope(),
          snapshotJob.snapshotTask(),
          snapshotPolicy,
          lease.jobId,
          lease.pinnedExecutorId);
    }
    boolean accepted =
        completePlanSuccess(
            jobId,
            leaseEpoch,
            "Planned " + plannedSnapshotJobs + " snapshot job(s)",
            plannedSnapshotJobs > 0L,
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
            jobId, leaseEpoch, "Planned view", false, 0L, 0L, 1L, changed ? 1L : 0L, 0L, 0L, 0L);
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
            !childSpecs.isEmpty(),
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
    if (effective.fileGroupCount() > 0
        && blankToEmpty(effective.fileGroupPlanBlobUri()).isBlank()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot plan success requires a persisted file-group manifest")
          .asRuntimeException();
    }
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        List.of(),
        true,
        effective.completionMode(),
        effective.fileGroupPlanBlobUri(),
        effective.fileGroupCount(),
        effective.directStatsBlobUri(),
        effective.directStatsRecordCount());
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
        .filter(
            current -> "JS_SUCCEEDED".equals(current.state) || "JS_WAITING".equals(current.state))
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
          ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY,
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
      boolean handedOffChildWork,
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
        handedOffChildWork
            ? ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING
            : ReconcileJobStore.CompletionKind.SUCCEEDED,
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

  private static ReconcileExecutionPolicy effectiveExecutionPolicy(
      ReconcileJobStore.LeasedJob lease) {
    return lease == null || lease.executionPolicy == null
        ? ReconcileExecutionPolicy.defaults()
        : lease.executionPolicy;
  }

  /**
   * Returns a child execution policy derived from the parent lease, with scheduler-internal
   * attributes stripped. Used for PLAN_TABLE and PLAN_VIEW children of PLAN_CONNECTOR jobs so that
   * the parent's {@code policy_deferred} flag is not inherited — each child is evaluated against
   * the admission policy independently at its own enqueue time.
   */
  private static ReconcileExecutionPolicy childExecutionPolicy(ReconcileJobStore.LeasedJob lease) {
    ReconcileExecutionPolicy parent = effectiveExecutionPolicy(lease);
    java.util.Map<String, String> stripped = strippedChildAttributes(parent);
    // If no stripping was needed, reuse the parent instance to avoid allocation.
    if (stripped == parent.attributes()) {
      return parent;
    }
    return ReconcileExecutionPolicy.of(
        parent.priorityClass(), parent.lane(), stripped, parent.priorityScore());
  }

  /**
   * Calls the scheduler priority policy's {@code assignForReconcileJob()} and builds a {@link
   * ReconcileExecutionPolicy} from the result. Falls back to the parent lease's policy (with the
   * class overridden to {@link StatsPriorityClass#P1_FRESHNESS} for new snapshots) when no registry
   * is configured or the call throws.
   */
  private ReconcileExecutionPolicy assignForReconcileJobSafe(
      ReconcileJobKind kind,
      String tableId,
      long snapshotId,
      boolean isNewSnapshot,
      ReconcileExecutionPolicy parentPolicy) {
    try {
      SchedulerPolicyRegistry registry =
          schedulerRegistryInstance == null || schedulerRegistryInstance.isUnsatisfied()
              ? null
              : schedulerRegistryInstance.get();
      if (registry != null) {
        var assignment =
            registry
                .activePriorityPolicy()
                .assignForReconcileJob(
                    kind.name(), tableId, snapshotId, isNewSnapshot, registry.activeContext());
        return ReconcileExecutionPolicy.of(
            assignment.priorityClass(),
            assignment.laneKey(),
            strippedChildAttributes(parentPolicy),
            assignment.score());
      }
    } catch (RuntimeException e) {
      LOG.debugf(
          e, "assignForReconcileJob failed for kind=%s table=%s; using fallback", kind, tableId);
    }
    // Fallback: P1_FRESHNESS for new snapshots, otherwise inherit parent class.
    StatsPriorityClass cls =
        isNewSnapshot
            ? StatsPriorityClass.P1_FRESHNESS
            : (parentPolicy != null
                ? parentPolicy.priorityClass()
                : StatsPriorityClass.P3_BACKGROUND);
    String laneKey = tableId;
    return ReconcileExecutionPolicy.of(cls, laneKey, strippedChildAttributes(parentPolicy), 0L);
  }

  /**
   * Attempts to compute and record the snapshot delta row count in {@link
   * ai.floedb.floecat.service.statistics.scheduler.SchedulerSignalIndex}. Uses current and parent
   * snapshot {@code total-records} summaries when available; falls back to unknown on any missing
   * field/parse error.
   *
   * <p>This is best-effort only and must never fail planner success persistence.
   */
  private void recordSnapshotDeltaSafe(String accountId, String tableId, long snapshotId) {
    if (signalIndexInstance == null || signalIndexInstance.isUnsatisfied()) {
      return;
    }
    if (snapshotRepoInstance == null || snapshotRepoInstance.isUnsatisfied()) {
      return;
    }
    try {
      SnapshotRepository snapshotRepo = snapshotRepoInstance.get();
      var signalIndex = signalIndexInstance.get();
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
      String currentTotalRaw = current.getSummaryMap().get("total-records");
      if (currentTotalRaw == null || currentTotalRaw.isBlank()) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      long currentTotal;
      try {
        currentTotal = Long.parseLong(currentTotalRaw.trim());
      } catch (NumberFormatException ignored) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      if (!current.hasParentSnapshotId()) {
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
      String parentTotalRaw = parentOpt.get().getSummaryMap().get("total-records");
      if (parentTotalRaw == null || parentTotalRaw.isBlank()) {
        signalIndex.recordSnapshotDelta(accountId, tableId, snapshotId, OptionalLong.empty());
        return;
      }
      long parentTotal;
      try {
        parentTotal = Long.parseLong(parentTotalRaw.trim());
      } catch (NumberFormatException ignored) {
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

  /**
   * Returns a copy of the parent policy's attributes with scheduler-internal keys stripped. {@code
   * policy_deferred} is an admission-control decision specific to the parent job's enqueue context
   * — child jobs must be evaluated independently and must not inherit a stale deferral flag from
   * their parent.
   */
  private static java.util.Map<String, String> strippedChildAttributes(
      ReconcileExecutionPolicy parentPolicy) {
    if (parentPolicy == null || parentPolicy.attributes().isEmpty()) {
      return java.util.Map.of();
    }
    if (!parentPolicy
        .attributes()
        .containsKey(
            ai.floedb.floecat.reconciler.jobs.impl.SchedulerStoreHelpers.ATTR_POLICY_DEFERRED)) {
      return parentPolicy.attributes(); // fast path — no stripping needed
    }
    java.util.Map<String, String> stripped = new java.util.HashMap<>(parentPolicy.attributes());
    stripped.remove(
        ai.floedb.floecat.reconciler.jobs.impl.SchedulerStoreHelpers.ATTR_POLICY_DEFERRED);
    return java.util.Map.copyOf(stripped);
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
