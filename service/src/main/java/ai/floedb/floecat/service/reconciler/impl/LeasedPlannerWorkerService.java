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
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class LeasedPlannerWorkerService extends BaseServiceImpl {
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerBackend backend;
  @Inject ConnectorRepository connectorRepo;
  @Inject IdempotencyRepository idempotencyStore;

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
    Boolean cancelled = cancelIfCanonicalConnectorMissing(lease);
    if (cancelled != null) {
      return cancelled;
    }
    cancelled = cancelIfCancellationRequested(lease);
    if (cancelled != null) {
      return cancelled;
    }
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
    String completionMessage =
        "Planned "
            + plannedTableJobs
            + " table job(s)"
            + (plannedViewJobs == 0L ? "" : " and " + plannedViewJobs + " view job(s)");
    boolean accepted =
        jobs.bulkEnqueueAndApplyLeaseOutcome(
            childSpecs,
            jobId,
            leaseEpoch,
            childSpecs.isEmpty()
                ? ReconcileJobStore.CompletionKind.SUCCEEDED
                : ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            System.currentTimeMillis(),
            completionMessage,
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
    return persistPlanFailure(
        jobId, leaseEpoch, failureKind, retryDisposition, retryClass, message);
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
      int chunkCount) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    Boolean cancelled = cancelIfCanonicalConnectorMissing(lease);
    if (cancelled != null) {
      return cancelled;
    }
    cancelled = cancelIfCancellationRequested(lease);
    if (cancelled != null) {
      return cancelled;
    }
    List<SubmitLeasedPlanTableResultRequest.Chunk> stagedChunks =
        loadStagedPlanTableChunks(principalContext, jobId, leaseEpoch, chunkCount);
    long plannedSnapshotJobs =
        stagedChunks.stream().mapToLong(chunk -> chunk.getSnapshotJobsCount()).sum();
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

  public boolean persistPlanTableSnapshotChunk(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      SubmitLeasedPlanTableResultRequest.Chunk chunk,
      List<PlannedSnapshotJob> snapshotJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_TABLE);
    Boolean cancelled = cancelIfCanonicalConnectorMissing(lease);
    if (cancelled != null) {
      return cancelled;
    }
    cancelled = cancelIfCancellationRequested(lease);
    if (cancelled != null) {
      return cancelled;
    }
    SubmitLeasedPlanTableResultRequest.Chunk stagedChunk =
        chunk == null
            ? SubmitLeasedPlanTableResultRequest.Chunk.newBuilder().build()
            : chunk.toBuilder().setChunkIndex(Math.max(0, chunk.getChunkIndex())).build();
    byte[] requestBytes = stagedChunk.toByteArray();
    java.util.ArrayList<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        new java.util.ArrayList<>(nullToEmpty(snapshotJobs).size());
    for (PlannedSnapshotJob snapshotJob : nullToEmpty(snapshotJobs)) {
      if (snapshotJob == null || snapshotJob.snapshotTask().isEmpty()) {
        continue;
      }
      childSpecs.add(
          ReconcileJobStore.BulkEnqueueSpec.of(
              lease.accountId,
              lease.connectorId,
              lease.fullRescan,
              lease.captureMode,
              snapshotJob.scope(),
              ReconcileJobKind.PLAN_SNAPSHOT,
              ReconcileTableTask.empty(),
              ReconcileViewTask.empty(),
              snapshotJob.snapshotTask(),
              ReconcileFileGroupTask.empty(),
              effectiveExecutionPolicy(lease),
              lease.jobId,
              lease.pinnedExecutorId));
    }
    return runIdempotentCreate(
                () ->
                    MutationOps.createProto(
                        principalContext.getAccountId(),
                        "SubmitLeasedPlanTableResult",
                        planTableChunkIdempotencyKey(
                            jobId, leaseEpoch, stagedChunk.getChunkIndex()),
                        () -> requestBytes,
                        () -> {
                          if (!childSpecs.isEmpty()) {
                            ReconcileJobStore.BulkEnqueueResult enqueueResult =
                                jobs.bulkEnqueue(childSpecs);
                            enqueueResult.requireAllSucceeded("table snapshot child enqueue");
                          }
                          return new IdempotencyGuard.CreateResult<>(
                              stagedChunk, connectorId(lease));
                        },
                        ignored -> MutationMeta.getDefaultInstance(),
                        idempotencyStore,
                        nowTs(),
                        idempotencyTtlSeconds(),
                        principalContext::getCorrelationId,
                        SubmitLeasedPlanTableResultRequest.Chunk::parseFrom))
            .body
        != null;
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
    return persistPlanFailure(
        jobId, leaseEpoch, failureKind, retryDisposition, retryClass, message);
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
    return persistPlanFailure(
        jobId, leaseEpoch, failureKind, retryDisposition, retryClass, message);
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
      ReconcileSnapshotTask plannedSnapshotTask,
      int chunkCount) {
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
                baseSnapshotTask.fileGroupCount(),
                baseSnapshotTask.sourceFileCount(),
                baseSnapshotTask.directStatsBlobUri(),
                baseSnapshotTask.directStatsRecordCount())
            : plannedSnapshotTask;
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
    Boolean cancelled = cancelIfCanonicalConnectorMissing(lease);
    if (cancelled != null) {
      return cancelled;
    }
    cancelled = cancelIfCancellationRequested(lease);
    if (cancelled != null) {
      return cancelled;
    }
    long plannedFileGroupJobs =
        durableSnapshotTask.completionMode() == ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
            ? Math.max(0L, durableSnapshotTask.fileGroupCount())
            : 0L;
    List<SubmitLeasedPlanSnapshotResultRequest.Chunk> stagedChunks =
        loadStagedPlanSnapshotChunks(principalContext, jobId, leaseEpoch, chunkCount);
    long stagedFileGroupJobs =
        stagedChunks.stream().mapToLong(chunk -> chunk.getFileGroupJobsCount()).sum();
    if (plannedFileGroupJobs != stagedFileGroupJobs) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot plan declared file_group_count="
                  + plannedFileGroupJobs
                  + " but staged "
                  + stagedFileGroupJobs
                  + " file group job(s)")
          .asRuntimeException();
    }
    boolean adopted =
        jobs.adoptSnapshotPlanManifest(
            lease.jobId,
            lease.leaseEpoch,
            durableSnapshotTask,
            durableSnapshotTask.fileGroupPlanBlobUri(),
            true);
    if (!adopted && !currentSnapshotPlanAdopted(jobId, durableSnapshotTask)) {
      return currentSnapshotPlanSuccess(jobId, durableSnapshotTask);
    }
    boolean enqueuedFinalizer = false;
    if (plannedFileGroupJobs == 0L) {
      jobs.enqueueSnapshotFinalization(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          effectiveScope(lease),
          durableSnapshotTask,
          effectiveExecutionPolicy(lease),
          lease.jobId,
          "");
      enqueuedFinalizer = true;
    }
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
            plannedFileGroupJobs > 0L || enqueuedFinalizer,
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

  public boolean persistPlanSnapshotFileGroupChunk(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      ReconcileSnapshotTask plannedSnapshotTask,
      SubmitLeasedPlanSnapshotResultRequest.Chunk chunk,
      List<PlannedFileGroupJob> plannedJobs) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedJob(
            principalContext.getCorrelationId(), jobId, leaseEpoch, ReconcileJobKind.PLAN_SNAPSHOT);
    Boolean cancelled = cancelIfCanonicalConnectorMissing(lease);
    if (cancelled != null) {
      return cancelled;
    }
    cancelled = cancelIfCancellationRequested(lease);
    if (cancelled != null) {
      return cancelled;
    }
    ReconcileSnapshotTask durableSnapshotTask = durableSnapshotTask(plannedSnapshotTask);
    SubmitLeasedPlanSnapshotResultRequest.Chunk stagedChunk =
        chunk == null
            ? SubmitLeasedPlanSnapshotResultRequest.Chunk.newBuilder().build()
            : chunk.toBuilder().setChunkIndex(Math.max(0, chunk.getChunkIndex())).build();
    byte[] requestBytes = stagedChunk.toByteArray();
    boolean adopted =
        jobs.adoptSnapshotPlanManifest(
            lease.jobId,
            lease.leaseEpoch,
            durableSnapshotTask,
            durableSnapshotTask.fileGroupPlanBlobUri(),
            true);
    if (!adopted && !currentSnapshotPlanAdopted(jobId, durableSnapshotTask)) {
      return currentSnapshotPlanSuccess(jobId, durableSnapshotTask);
    }
    java.util.Set<String> existingFileGroupKeys =
        new java.util.LinkedHashSet<>(existingExecFileGroupKeys(lease.accountId, lease.jobId));
    java.util.ArrayList<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        new java.util.ArrayList<>(nullToEmpty(plannedJobs).size());
    for (PlannedFileGroupJob fileGroupJob : nullToEmpty(plannedJobs)) {
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
    return runIdempotentCreate(
                () ->
                    MutationOps.createProto(
                        principalContext.getAccountId(),
                        "SubmitLeasedPlanSnapshotResult",
                        planSnapshotChunkIdempotencyKey(
                            jobId, leaseEpoch, stagedChunk.getChunkIndex()),
                        () -> requestBytes,
                        () -> {
                          if (!childSpecs.isEmpty()) {
                            ReconcileJobStore.BulkEnqueueResult enqueueResult =
                                jobs.bulkEnqueue(childSpecs);
                            enqueueResult.requireAllSucceeded("snapshot file-group child enqueue");
                          }
                          return new IdempotencyGuard.CreateResult<>(
                              stagedChunk, tableId(lease, durableSnapshotTask));
                        },
                        ignored -> MutationMeta.getDefaultInstance(),
                        idempotencyStore,
                        nowTs(),
                        idempotencyTtlSeconds(),
                        principalContext::getCorrelationId,
                        SubmitLeasedPlanSnapshotResultRequest.Chunk::parseFrom))
            .body
        != null;
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

  private java.util.Set<String> existingExecFileGroupKeys(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return java.util.Set.of();
    }
    java.util.Set<String> existingKeys = new java.util.LinkedHashSet<>();
    String pageToken = "";
    do {
      ReconcileJobStore.ReconcileJobPage page =
          jobs.childJobsPage(accountId, parentJobId, 200, pageToken);
      if (page == null || page.jobs == null || page.jobs.isEmpty()) {
        break;
      }
      for (ReconcileJobStore.ReconcileJob child : page.jobs) {
        if (child == null) {
          continue;
        }
        if (child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP
            || child.fileGroupTask == null
            || child.fileGroupTask.isEmpty()) {
          continue;
        }
        String key = execFileGroupKey(child.fileGroupTask);
        if (key.isBlank()) {
          continue;
        }
        existingKeys.add(key);
      }
      pageToken = page.nextPageToken == null ? "" : page.nextPageToken;
    } while (!pageToken.isBlank());
    return java.util.Set.copyOf(existingKeys);
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
        effective.sourceFileCount(),
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
    return persistPlanFailure(
        jobId, leaseEpoch, failureKind, retryDisposition, retryClass, message);
  }

  record PlannedTableJob(ReconcileScope scope, ReconcileTableTask tableTask) {}

  record PlannedViewJob(ReconcileScope scope, ReconcileViewTask viewTask) {}

  record PlannedSnapshotJob(ReconcileScope scope, ReconcileSnapshotTask snapshotTask) {}

  private List<SubmitLeasedPlanTableResultRequest.Chunk> loadStagedPlanTableChunks(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkCount) {
    int expectedChunkCount = Math.max(0, chunkCount);
    if (expectedChunkCount == 0) {
      requireNoStagedPlanTableChunkAt(principalContext, jobId, leaseEpoch, 0, expectedChunkCount);
      return List.of();
    }
    java.util.ArrayList<SubmitLeasedPlanTableResultRequest.Chunk> chunks =
        new java.util.ArrayList<>(expectedChunkCount);
    for (int chunkIndex = 0; chunkIndex < expectedChunkCount; chunkIndex++) {
      chunks.add(loadStagedPlanTableChunk(principalContext, jobId, leaseEpoch, chunkIndex));
    }
    requireNoStagedPlanTableChunkAt(
        principalContext, jobId, leaseEpoch, expectedChunkCount, expectedChunkCount);
    return List.copyOf(chunks);
  }

  private List<SubmitLeasedPlanSnapshotResultRequest.Chunk> loadStagedPlanSnapshotChunks(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkCount) {
    int expectedChunkCount = Math.max(0, chunkCount);
    if (expectedChunkCount == 0) {
      requireNoStagedPlanSnapshotChunkAt(
          principalContext, jobId, leaseEpoch, 0, expectedChunkCount);
      return List.of();
    }
    java.util.ArrayList<SubmitLeasedPlanSnapshotResultRequest.Chunk> chunks =
        new java.util.ArrayList<>(expectedChunkCount);
    for (int chunkIndex = 0; chunkIndex < expectedChunkCount; chunkIndex++) {
      chunks.add(loadStagedPlanSnapshotChunk(principalContext, jobId, leaseEpoch, chunkIndex));
    }
    requireNoStagedPlanSnapshotChunkAt(
        principalContext, jobId, leaseEpoch, expectedChunkCount, expectedChunkCount);
    return List.copyOf(chunks);
  }

  private void requireNoStagedPlanTableChunkAt(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      int chunkIndex,
      int declaredChunkCount) {
    if (stagedPlanTableChunkExists(principalContext, jobId, leaseEpoch, chunkIndex)) {
      throw new IllegalArgumentException(
          "plan-table success declared chunk_count="
              + declaredChunkCount
              + " but a staged chunk exists at index "
              + chunkIndex
              + "; the declared count is too low and would drop child snapshot jobs");
    }
  }

  private void requireNoStagedPlanSnapshotChunkAt(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      int chunkIndex,
      int declaredChunkCount) {
    if (stagedPlanSnapshotChunkExists(principalContext, jobId, leaseEpoch, chunkIndex)) {
      throw new IllegalArgumentException(
          "plan-snapshot success declared chunk_count="
              + declaredChunkCount
              + " but a staged chunk exists at index "
              + chunkIndex
              + "; the declared count is too low and would drop child file-group jobs");
    }
  }

  private boolean stagedPlanTableChunkExists(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedPlanTableResult",
            planTableChunkIdempotencyKey(jobId, leaseEpoch, chunkIndex));
    return idempotencyStore
        .get(idempotencyKey)
        .map(record -> record.getStatus() == IdempotencyRecord.Status.SUCCEEDED)
        .orElse(false);
  }

  private boolean stagedPlanSnapshotChunkExists(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedPlanSnapshotResult",
            planSnapshotChunkIdempotencyKey(jobId, leaseEpoch, chunkIndex));
    return idempotencyStore
        .get(idempotencyKey)
        .map(record -> record.getStatus() == IdempotencyRecord.Status.SUCCEEDED)
        .orElse(false);
  }

  private SubmitLeasedPlanTableResultRequest.Chunk loadStagedPlanTableChunk(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedPlanTableResult",
            planTableChunkIdempotencyKey(jobId, leaseEpoch, chunkIndex));
    IdempotencyRecord record =
        idempotencyStore
            .get(idempotencyKey)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "plan-table result chunk not yet staged: key=" + idempotencyKey));
    if (record.getStatus() != IdempotencyRecord.Status.SUCCEEDED) {
      throw new StorageAbortRetryableException(
          "plan-table result chunk is not complete: key=" + idempotencyKey);
    }
    try {
      SubmitLeasedPlanTableResultRequest.Chunk chunk =
          SubmitLeasedPlanTableResultRequest.Chunk.parseFrom(record.getPayload());
      if (chunk.getChunkIndex() != chunkIndex) {
        throw new IllegalArgumentException("staged plan-table result chunk identity mismatch");
      }
      return chunk;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException(
          "failed to parse staged plan-table result chunk", e);
    }
  }

  private SubmitLeasedPlanSnapshotResultRequest.Chunk loadStagedPlanSnapshotChunk(
      PrincipalContext principalContext, String jobId, String leaseEpoch, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedPlanSnapshotResult",
            planSnapshotChunkIdempotencyKey(jobId, leaseEpoch, chunkIndex));
    IdempotencyRecord record =
        idempotencyStore
            .get(idempotencyKey)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "plan-snapshot result chunk not yet staged: key=" + idempotencyKey));
    if (record.getStatus() != IdempotencyRecord.Status.SUCCEEDED) {
      throw new StorageAbortRetryableException(
          "plan-snapshot result chunk is not complete: key=" + idempotencyKey);
    }
    try {
      SubmitLeasedPlanSnapshotResultRequest.Chunk chunk =
          SubmitLeasedPlanSnapshotResultRequest.Chunk.parseFrom(record.getPayload());
      if (chunk.getChunkIndex() != chunkIndex) {
        throw new IllegalArgumentException("staged plan-snapshot result chunk identity mismatch");
      }
      return chunk;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException(
          "failed to parse staged plan-snapshot result chunk", e);
    }
  }

  private static String planTableChunkIdempotencyKey(
      String jobId, String leaseEpoch, int chunkIndex) {
    return blankToEmpty(jobId)
        + ":"
        + blankToEmpty(leaseEpoch)
        + ":chunk:"
        + Math.max(0, chunkIndex);
  }

  private static String planSnapshotChunkIdempotencyKey(
      String jobId, String leaseEpoch, int chunkIndex) {
    return blankToEmpty(jobId)
        + ":"
        + blankToEmpty(leaseEpoch)
        + ":chunk:"
        + Math.max(0, chunkIndex);
  }

  private ReconcileJobStore.LeasedJob requireLeasedJob(
      String corr, String jobId, String leaseEpoch, ReconcileJobKind expectedKind) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed("reconcile lease is no longer valid");
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
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile job is no longer active for lease "
              + jobId
              + " state="
              + blankToEmpty(job.state));
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
                    ReconcileLeaseGrpcStatus.leasePreconditionFailed(
                        "reconcile lease is no longer valid"));
    if (lease.jobKind != expectedKind) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a " + expectedKind + " job")
          .asRuntimeException();
    }
    return lease;
  }

  private Boolean cancelIfCanonicalConnectorMissing(ReconcileJobStore.LeasedJob lease) {
    if (canonicalConnectorExists(lease)) {
      return null;
    }
    return jobs.applyLeaseOutcome(
        lease.jobId,
        lease.leaseEpoch,
        ReconcileJobStore.CompletionKind.CANCELLED,
        System.currentTimeMillis(),
        "connector deleted: " + lease.connectorId,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);
  }

  private Boolean cancelIfCancellationRequested(ReconcileJobStore.LeasedJob lease) {
    if (lease == null || !jobs.isCancellationRequested(lease.jobId)) {
      return null;
    }
    return jobs.applyLeaseOutcome(
        lease.jobId,
        lease.leaseEpoch,
        ReconcileJobStore.CompletionKind.CANCELLED,
        System.currentTimeMillis(),
        "Cancelled",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);
  }

  private boolean canonicalConnectorExists(ReconcileJobStore.LeasedJob lease) {
    if (connectorRepo == null || lease == null) {
      return true;
    }
    if (lease.accountId == null
        || lease.accountId.isBlank()
        || lease.connectorId == null
        || lease.connectorId.isBlank()) {
      return false;
    }
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(lease.connectorId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    return connectorRepo.existsById(connectorId);
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

  private boolean currentSnapshotPlanAdopted(
      String jobId, ReconcileSnapshotTask durableSnapshotTask) {
    return jobs.getLeaseView(jobId)
        .filter(current -> current.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
        .map(current -> durableSnapshotTask.equals(current.snapshotTask))
        .orElse(false);
  }

  private boolean persistPlanFailure(
      String jobId,
      String leaseEpoch,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
          retryDisposition,
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    long finishedAtMs = System.currentTimeMillis();
    if (isObsoleteFailureKind(failureKind)) {
      return jobs.applyLeaseOutcome(
          jobId,
          leaseEpoch,
          ReconcileJobStore.CompletionKind.CANCELLED,
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
    if (retryDisposition
        == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
            .TERMINAL) {
      return jobs.applyLeaseOutcome(
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
    }
    if (retryClass
        == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
            .DEPENDENCY_NOT_READY) {
      return jobs.applyLeaseOutcome(
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
    }
    return jobs.applyLeaseOutcome(
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

  private static boolean isObsoleteFailureKind(
      ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind failureKind) {
    return failureKind
            == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .CONNECTOR_MISSING
        || failureKind
            == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .TABLE_MISSING
        || failureKind
            == ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .VIEW_MISSING;
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

  private static ResourceId tableId(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId)
        .setId(effective.tableId())
        .setKind(ResourceKind.RK_TABLE)
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
