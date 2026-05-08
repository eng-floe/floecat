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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedFileGroupExecutionRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedFileGroupExecutionResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanSnapshotInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanSnapshotInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanViewInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanViewInputResponse;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationResponse;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.LeasedReconcileJob;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControl;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureKind;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryClass;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryDisposition;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseResponse;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressResponse;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanConnectorResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanConnectorResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Set;

@GrpcService
public class ReconcileExecutorControlImpl extends BaseServiceImpl
    implements ReconcileExecutorControl {

  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcileCancellationRegistry cancellations;
  @Inject LeasedFileGroupExecutionService leasedFileGroupExecutionService;
  @Inject LeasedPlannerWorkerService leasedPlannerWorkerService;

  @Override
  public Uni<LeaseReconcileJobResponse> leaseReconcileJob(LeaseReconcileJobRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              Set<String> executorIds;
              if (request == null) {
                executorIds = Set.of();
              } else {
                java.util.LinkedHashSet<String> mergedExecutorIds = new java.util.LinkedHashSet<>();
                request.getExecutorIdsList().stream()
                    .map(executorId -> executorId == null ? "" : executorId.trim())
                    .filter(executorId -> !executorId.isEmpty())
                    .forEach(mergedExecutorIds::add);
                String executorId =
                    request.getExecutorId() == null ? "" : request.getExecutorId().trim();
                if (!executorId.isBlank()) {
                  mergedExecutorIds.add(executorId);
                }
                executorIds = Set.copyOf(mergedExecutorIds);
              }
              var leaseRequest =
                  ReconcileJobStore.LeaseRequest.of(
                      executionClassesFrom(request),
                      lanesFrom(request),
                      executorIds,
                      jobKindsFrom(request));
              var lease = jobs.leaseNext(leaseRequest);
              if (lease.isEmpty()) {
                return LeaseReconcileJobResponse.newBuilder().setFound(false).build();
              }
              return LeaseReconcileJobResponse.newBuilder()
                  .setFound(true)
                  .setJob(toProtoLease(lease.get()))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<RenewReconcileLeaseResponse> renewReconcileLease(RenewReconcileLeaseRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              boolean renewed = jobs.renewLease(jobId, leaseEpoch);
              return RenewReconcileLeaseResponse.newBuilder()
                  .setRenewed(renewed)
                  .setCancellationRequested(jobs.isCancellationRequested(jobId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<StartLeasedReconcileJobResponse> startLeasedReconcileJob(
      StartLeasedReconcileJobRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              String executorId = mustNonEmpty(request.getExecutorId(), "executor_id", corr);
              jobs.markRunning(jobId, leaseEpoch, System.currentTimeMillis(), executorId);
              return StartLeasedReconcileJobResponse.getDefaultInstance();
            }),
        correlationId());
  }

  @Override
  public Uni<ReportReconcileProgressResponse> reportReconcileProgress(
      ReportReconcileProgressRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              boolean leaseValid = jobs.renewLease(jobId, leaseEpoch);
              if (leaseValid) {
                jobs.markProgress(
                    jobId,
                    leaseEpoch,
                    request.getTablesScanned(),
                    request.getTablesChanged(),
                    request.getViewsScanned(),
                    request.getViewsChanged(),
                    request.getErrors(),
                    request.getSnapshotsProcessed(),
                    request.getStatsProcessed(),
                    request.getMessage());
              }
              return ReportReconcileProgressResponse.newBuilder()
                  .setLeaseValid(leaseValid)
                  .setCancellationRequested(jobs.isCancellationRequested(jobId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<CompleteLeasedReconcileJobResponse> completeLeasedReconcileJob(
      CompleteLeasedReconcileJobRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.getState() == ReconcileCompletionState.RCS_UNSPECIFIED
                  || request.getState() == ReconcileCompletionState.UNRECOGNIZED) {
                throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "state"));
              }
              boolean leaseValid = jobs.renewLease(jobId, leaseEpoch);
              if (!leaseValid) {
                return CompleteLeasedReconcileJobResponse.newBuilder().setAccepted(false).build();
              }
              long finishedAtMs = System.currentTimeMillis();
              switch (request.getState()) {
                case RCS_SUCCEEDED ->
                    jobs.markSucceeded(
                        jobId,
                        leaseEpoch,
                        finishedAtMs,
                        request.getTablesScanned(),
                        request.getTablesChanged(),
                        request.getViewsScanned(),
                        request.getViewsChanged(),
                        request.getSnapshotsProcessed(),
                        request.getStatsProcessed());
                case RCS_FAILED -> {
                  if (isDependencyNotReady(request)) {
                    jobs.markWaiting(
                        jobId,
                        leaseEpoch,
                        finishedAtMs,
                        request.getMessage(),
                        request.getTablesScanned(),
                        request.getTablesChanged(),
                        request.getViewsScanned(),
                        request.getViewsChanged(),
                        request.getErrors(),
                        request.getSnapshotsProcessed(),
                        request.getStatsProcessed());
                  } else if (isTerminalFailure(request)) {
                    jobs.markFailedTerminal(
                        jobId,
                        leaseEpoch,
                        finishedAtMs,
                        request.getMessage(),
                        request.getTablesScanned(),
                        request.getTablesChanged(),
                        request.getViewsScanned(),
                        request.getViewsChanged(),
                        request.getErrors(),
                        request.getSnapshotsProcessed(),
                        request.getStatsProcessed());
                    cancelChildJobs(jobId, request.getMessage());
                  } else {
                    jobs.markFailed(
                        jobId,
                        leaseEpoch,
                        finishedAtMs,
                        request.getMessage(),
                        request.getTablesScanned(),
                        request.getTablesChanged(),
                        request.getViewsScanned(),
                        request.getViewsChanged(),
                        request.getErrors(),
                        request.getSnapshotsProcessed(),
                        request.getStatsProcessed());
                  }
                }
                case RCS_CANCELLED -> {
                  jobs.markCancelled(
                      jobId,
                      leaseEpoch,
                      finishedAtMs,
                      request.getMessage(),
                      request.getTablesScanned(),
                      request.getTablesChanged(),
                      request.getViewsScanned(),
                      request.getViewsChanged(),
                      request.getErrors(),
                      request.getSnapshotsProcessed(),
                      request.getStatsProcessed());
                  cancelChildJobs(jobId, request.getMessage());
                }
                case RCS_UNSPECIFIED, UNRECOGNIZED ->
                    throw GrpcErrors.invalidArgument(
                        corr, null, java.util.Map.of("field", "state"));
              }
              return CompleteLeasedReconcileJobResponse.newBuilder().setAccepted(true).build();
            }),
        correlationId());
  }

  private void cancelChildJobs(String jobId, String reason) {
    jobs.get(null, jobId).ifPresent(job -> cancelChildJobs(job.accountId, job, reason));
  }

  private void cancelChildJobs(
      String accountId, ReconcileJobStore.ReconcileJob job, String reason) {
    if (job == null
        || accountId == null
        || accountId.isBlank()
        || !supportsChildCancellation(job.jobKind)) {
      return;
    }
    String message = (reason == null || reason.isBlank()) ? "Parent plan job terminated" : reason;
    for (var child : jobs.childJobs(accountId, job.jobId)) {
      var storedChild = jobs.get(accountId, child.jobId).orElse(child);
      var cancelled = jobs.cancel(accountId, child.jobId, message);
      if (cancelled.isPresent() && "JS_CANCELLING".equals(cancelled.get().state)) {
        cancellations.requestCancel(cancelled.get().jobId);
      } else if (cancelled.isEmpty()
          && storedChild != null
          && supportsChildCancellation(storedChild.jobKind)
          && !isTerminalState(storedChild.state)) {
        cancelChildJobs(accountId, storedChild, message);
      }
    }
  }

  private static boolean supportsChildCancellation(ReconcileJobKind jobKind) {
    return jobKind == ReconcileJobKind.PLAN_CONNECTOR
        || jobKind == ReconcileJobKind.PLAN_TABLE
        || jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  private static boolean isTerminalState(String state) {
    return "JS_SUCCEEDED".equals(state)
        || "JS_FAILED".equals(state)
        || "JS_CANCELLED".equals(state);
  }

  private static boolean isTerminalFailure(CompleteLeasedReconcileJobRequest request) {
    return request != null
        && request.getFailureRetryDisposition() == ReconcileFailureRetryDisposition.RFRD_TERMINAL;
  }

  private static boolean isDependencyNotReady(CompleteLeasedReconcileJobRequest request) {
    return request != null
        && request.getFailureRetryDisposition() == ReconcileFailureRetryDisposition.RFRD_RETRYABLE
        && request.getFailureRetryClass() == ReconcileFailureRetryClass.RFRC_DEPENDENCY_NOT_READY;
  }

  private static ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
      fromProtoFailureKind(ReconcileFailureKind failureKind) {
    return switch (failureKind) {
      case RFK_CONNECTOR_MISSING ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
              .CONNECTOR_MISSING;
      case RFK_TABLE_MISSING ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
              .TABLE_MISSING;
      case RFK_VIEW_MISSING ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
              .VIEW_MISSING;
      case RFK_INTERNAL ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL;
      case RFK_UNSPECIFIED, UNRECOGNIZED ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind.NONE;
    };
  }

  private static ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult
          .RetryDisposition
      fromProtoRetryDisposition(ReconcileFailureRetryDisposition retryDisposition) {
    return switch (retryDisposition) {
      case RFRD_TERMINAL ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
              .TERMINAL;
      case RFRD_RETRYABLE, RFRD_UNSPECIFIED, UNRECOGNIZED ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryDisposition
              .RETRYABLE;
    };
  }

  private static ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
      fromProtoRetryClass(ReconcileFailureRetryClass retryClass) {
    return switch (retryClass) {
      case RFRC_TRANSIENT_ERROR ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
              .TRANSIENT_ERROR;
      case RFRC_DEPENDENCY_NOT_READY ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
              .DEPENDENCY_NOT_READY;
      case RFRC_STATE_UNCERTAIN ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass
              .STATE_UNCERTAIN;
      case RFRC_UNSPECIFIED, UNRECOGNIZED ->
          ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.RetryClass.NONE;
    };
  }

  @Override
  public Uni<GetReconcileCancellationResponse> getReconcileCancellation(
      GetReconcileCancellationRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              return GetReconcileCancellationResponse.newBuilder()
                  .setCancellationRequested(jobs.isCancellationRequested(jobId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetLeasedPlanConnectorInputResponse> getLeasedPlanConnectorInput(
      GetLeasedPlanConnectorInputRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              var payload =
                  leasedPlannerWorkerService.resolvePlanConnector(
                      principalContext, jobId, leaseEpoch);
              return GetLeasedPlanConnectorInputResponse.newBuilder()
                  .setInput(
                      ai.floedb.floecat.reconciler.rpc.LeasedPlanConnectorInput.newBuilder()
                          .setJobId(payload.jobId())
                          .setLeaseEpoch(payload.leaseEpoch())
                          .setConnectorId(payload.connectorId())
                          .setMode(toProtoCaptureMode(payload.captureMode()))
                          .setFullRescan(payload.fullRescan())
                          .setScope(toProtoScope(payload.scope(), payload.connectorId()))
                          .setExecutionPolicy(toProtoExecutionPolicy(payload.executionPolicy()))
                          .setPinnedExecutorId(payload.pinnedExecutorId())
                          .build())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<SubmitLeasedPlanConnectorResultResponse> submitLeasedPlanConnectorResult(
      SubmitLeasedPlanConnectorResultRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.hasSuccess()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanConnectorSuccess(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getSuccess().getTableJobsList().stream()
                            .map(
                                job ->
                                    new LeasedPlannerWorkerService.PlannedTableJob(
                                        fromProtoScope(job.getScope()),
                                        fromProtoTableTask(job.getTableTask())))
                            .toList(),
                        request.getSuccess().getViewJobsList().stream()
                            .map(
                                job ->
                                    new LeasedPlannerWorkerService.PlannedViewJob(
                                        fromProtoScope(job.getScope()),
                                        fromProtoViewTask(job.getViewTask())))
                            .toList());
                return SubmitLeasedPlanConnectorResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              if (request.hasFailure()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanConnectorFailure(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        fromProtoFailureKind(request.getFailure().getFailureKind()),
                        fromProtoRetryDisposition(request.getFailure().getRetryDisposition()),
                        fromProtoRetryClass(request.getFailure().getRetryClass()),
                        request.getFailure().getMessage());
                return SubmitLeasedPlanConnectorResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "outcome"));
            }),
        correlationId());
  }

  @Override
  public Uni<GetLeasedPlanTableInputResponse> getLeasedPlanTableInput(
      GetLeasedPlanTableInputRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              var payload =
                  leasedPlannerWorkerService.resolvePlanTable(principalContext, jobId, leaseEpoch);
              return GetLeasedPlanTableInputResponse.newBuilder()
                  .setInput(
                      ai.floedb.floecat.reconciler.rpc.LeasedPlanTableInput.newBuilder()
                          .setJobId(payload.jobId())
                          .setLeaseEpoch(payload.leaseEpoch())
                          .setParentJobId(payload.parentJobId())
                          .setConnectorId(payload.connectorId())
                          .setMode(toProtoCaptureMode(payload.captureMode()))
                          .setFullRescan(payload.fullRescan())
                          .setScope(toProtoScope(payload.scope(), payload.connectorId()))
                          .setTableTask(toProtoTableTask(payload.tableTask()))
                          .build())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<SubmitLeasedPlanTableResultResponse> submitLeasedPlanTableResult(
      SubmitLeasedPlanTableResultRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.hasSuccess()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanTableSuccess(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getSuccess().getSnapshotJobsList().stream()
                            .map(
                                job ->
                                    new LeasedPlannerWorkerService.PlannedSnapshotJob(
                                        fromProtoScope(job.getScope()),
                                        fromProtoSnapshotTask(job.getSnapshotTask())))
                            .toList());
                return SubmitLeasedPlanTableResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              if (request.hasFailure()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanTableFailure(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        fromProtoFailureKind(request.getFailure().getFailureKind()),
                        fromProtoRetryDisposition(request.getFailure().getRetryDisposition()),
                        fromProtoRetryClass(request.getFailure().getRetryClass()),
                        request.getFailure().getMessage());
                return SubmitLeasedPlanTableResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "outcome"));
            }),
        correlationId());
  }

  @Override
  public Uni<GetLeasedPlanViewInputResponse> getLeasedPlanViewInput(
      GetLeasedPlanViewInputRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              var payload =
                  leasedPlannerWorkerService.resolvePlanView(principalContext, jobId, leaseEpoch);
              return GetLeasedPlanViewInputResponse.newBuilder()
                  .setInput(
                      ai.floedb.floecat.reconciler.rpc.LeasedPlanViewInput.newBuilder()
                          .setJobId(payload.jobId())
                          .setLeaseEpoch(payload.leaseEpoch())
                          .setParentJobId(payload.parentJobId())
                          .setConnectorId(payload.connectorId())
                          .setScope(toProtoScope(payload.scope(), payload.connectorId()))
                          .setViewTask(toProtoViewTask(payload.viewTask()))
                          .build())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<SubmitLeasedPlanViewResultResponse> submitLeasedPlanViewResult(
      SubmitLeasedPlanViewResultRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.hasSuccess()) {
                var result =
                    leasedPlannerWorkerService.persistPlanViewSuccess(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getSuccess().hasMutation()
                            ? new LeasedPlannerWorkerService.PlannedViewMutation(
                                request.getSuccess().getMutation().hasDestinationViewId()
                                    ? request.getSuccess().getMutation().getDestinationViewId()
                                    : null,
                                request.getSuccess().getMutation().hasViewSpec()
                                    ? request.getSuccess().getMutation().getViewSpec()
                                    : null,
                                request.getSuccess().getMutation().getIdempotencyKey())
                            : null);
                return SubmitLeasedPlanViewResultResponse.newBuilder()
                    .setAccepted(result.accepted())
                    .setViewsChanged(result.viewsChanged())
                    .build();
              }
              if (request.hasFailure()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanViewFailure(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        fromProtoFailureKind(request.getFailure().getFailureKind()),
                        fromProtoRetryDisposition(request.getFailure().getRetryDisposition()),
                        fromProtoRetryClass(request.getFailure().getRetryClass()),
                        request.getFailure().getMessage());
                return SubmitLeasedPlanViewResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "outcome"));
            }),
        correlationId());
  }

  @Override
  public Uni<GetLeasedPlanSnapshotInputResponse> getLeasedPlanSnapshotInput(
      GetLeasedPlanSnapshotInputRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              var payload =
                  leasedPlannerWorkerService.resolvePlanSnapshot(
                      principalContext, jobId, leaseEpoch);
              return GetLeasedPlanSnapshotInputResponse.newBuilder()
                  .setInput(
                      ai.floedb.floecat.reconciler.rpc.LeasedPlanSnapshotInput.newBuilder()
                          .setJobId(payload.jobId())
                          .setLeaseEpoch(payload.leaseEpoch())
                          .setParentJobId(payload.parentJobId())
                          .setConnectorId(payload.connectorId())
                          .setMode(toProtoCaptureMode(payload.captureMode()))
                          .setFullRescan(payload.fullRescan())
                          .setScope(toProtoScope(payload.scope(), payload.connectorId()))
                          .setSnapshotTask(toProtoSnapshotTask(payload.snapshotTask()))
                          .build())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<SubmitLeasedPlanSnapshotResultResponse> submitLeasedPlanSnapshotResult(
      SubmitLeasedPlanSnapshotResultRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.hasSuccess()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanSnapshotSuccess(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getSuccess().getFileGroupJobsList().stream()
                            .map(
                                job ->
                                    new LeasedPlannerWorkerService.PlannedFileGroupJob(
                                        fromProtoScope(job.getScope()),
                                        fromProtoFileGroupTask(job.getFileGroupTask())))
                            .toList());
                return SubmitLeasedPlanSnapshotResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              if (request.hasFailure()) {
                boolean accepted =
                    leasedPlannerWorkerService.persistPlanSnapshotFailure(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        fromProtoFailureKind(request.getFailure().getFailureKind()),
                        fromProtoRetryDisposition(request.getFailure().getRetryDisposition()),
                        fromProtoRetryClass(request.getFailure().getRetryClass()),
                        request.getFailure().getMessage());
                return SubmitLeasedPlanSnapshotResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "outcome"));
            }),
        correlationId());
  }

  @Override
  public Uni<GetLeasedFileGroupExecutionResponse> getLeasedFileGroupExecution(
      GetLeasedFileGroupExecutionRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              var payload =
                  leasedFileGroupExecutionService.resolve(principalContext, jobId, leaseEpoch);
              var executionBuilder =
                  ai.floedb.floecat.reconciler.rpc.LeasedFileGroupExecution.newBuilder()
                      .setJobId(payload.jobId())
                      .setLeaseEpoch(payload.leaseEpoch())
                      .setParentJobId(payload.parentJobId())
                      .setMetadataLocation(payload.metadataLocation())
                      .setSourceNamespace(payload.sourceNamespace())
                      .setSourceTable(payload.sourceTable())
                      .setSnapshotId(payload.snapshotId())
                      .setPlanId(payload.planId())
                      .setGroupId(payload.groupId())
                      .addAllFilePaths(payload.plannedFilePaths())
                      .setCapturePolicy(
                          ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                              .addAllColumns(
                                  payload.capturePolicy().columns().stream()
                                      .map(
                                          column ->
                                              ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy
                                                  .newBuilder()
                                                  .setSelector(column.selector())
                                                  .setCaptureStats(column.captureStats())
                                                  .setCaptureIndex(column.captureIndex())
                                                  .build())
                                      .toList())
                              .addAllOutputs(
                                  payload.capturePolicy().outputs().stream()
                                      .map(ReconcileExecutorControlImpl::toProtoCaptureOutput)
                                      .toList())
                              .build());
              if (payload.sourceConnector() != null) {
                executionBuilder.setSourceConnector(payload.sourceConnector());
              }
              if (payload.tableId() != null) {
                executionBuilder.setTableId(payload.tableId());
              }
              return GetLeasedFileGroupExecutionResponse.newBuilder()
                  .setExecution(executionBuilder.build())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<SubmitLeasedFileGroupExecutionResultResponse> submitLeasedFileGroupExecutionResult(
      SubmitLeasedFileGroupExecutionResultRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String jobId = mustNonEmpty(request.getJobId(), "job_id", corr);
              String leaseEpoch = mustNonEmpty(request.getLeaseEpoch(), "lease_epoch", corr);
              if (request.hasSuccess()) {
                boolean accepted =
                    leasedFileGroupExecutionService.persistSuccess(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getSuccess().getResultId(),
                        request.getSuccess().getStatsRecordsList(),
                        request.getSuccess().getIndexArtifactsList().stream()
                            .map(
                                artifact ->
                                    new ai.floedb.floecat.reconciler.spi.ReconcilerBackend
                                        .StagedIndexArtifact(
                                        artifact.getRecord(),
                                        artifact.getContent().toByteArray(),
                                        artifact.getContentType()))
                            .toList());
                return SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              if (request.hasFailure()) {
                boolean accepted =
                    leasedFileGroupExecutionService.persistFailure(
                        principalContext,
                        jobId,
                        leaseEpoch,
                        request.getFailure().getResultId(),
                        request.getFailure().getMessage());
                return SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                    .setAccepted(accepted)
                    .build();
              }
              throw GrpcErrors.invalidArgument(corr, null, java.util.Map.of("field", "outcome"));
            }),
        correlationId());
  }

  private static Set<ReconcileExecutionClass> executionClassesFrom(
      LeaseReconcileJobRequest request) {
    EnumSet<ReconcileExecutionClass> executionClasses =
        EnumSet.noneOf(ReconcileExecutionClass.class);
    if (request == null) {
      return executionClasses;
    }
    for (var executionClass : request.getExecutionClassesList()) {
      switch (executionClass) {
        case EC_INTERACTIVE -> executionClasses.add(ReconcileExecutionClass.INTERACTIVE);
        case EC_BATCH -> executionClasses.add(ReconcileExecutionClass.BATCH);
        case EC_HEAVY -> executionClasses.add(ReconcileExecutionClass.HEAVY);
        case EC_DEFAULT, EC_UNSPECIFIED, UNRECOGNIZED ->
            executionClasses.add(ReconcileExecutionClass.DEFAULT);
      }
    }
    return executionClasses;
  }

  private static Set<String> lanesFrom(LeaseReconcileJobRequest request) {
    if (request == null) {
      return Set.of();
    }
    return request.getLanesList().stream()
        .map(lane -> lane == null ? "" : lane.trim())
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static Set<ReconcileJobKind> jobKindsFrom(LeaseReconcileJobRequest request) {
    EnumSet<ReconcileJobKind> jobKinds = EnumSet.noneOf(ReconcileJobKind.class);
    if (request == null) {
      return jobKinds;
    }
    for (var jobKind : request.getJobKindsList()) {
      switch (jobKind) {
        case RJK_PLAN_CONNECTOR -> jobKinds.add(ReconcileJobKind.PLAN_CONNECTOR);
        case RJK_PLAN_TABLE -> jobKinds.add(ReconcileJobKind.PLAN_TABLE);
        case RJK_PLAN_VIEW -> jobKinds.add(ReconcileJobKind.PLAN_VIEW);
        case RJK_PLAN_SNAPSHOT -> jobKinds.add(ReconcileJobKind.PLAN_SNAPSHOT);
        case RJK_FINALIZE_SNAPSHOT_CAPTURE ->
            jobKinds.add(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE);
        case RJK_EXEC_FILE_GROUP -> jobKinds.add(ReconcileJobKind.EXEC_FILE_GROUP);
        case RJK_UNSPECIFIED, UNRECOGNIZED -> {}
      }
    }
    return jobKinds;
  }

  private static LeasedReconcileJob toProtoLease(ReconcileJobStore.LeasedJob lease) {
    return LeasedReconcileJob.newBuilder()
        .setJobId(lease.jobId)
        .setConnectorId(
            ResourceId.newBuilder()
                .setAccountId(lease.accountId)
                .setKind(ResourceKind.RK_CONNECTOR)
                .setId(lease.connectorId)
                .build())
        .setMode(toProtoCaptureMode(lease.captureMode))
        .setFullRescan(lease.fullRescan)
        .setScope(toProtoScope(lease))
        .setLeaseEpoch(lease.leaseEpoch)
        .setExecutionPolicy(toProtoExecutionPolicy(lease.executionPolicy))
        .setPinnedExecutorId(lease.pinnedExecutorId)
        .setExecutorId(lease.executorId)
        .setKind(toProtoJobKind(lease.jobKind))
        .setParentJobId(lease.parentJobId)
        .setTableTask(toProtoTableTask(lease.tableTask))
        .setViewTask(toProtoViewTask(lease.viewTask))
        .setSnapshotTask(toProtoSnapshotTask(lease.snapshotTask))
        .setFileGroupTask(toProtoFileGroupTask(lease.fileGroupTask))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileJobKind toProtoJobKind(
      ReconcileJobKind jobKind) {
    return switch (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind) {
      case PLAN_CONNECTOR -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_CONNECTOR;
      case PLAN_TABLE -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_TABLE;
      case PLAN_VIEW -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_VIEW;
      case PLAN_SNAPSHOT -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_SNAPSHOT;
      case FINALIZE_SNAPSHOT_CAPTURE ->
          ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_FINALIZE_SNAPSHOT_CAPTURE;
      case EXEC_FILE_GROUP -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_FILE_GROUP;
    };
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileTableTask toProtoTableTask(
      ReconcileTableTask tableTask) {
    ReconcileTableTask effective = tableTask == null ? ReconcileTableTask.empty() : tableTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileTableTask.newBuilder()
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceTable(effective.sourceTable())
        .setDestinationTableId(blankToEmpty(effective.destinationTableId()))
        .setDestinationTableDisplayName(effective.destinationTableDisplayName())
        .setDestinationNamespaceId(effective.destinationNamespaceId())
        .setMode(effective.mode().name())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileViewTask toProtoViewTask(
      ReconcileViewTask viewTask) {
    ReconcileViewTask effective = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileViewTask.newBuilder()
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceView(effective.sourceView())
        .setDestinationNamespaceId(effective.destinationNamespaceId())
        .setDestinationViewId(blankToEmpty(effective.destinationViewId()))
        .setDestinationViewDisplayName(effective.destinationViewDisplayName())
        .setMode(effective.mode().name())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask toProtoSnapshotTask(
      ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.newBuilder()
        .setTableId(effective.tableId())
        .setSnapshotId(effective.snapshotId())
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceTable(effective.sourceTable())
        .setFileGroupPlanRecorded(effective.fileGroupPlanRecorded())
        .addAllFileGroups(
            effective.fileGroups().stream()
                .map(ReconcileExecutorControlImpl::toProtoFileGroupTask)
                .toList())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileFileGroupTask toProtoFileGroupTask(
      ReconcileFileGroupTask fileGroupTask) {
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileFileGroupTask.newBuilder()
        .setPlanId(effective.planId())
        .setGroupId(effective.groupId())
        .setTableId(effective.tableId())
        .setSnapshotId(effective.snapshotId())
        .addAllFilePaths(effective.filePaths())
        .addAllFileResults(
            effective.fileResults().stream()
                .map(ReconcileExecutorControlImpl::toProtoFileResult)
                .toList())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileFileResult toProtoFileResult(
      ReconcileFileResult fileResult) {
    ReconcileFileResult effective = fileResult == null ? ReconcileFileResult.empty() : fileResult;
    return ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.newBuilder()
        .setFilePath(effective.filePath())
        .setState(
            switch (effective.state()) {
              case SUCCEEDED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SUCCEEDED;
              case FAILED -> ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_FAILED;
              case SKIPPED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SKIPPED;
              case UNSPECIFIED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_UNSPECIFIED;
            })
        .setStatsProcessed(effective.statsProcessed())
        .setMessage(effective.message())
        .setIndexArtifact(toProtoIndexArtifact(effective.indexArtifact()))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.ReconcileIndexArtifactResult
      toProtoIndexArtifact(ReconcileIndexArtifactResult indexArtifact) {
    ReconcileIndexArtifactResult effective =
        indexArtifact == null ? ReconcileIndexArtifactResult.empty() : indexArtifact;
    return ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.ReconcileIndexArtifactResult
        .newBuilder()
        .setArtifactUri(effective.artifactUri())
        .setArtifactFormat(effective.artifactFormat())
        .setArtifactFormatVersion(effective.artifactFormatVersion())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.CaptureMode toProtoCaptureMode(
      CaptureMode captureMode) {
    return switch (java.util.Objects.requireNonNull(captureMode, "captureMode")) {
      case METADATA_ONLY -> ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY;
      case CAPTURE_ONLY -> ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_CAPTURE_ONLY;
      case METADATA_AND_CAPTURE ->
          ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_CAPTURE;
    };
  }

  private static ai.floedb.floecat.reconciler.rpc.ExecutionPolicy toProtoExecutionPolicy(
      ReconcileExecutionPolicy policy) {
    ReconcileExecutionPolicy effective =
        policy == null ? ReconcileExecutionPolicy.defaults() : policy;
    return ai.floedb.floecat.reconciler.rpc.ExecutionPolicy.newBuilder()
        .setExecutionClass(
            switch (effective.executionClass()) {
              case INTERACTIVE -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_INTERACTIVE;
              case BATCH -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_BATCH;
              case HEAVY -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_HEAVY;
              case DEFAULT -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_DEFAULT;
            })
        .setLane(effective.lane())
        .putAllAttributes(effective.attributes())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.CaptureScope toProtoScope(
      ReconcileJobStore.LeasedJob lease) {
    ReconcileScope scope =
        lease == null || lease.scope == null ? ReconcileScope.empty() : lease.scope;
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId(lease.connectorId)
            .build();
    return toProtoScope(scope, connectorId);
  }

  private static ai.floedb.floecat.reconciler.rpc.CaptureScope toProtoScope(
      ReconcileScope scope, ResourceId connectorId) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    var builder =
        ai.floedb.floecat.reconciler.rpc.CaptureScope.newBuilder()
            .setConnectorId(connectorId == null ? ResourceId.getDefaultInstance() : connectorId)
            .setDestinationTableId(
                effectiveScope.destinationTableId() == null
                    ? ""
                    : effectiveScope.destinationTableId())
            .setDestinationViewId(
                effectiveScope.destinationViewId() == null
                    ? ""
                    : effectiveScope.destinationViewId());
    for (String namespaceId : effectiveScope.destinationNamespaceIds()) {
      builder.addDestinationNamespaceIds(namespaceId);
    }
    for (ReconcileScope.ScopedCaptureRequest request :
        effectiveScope.destinationCaptureRequests()) {
      builder.addDestinationCaptureRequests(
          ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest.newBuilder()
              .setTableId(request.tableId())
              .setSnapshotId(request.snapshotId())
              .setTargetSpec(request.targetSpec())
              .addAllColumnSelectors(request.columnSelectors())
              .build());
    }
    if (effectiveScope.hasCapturePolicy()) {
      builder.setCapturePolicy(
          ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
              .addAllColumns(
                  effectiveScope.capturePolicy().columns().stream()
                      .map(
                          column ->
                              ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy.newBuilder()
                                  .setSelector(column.selector())
                                  .setCaptureStats(column.captureStats())
                                  .setCaptureIndex(column.captureIndex())
                                  .build())
                      .toList())
              .addAllOutputs(
                  effectiveScope.capturePolicy().outputs().stream()
                      .map(ReconcileExecutorControlImpl::toProtoCaptureOutput)
                      .toList())
              .build());
    }
    return builder.build();
  }

  private static ai.floedb.floecat.reconciler.rpc.CaptureOutput toProtoCaptureOutput(
      ReconcileCapturePolicy.Output output) {
    return switch (output) {
      case TABLE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS;
      case FILE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_FILE_STATS;
      case COLUMN_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_COLUMN_STATS;
      case PARQUET_PAGE_INDEX ->
          ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_PARQUET_PAGE_INDEX;
    };
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static ReconcileScope fromProtoScope(
      ai.floedb.floecat.reconciler.rpc.CaptureScope scope) {
    if (scope == null) {
      return ReconcileScope.empty();
    }
    return ReconcileScope.of(
        scope.getDestinationNamespaceIdsList(),
        blankToNull(scope.getDestinationTableId()),
        blankToNull(scope.getDestinationViewId()),
        scope.getDestinationCaptureRequestsList().stream()
            .map(
                request ->
                    new ReconcileScope.ScopedCaptureRequest(
                        request.getTableId(),
                        request.getSnapshotId(),
                        request.getTargetSpec(),
                        request.getColumnSelectorsList()))
            .toList(),
        scope.hasCapturePolicy()
            ? ReconcileCapturePolicy.of(
                scope.getCapturePolicy().getColumnsList().stream()
                    .map(
                        column ->
                            new ReconcileCapturePolicy.Column(
                                column.getSelector(),
                                column.getCaptureStats(),
                                column.getCaptureIndex()))
                    .toList(),
                scope.getCapturePolicy().getOutputsList().stream()
                    .map(ReconcileExecutorControlImpl::fromProtoCaptureOutput)
                    .collect(java.util.stream.Collectors.toSet()))
            : ReconcileCapturePolicy.empty());
  }

  private static ReconcileTableTask fromProtoTableTask(
      ai.floedb.floecat.reconciler.rpc.ReconcileTableTask tableTask) {
    if (tableTask == null) {
      return ReconcileTableTask.empty();
    }
    if (ReconcileTableTask.Mode.DISCOVERY.name().equals(tableTask.getMode())) {
      return ReconcileTableTask.discovery(
          tableTask.getSourceNamespace(),
          tableTask.getSourceTable(),
          tableTask.getDestinationNamespaceId(),
          blankToNull(tableTask.getDestinationTableId()),
          tableTask.getDestinationTableDisplayName());
    }
    return ReconcileTableTask.of(
        tableTask.getSourceNamespace(),
        tableTask.getSourceTable(),
        tableTask.getDestinationNamespaceId(),
        tableTask.getDestinationTableId(),
        tableTask.getDestinationTableDisplayName());
  }

  private static ReconcileViewTask fromProtoViewTask(
      ai.floedb.floecat.reconciler.rpc.ReconcileViewTask viewTask) {
    if (viewTask == null) {
      return ReconcileViewTask.empty();
    }
    if (ReconcileViewTask.Mode.DISCOVERY.name().equals(viewTask.getMode())) {
      return ReconcileViewTask.discovery(
          viewTask.getSourceNamespace(),
          viewTask.getSourceView(),
          viewTask.getDestinationNamespaceId(),
          blankToNull(viewTask.getDestinationViewId()),
          viewTask.getDestinationViewDisplayName());
    }
    return ReconcileViewTask.of(
        viewTask.getSourceNamespace(),
        viewTask.getSourceView(),
        viewTask.getDestinationNamespaceId(),
        viewTask.getDestinationViewId(),
        viewTask.getDestinationViewDisplayName());
  }

  private static ReconcileSnapshotTask fromProtoSnapshotTask(
      ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask snapshotTask) {
    if (snapshotTask == null) {
      return ReconcileSnapshotTask.empty();
    }
    return ReconcileSnapshotTask.of(
        snapshotTask.getTableId(),
        snapshotTask.getSnapshotId(),
        snapshotTask.getSourceNamespace(),
        snapshotTask.getSourceTable(),
        snapshotTask.getFileGroupsList().stream()
            .map(ReconcileExecutorControlImpl::fromProtoFileGroupTask)
            .toList(),
        snapshotTask.getFileGroupPlanRecorded());
  }

  private static ReconcileFileGroupTask fromProtoFileGroupTask(
      ai.floedb.floecat.reconciler.rpc.ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return ReconcileFileGroupTask.empty();
    }
    return ReconcileFileGroupTask.of(
        fileGroupTask.getPlanId(),
        fileGroupTask.getGroupId(),
        fileGroupTask.getTableId(),
        fileGroupTask.getSnapshotId(),
        fileGroupTask.getFilePathsList(),
        fileGroupTask.getFileResultsList().stream()
            .map(ReconcileExecutorControlImpl::fromProtoFileResult)
            .toList());
  }

  private static ReconcileFileResult fromProtoFileResult(
      ai.floedb.floecat.reconciler.rpc.ReconcileFileResult fileResult) {
    if (fileResult == null) {
      return ReconcileFileResult.empty();
    }
    return ReconcileFileResult.of(
        fileResult.getFilePath(),
        switch (fileResult.getState()) {
          case RFRS_SUCCEEDED -> ReconcileFileResult.State.SUCCEEDED;
          case RFRS_FAILED -> ReconcileFileResult.State.FAILED;
          case RFRS_SKIPPED -> ReconcileFileResult.State.SKIPPED;
          case RFRS_UNSPECIFIED, UNRECOGNIZED -> ReconcileFileResult.State.UNSPECIFIED;
        },
        fileResult.getStatsProcessed(),
        fileResult.getMessage(),
        fromProtoIndexArtifact(fileResult.getIndexArtifact()));
  }

  private static ReconcileIndexArtifactResult fromProtoIndexArtifact(
      ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.ReconcileIndexArtifactResult
          indexArtifact) {
    if (indexArtifact == null) {
      return ReconcileIndexArtifactResult.empty();
    }
    return ReconcileIndexArtifactResult.of(
        indexArtifact.getArtifactUri(),
        indexArtifact.getArtifactFormat(),
        indexArtifact.getArtifactFormatVersion());
  }

  private static ReconcileCapturePolicy.Output fromProtoCaptureOutput(
      ai.floedb.floecat.reconciler.rpc.CaptureOutput output) {
    return switch (output) {
      case CO_TABLE_STATS -> ReconcileCapturePolicy.Output.TABLE_STATS;
      case CO_FILE_STATS -> ReconcileCapturePolicy.Output.FILE_STATS;
      case CO_COLUMN_STATS -> ReconcileCapturePolicy.Output.COLUMN_STATS;
      case CO_PARQUET_PAGE_INDEX -> ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX;
      case CO_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("capture output is required");
    };
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
