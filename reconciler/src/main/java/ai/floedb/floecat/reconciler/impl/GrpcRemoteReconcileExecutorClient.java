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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
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
import ai.floedb.floecat.reconciler.rpc.GetLeasedFileGroupExecutionRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanSnapshotInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanViewInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureKind;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryClass;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryDisposition;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanConnectorResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultRequest;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
class GrpcRemoteReconcileExecutorClient
    implements RemoteReconcileExecutorClient,
        RemotePlannerWorkerClient,
        RemoteFileGroupWorkerClient {
  private static final Logger LOG = Logger.getLogger(GrpcRemoteReconcileExecutorClient.class);

  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;

  GrpcRemoteReconcileExecutorClient(
      @ConfigProperty(name = "floecat.reconciler.authorization.header") Optional<String> headerName,
      @ConfigProperty(name = "floecat.reconciler.authorization.token")
          Optional<String> staticToken) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
    this.staticToken = staticToken.map(String::trim).filter(v -> !v.isBlank());
  }

  @GrpcClient("floecat")
  ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub executorControl;

  @Override
  public Optional<RemoteLeasedJob> lease(
      ReconcileJobStore.LeaseRequest request, String leaseClientId) {
    ReconcileJobStore.LeaseRequest effective =
        request == null ? ReconcileJobStore.LeaseRequest.all() : request;
    var response =
        withHeaders(
                executorControl,
                "reconcile-lease-"
                    + (leaseClientId == null || leaseClientId.isBlank()
                        ? "aggregate"
                        : leaseClientId))
            .leaseReconcileJob(
                LeaseReconcileJobRequest.newBuilder()
                    .setExecutorId(
                        leaseClientId == null || leaseClientId.isBlank()
                            ? ""
                            : leaseClientId.trim())
                    .addAllExecutionClasses(
                        effective.executionClasses.stream()
                            .map(GrpcRemoteReconcileExecutorClient::toProtoExecutionClass)
                            .toList())
                    .addAllLanes(effective.lanes)
                    .addAllJobKinds(
                        effective.jobKinds.stream()
                            .map(GrpcRemoteReconcileExecutorClient::toProtoJobKind)
                            .toList())
                    .addAllExecutorIds(effective.executorIds)
                    .build());
    if (!response.getFound()) {
      return Optional.empty();
    }
    return Optional.of(new RemoteLeasedJob(fromProtoLease(response.getJob())));
  }

  @Override
  public void start(RemoteLeasedJob lease, String executorId) {
    withHeaders(executorControl, correlationId(lease))
        .startLeasedReconcileJob(
            StartLeasedReconcileJobRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setExecutorId(executorId)
                .build());
  }

  @Override
  public LeaseHeartbeat renew(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .renewReconcileLease(
                RenewReconcileLeaseRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    return new LeaseHeartbeat(response.getRenewed(), response.getCancellationRequested());
  }

  @Override
  public LeaseHeartbeat reportProgress(
      RemoteLeasedJob lease,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .reportReconcileProgress(
                ReportReconcileProgressRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .setTablesScanned(tablesScanned)
                    .setTablesChanged(tablesChanged)
                    .setViewsScanned(viewsScanned)
                    .setViewsChanged(viewsChanged)
                    .setErrors(errors)
                    .setSnapshotsProcessed(snapshotsProcessed)
                    .setStatsProcessed(statsProcessed)
                    .setMessage(message == null ? "" : message)
                    .build());
    return new LeaseHeartbeat(response.getLeaseValid(), response.getCancellationRequested());
  }

  @Override
  public CompletionResult complete(
      RemoteLeasedJob lease,
      RemoteLeasedJob.CompletionState state,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .setState(toProtoCompletionState(state))
                    .setFailureRetryDisposition(toProtoRetryDisposition(retryDisposition))
                    .setFailureRetryClass(toProtoRetryClass(retryClass))
                    .setTablesScanned(tablesScanned)
                    .setTablesChanged(tablesChanged)
                    .setViewsScanned(viewsScanned)
                    .setViewsChanged(viewsChanged)
                    .setErrors(errors)
                    .setSnapshotsProcessed(snapshotsProcessed)
                    .setStatsProcessed(statsProcessed)
                    .setMessage(message == null ? "" : message)
                    .build());
    return new CompletionResult(response.getAccepted());
  }

  @Override
  public boolean cancellationRequested(RemoteLeasedJob lease) {
    return withHeaders(executorControl, correlationId(lease))
        .getReconcileCancellation(
            GetReconcileCancellationRequest.newBuilder().setJobId(lease.lease().jobId).build())
        .getCancellationRequested();
  }

  public StandalonePlanConnectorPayload getPlanConnectorInput(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .getLeasedPlanConnectorInput(
                GetLeasedPlanConnectorInputRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    var input = response.getInput();
    return new StandalonePlanConnectorPayload(
        input.getJobId(),
        input.getLeaseEpoch(),
        input.getConnectorId(),
        fromProtoCaptureMode(input.getMode()),
        input.getFullRescan(),
        fromProtoScope(input.getScope()),
        fromProtoExecutionPolicy(input.getExecutionPolicy()),
        input.getPinnedExecutorId());
  }

  public boolean submitPlanConnectorSuccess(
      RemoteLeasedJob lease, List<PlannedTableJob> tableJobs, List<PlannedViewJob> viewJobs) {
    SubmitLeasedPlanConnectorResultRequest.Success.Builder success =
        SubmitLeasedPlanConnectorResultRequest.Success.newBuilder();
    for (PlannedTableJob tableJob : tableJobs == null ? List.<PlannedTableJob>of() : tableJobs) {
      if (tableJob == null || tableJob.tableTask() == null) {
        continue;
      }
      success.addTableJobs(
          ai.floedb.floecat.reconciler.rpc.PlannedTablePlanJob.newBuilder()
              .setScope(toProtoScope(tableJob.scope(), lease.lease()))
              .setTableTask(toProtoTableTask(tableJob.tableTask()))
              .build());
    }
    for (PlannedViewJob viewJob : viewJobs == null ? List.<PlannedViewJob>of() : viewJobs) {
      if (viewJob == null || viewJob.viewTask() == null) {
        continue;
      }
      success.addViewJobs(
          ai.floedb.floecat.reconciler.rpc.PlannedViewPlanJob.newBuilder()
              .setScope(toProtoScope(viewJob.scope(), lease.lease()))
              .setViewTask(toProtoViewTask(viewJob.viewTask()))
              .build());
    }
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanConnectorResult(
            SubmitLeasedPlanConnectorResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setSuccess(success.build())
                .build())
        .getAccepted();
  }

  public boolean submitPlanConnectorFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanConnectorResult(
            SubmitLeasedPlanConnectorResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setFailure(
                    SubmitLeasedPlanConnectorResultRequest.Failure.newBuilder()
                        .setMessage(message == null ? "" : message)
                        .setFailureKind(toProtoFailureKind(failureKind))
                        .setRetryDisposition(toProtoRetryDisposition(retryDisposition))
                        .setRetryClass(toProtoRetryClass(retryClass))
                        .build())
                .build())
        .getAccepted();
  }

  public StandalonePlanTablePayload getPlanTableInput(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .getLeasedPlanTableInput(
                GetLeasedPlanTableInputRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    var input = response.getInput();
    return new StandalonePlanTablePayload(
        input.getJobId(),
        input.getLeaseEpoch(),
        input.getParentJobId(),
        input.getConnectorId(),
        fromProtoCaptureMode(input.getMode()),
        input.getFullRescan(),
        fromProtoScope(input.getScope()),
        fromProtoTableTask(input.getTableTask()));
  }

  public boolean submitPlanTableSuccess(
      RemoteLeasedJob lease, List<PlannedSnapshotJob> snapshotJobs) {
    SubmitLeasedPlanTableResultRequest.Success.Builder success =
        SubmitLeasedPlanTableResultRequest.Success.newBuilder();
    for (PlannedSnapshotJob snapshotJob :
        snapshotJobs == null ? List.<PlannedSnapshotJob>of() : snapshotJobs) {
      if (snapshotJob == null || snapshotJob.snapshotTask() == null) {
        continue;
      }
      success.addSnapshotJobs(
          ai.floedb.floecat.reconciler.rpc.PlannedSnapshotPlanJob.newBuilder()
              .setScope(toProtoScope(snapshotJob.scope(), lease.lease()))
              .setSnapshotTask(toProtoSnapshotTask(snapshotJob.snapshotTask()))
              .build());
    }
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanTableResult(
            SubmitLeasedPlanTableResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setSuccess(success.build())
                .build())
        .getAccepted();
  }

  public boolean submitPlanTableFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanTableResult(
            SubmitLeasedPlanTableResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setFailure(
                    SubmitLeasedPlanTableResultRequest.Failure.newBuilder()
                        .setMessage(message == null ? "" : message)
                        .setFailureKind(toProtoFailureKind(failureKind))
                        .setRetryDisposition(toProtoRetryDisposition(retryDisposition))
                        .setRetryClass(toProtoRetryClass(retryClass))
                        .build())
                .build())
        .getAccepted();
  }

  public StandalonePlanViewPayload getPlanViewInput(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .getLeasedPlanViewInput(
                GetLeasedPlanViewInputRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    var input = response.getInput();
    return new StandalonePlanViewPayload(
        input.getJobId(),
        input.getLeaseEpoch(),
        input.getParentJobId(),
        input.getConnectorId(),
        fromProtoScope(input.getScope()),
        fromProtoViewTask(input.getViewTask()));
  }

  public RemotePlannerWorkerClient.PlanViewSubmitResult submitPlanViewSuccess(
      RemoteLeasedJob lease, PlannedViewMutation mutation) {
    SubmitLeasedPlanViewResultRequest.Success.Builder success =
        SubmitLeasedPlanViewResultRequest.Success.newBuilder();
    if (mutation != null) {
      success.setMutation(
          ai.floedb.floecat.reconciler.rpc.PlannedViewMutation.newBuilder()
              .setDestinationViewId(
                  mutation.destinationViewId() == null
                      ? ResourceId.getDefaultInstance()
                      : mutation.destinationViewId())
              .setViewSpec(
                  mutation.viewSpec() == null
                      ? ai.floedb.floecat.catalog.rpc.ViewSpec.getDefaultInstance()
                      : mutation.viewSpec())
              .setIdempotencyKey(mutation.idempotencyKey() == null ? "" : mutation.idempotencyKey())
              .build());
    }
    var response =
        withHeaders(executorControl, correlationId(lease))
            .submitLeasedPlanViewResult(
                SubmitLeasedPlanViewResultRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .setSuccess(success.build())
                    .build());
    return new RemotePlannerWorkerClient.PlanViewSubmitResult(
        response.getAccepted(), response.getViewsChanged());
  }

  public boolean submitPlanViewFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanViewResult(
            SubmitLeasedPlanViewResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setFailure(
                    SubmitLeasedPlanViewResultRequest.Failure.newBuilder()
                        .setMessage(message == null ? "" : message)
                        .setFailureKind(toProtoFailureKind(failureKind))
                        .setRetryDisposition(toProtoRetryDisposition(retryDisposition))
                        .setRetryClass(toProtoRetryClass(retryClass))
                        .build())
                .build())
        .getAccepted();
  }

  public StandalonePlanSnapshotPayload getPlanSnapshotInput(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .getLeasedPlanSnapshotInput(
                GetLeasedPlanSnapshotInputRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    var input = response.getInput();
    return new StandalonePlanSnapshotPayload(
        input.getJobId(),
        input.getLeaseEpoch(),
        input.getParentJobId(),
        input.getConnectorId(),
        fromProtoCaptureMode(input.getMode()),
        input.getFullRescan(),
        fromProtoScope(input.getScope()),
        fromProtoSnapshotTask(input.getSnapshotTask()));
  }

  public boolean submitPlanSnapshotSuccess(
      RemoteLeasedJob lease, List<PlannedFileGroupJob> fileGroupJobs) {
    SubmitLeasedPlanSnapshotResultRequest.Success.Builder success =
        SubmitLeasedPlanSnapshotResultRequest.Success.newBuilder();
    for (PlannedFileGroupJob fileGroupJob :
        fileGroupJobs == null ? List.<PlannedFileGroupJob>of() : fileGroupJobs) {
      if (fileGroupJob == null || fileGroupJob.fileGroupTask() == null) {
        continue;
      }
      success.addFileGroupJobs(
          ai.floedb.floecat.reconciler.rpc.PlannedFileGroupExecutionJob.newBuilder()
              .setScope(toProtoScope(fileGroupJob.scope(), lease.lease()))
              .setFileGroupTask(toProtoFileGroupTask(fileGroupJob.fileGroupTask()))
              .build());
    }
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanSnapshotResult(
            SubmitLeasedPlanSnapshotResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setSuccess(success.build())
                .build())
        .getAccepted();
  }

  public boolean submitPlanSnapshotFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedPlanSnapshotResult(
            SubmitLeasedPlanSnapshotResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setFailure(
                    SubmitLeasedPlanSnapshotResultRequest.Failure.newBuilder()
                        .setMessage(message == null ? "" : message)
                        .setFailureKind(toProtoFailureKind(failureKind))
                        .setRetryDisposition(toProtoRetryDisposition(retryDisposition))
                        .setRetryClass(toProtoRetryClass(retryClass))
                        .build())
                .build())
        .getAccepted();
  }

  public StandaloneFileGroupExecutionPayload getExecution(RemoteLeasedJob lease) {
    var response =
        withHeaders(executorControl, correlationId(lease))
            .getLeasedFileGroupExecution(
                GetLeasedFileGroupExecutionRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .build());
    var execution = response.getExecution();
    return new StandaloneFileGroupExecutionPayload(
        execution.getJobId(),
        execution.getLeaseEpoch(),
        execution.getParentJobId(),
        execution.hasSourceConnector()
            ? execution.getSourceConnector()
            : Connector.getDefaultInstance(),
        execution.getSourceNamespace(),
        execution.getSourceTable(),
        execution.hasTableId() ? execution.getTableId() : null,
        execution.getSnapshotId(),
        execution.getPlanId(),
        execution.getGroupId(),
        execution.getFilePathsList(),
        execution.hasCapturePolicy()
            ? ReconcileCapturePolicy.of(
                execution.getCapturePolicy().getColumnsList().stream()
                    .map(
                        column ->
                            new ReconcileCapturePolicy.Column(
                                column.getSelector(),
                                column.getCaptureStats(),
                                column.getCaptureIndex()))
                    .toList(),
                execution.getCapturePolicy().getOutputsList().stream()
                    .map(GrpcRemoteReconcileExecutorClient::fromProtoCaptureOutput)
                    .collect(java.util.stream.Collectors.toSet()))
            : ReconcileCapturePolicy.empty());
  }

  public boolean submitSuccess(RemoteLeasedJob lease, StandaloneFileGroupExecutionResult result) {
    SubmitLeasedFileGroupExecutionResultRequest.Success.Builder success =
        SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
            .setResultId(result.resultId() == null ? "" : result.resultId())
            .addAllStatsRecords(result.statsRecords());
    for (var artifact : result.stagedIndexArtifacts()) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      byte[] content = artifact.content();
      success.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact.newBuilder()
              .setRecord(artifact.record())
              .setContent(
                  content == null
                      ? com.google.protobuf.ByteString.EMPTY
                      : com.google.protobuf.ByteString.copyFrom(content))
              .setContentType(artifact.contentType() == null ? "" : artifact.contentType())
              .build());
    }
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedFileGroupExecutionResult(
            SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setSuccess(success.build())
                .build())
        .getAccepted();
  }

  public boolean submitFailure(RemoteLeasedJob lease, String resultId, String message) {
    return withHeaders(executorControl, correlationId(lease))
        .submitLeasedFileGroupExecutionResult(
            SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setFailure(
                    SubmitLeasedFileGroupExecutionResultRequest.Failure.newBuilder()
                        .setResultId(resultId == null ? "" : resultId)
                        .setMessage(message == null ? "" : message)
                        .build())
                .build())
        .getAccepted();
  }

  private static ai.floedb.floecat.reconciler.rpc.ExecutionClass toProtoExecutionClass(
      ReconcileExecutionClass executionClass) {
    return switch (executionClass) {
      case INTERACTIVE -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_INTERACTIVE;
      case BATCH -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_BATCH;
      case HEAVY -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_HEAVY;
      case DEFAULT -> ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_DEFAULT;
    };
  }

  private static ReconcileJobStore.LeasedJob fromProtoLease(
      ai.floedb.floecat.reconciler.rpc.LeasedReconcileJob job) {
    ResourceId connectorId = job.getConnectorId();
    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            job.getJobId(),
            connectorId.getAccountId(),
            connectorId.getId(),
            job.getFullRescan(),
            fromProtoCaptureMode(job.getMode()),
            fromProtoScope(job.getScope()),
            fromProtoExecutionPolicy(job.getExecutionPolicy()),
            job.getLeaseEpoch(),
            job.getPinnedExecutorId(),
            job.getExecutorId(),
            fromProtoJobKind(job.getKind()),
            fromProtoTableTask(job.getTableTask()),
            fromProtoViewTask(job.getViewTask()),
            fromProtoSnapshotTask(job.getSnapshotTask()),
            fromProtoFileGroupTask(job.getFileGroupTask()),
            job.getParentJobId());
    return lease;
  }

  private static CaptureMode fromProtoCaptureMode(
      ai.floedb.floecat.reconciler.rpc.CaptureMode captureMode) {
    return switch (captureMode) {
      case CM_METADATA_ONLY -> CaptureMode.METADATA_ONLY;
      case CM_CAPTURE_ONLY -> CaptureMode.CAPTURE_ONLY;
      case CM_METADATA_AND_CAPTURE, CM_UNSPECIFIED, UNRECOGNIZED ->
          CaptureMode.METADATA_AND_CAPTURE;
    };
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
                    .map(GrpcRemoteReconcileExecutorClient::fromProtoCaptureOutput)
                    .collect(java.util.stream.Collectors.toSet()))
            : ReconcileCapturePolicy.empty());
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

  private static ReconcileExecutionPolicy fromProtoExecutionPolicy(
      ai.floedb.floecat.reconciler.rpc.ExecutionPolicy policy) {
    if (policy == null) {
      return ReconcileExecutionPolicy.defaults();
    }
    return ReconcileExecutionPolicy.of(
        switch (policy.getExecutionClass()) {
          case EC_INTERACTIVE -> ReconcileExecutionClass.INTERACTIVE;
          case EC_BATCH -> ReconcileExecutionClass.BATCH;
          case EC_HEAVY -> ReconcileExecutionClass.HEAVY;
          case EC_DEFAULT, EC_UNSPECIFIED, UNRECOGNIZED -> ReconcileExecutionClass.DEFAULT;
        },
        policy.getLane(),
        policy.getAttributesMap());
  }

  private static ai.floedb.floecat.reconciler.rpc.CaptureScope toProtoScope(
      ReconcileScope scope, ReconcileJobStore.LeasedJob lease) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    var builder =
        ai.floedb.floecat.reconciler.rpc.CaptureScope.newBuilder()
            .setConnectorId(
                ResourceId.newBuilder()
                    .setAccountId(lease == null ? "" : lease.accountId)
                    .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_CONNECTOR)
                    .setId(lease == null ? "" : lease.connectorId)
                    .build())
            .addAllDestinationNamespaceIds(effectiveScope.destinationNamespaceIds())
            .setDestinationTableId(
                effectiveScope.destinationTableId() == null
                    ? ""
                    : effectiveScope.destinationTableId())
            .setDestinationViewId(
                effectiveScope.destinationViewId() == null
                    ? ""
                    : effectiveScope.destinationViewId())
            .addAllDestinationCaptureRequests(
                effectiveScope.destinationCaptureRequests().stream()
                    .map(
                        request ->
                            ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest.newBuilder()
                                .setTableId(request.tableId())
                                .setSnapshotId(request.snapshotId())
                                .setTargetSpec(request.targetSpec())
                                .addAllColumnSelectors(request.columnSelectors())
                                .build())
                    .toList());
    if (effectiveScope.hasCapturePolicy()) {
      builder.setCapturePolicy(toProtoCapturePolicy(effectiveScope.capturePolicy()));
    }
    return builder.build();
  }

  private static ai.floedb.floecat.reconciler.rpc.CapturePolicy toProtoCapturePolicy(
      ReconcileCapturePolicy capturePolicy) {
    ReconcileCapturePolicy effective =
        capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    return ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
        .addAllColumns(
            effective.columns().stream()
                .map(
                    column ->
                        ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy.newBuilder()
                            .setSelector(column.selector())
                            .setCaptureStats(column.captureStats())
                            .setCaptureIndex(column.captureIndex())
                            .build())
                .toList())
        .addAllOutputs(
            effective.outputs().stream()
                .map(GrpcRemoteReconcileExecutorClient::toProtoCaptureOutput)
                .toList())
        .build();
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

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileTableTask toProtoTableTask(
      ReconcileTableTask tableTask) {
    ReconcileTableTask effective = tableTask == null ? ReconcileTableTask.empty() : tableTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileTableTask.newBuilder()
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceTable(effective.sourceTable())
        .setDestinationNamespaceId(effective.destinationNamespaceId())
        .setDestinationTableId(
            effective.destinationTableId() == null ? "" : effective.destinationTableId())
        .setDestinationTableDisplayName(effective.destinationTableDisplayName())
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
        .setDestinationViewId(
            effective.destinationViewId() == null ? "" : effective.destinationViewId())
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
                .map(GrpcRemoteReconcileExecutorClient::toProtoFileGroupTask)
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
                .map(GrpcRemoteReconcileExecutorClient::toProtoFileResult)
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

  private static ReconcileCompletionState toProtoCompletionState(
      RemoteLeasedJob.CompletionState state) {
    return switch (state) {
      case SUCCEEDED -> ReconcileCompletionState.RCS_SUCCEEDED;
      case FAILED -> ReconcileCompletionState.RCS_FAILED;
      case CANCELLED -> ReconcileCompletionState.RCS_CANCELLED;
    };
  }

  private static ReconcileFailureRetryDisposition toProtoRetryDisposition(
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition) {
    if (retryDisposition == null) {
      return ReconcileFailureRetryDisposition.RFRD_UNSPECIFIED;
    }
    return switch (retryDisposition) {
      case RETRYABLE -> ReconcileFailureRetryDisposition.RFRD_RETRYABLE;
      case TERMINAL -> ReconcileFailureRetryDisposition.RFRD_TERMINAL;
    };
  }

  private static ReconcileFailureRetryClass toProtoRetryClass(
      ReconcileExecutor.ExecutionResult.RetryClass retryClass) {
    if (retryClass == null) {
      return ReconcileFailureRetryClass.RFRC_UNSPECIFIED;
    }
    return switch (retryClass) {
      case NONE -> ReconcileFailureRetryClass.RFRC_UNSPECIFIED;
      case TRANSIENT_ERROR -> ReconcileFailureRetryClass.RFRC_TRANSIENT_ERROR;
      case DEPENDENCY_NOT_READY -> ReconcileFailureRetryClass.RFRC_DEPENDENCY_NOT_READY;
      case STATE_UNCERTAIN -> ReconcileFailureRetryClass.RFRC_STATE_UNCERTAIN;
    };
  }

  private static ReconcileFailureKind toProtoFailureKind(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind) {
    if (failureKind == null) {
      return ReconcileFailureKind.RFK_UNSPECIFIED;
    }
    return switch (failureKind) {
      case NONE -> ReconcileFailureKind.RFK_UNSPECIFIED;
      case CONNECTOR_MISSING -> ReconcileFailureKind.RFK_CONNECTOR_MISSING;
      case TABLE_MISSING -> ReconcileFailureKind.RFK_TABLE_MISSING;
      case VIEW_MISSING -> ReconcileFailureKind.RFK_VIEW_MISSING;
      case INTERNAL -> ReconcileFailureKind.RFK_INTERNAL;
    };
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

  private static ReconcileJobKind fromProtoJobKind(
      ai.floedb.floecat.reconciler.rpc.ReconcileJobKind jobKind) {
    return switch (jobKind) {
      case RJK_PLAN_CONNECTOR -> ReconcileJobKind.PLAN_CONNECTOR;
      case RJK_PLAN_TABLE -> ReconcileJobKind.PLAN_TABLE;
      case RJK_PLAN_VIEW -> ReconcileJobKind.PLAN_VIEW;
      case RJK_PLAN_SNAPSHOT -> ReconcileJobKind.PLAN_SNAPSHOT;
      case RJK_FINALIZE_SNAPSHOT_CAPTURE -> ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE;
      case RJK_EXEC_FILE_GROUP -> ReconcileJobKind.EXEC_FILE_GROUP;
      case RJK_UNSPECIFIED, UNRECOGNIZED -> ReconcileJobKind.PLAN_CONNECTOR;
    };
  }

  private static ReconcileTableTask fromProtoTableTask(
      ai.floedb.floecat.reconciler.rpc.ReconcileTableTask tableTask) {
    if (tableTask == null) {
      return ReconcileTableTask.empty();
    }
    ReconcileTableTask task =
        ReconcileTableTask.of(
            tableTask.getSourceNamespace(),
            tableTask.getSourceTable(),
            tableTask.getDestinationNamespaceId(),
            tableTask.getDestinationTableId(),
            tableTask.getDestinationTableDisplayName());
    if (ReconcileTableTask.Mode.DISCOVERY.name().equals(tableTask.getMode())) {
      return ReconcileTableTask.discovery(
          tableTask.getSourceNamespace(),
          tableTask.getSourceTable(),
          tableTask.getDestinationNamespaceId(),
          blankToNull(tableTask.getDestinationTableId()),
          tableTask.getDestinationTableDisplayName());
    }
    return task;
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
            .map(GrpcRemoteReconcileExecutorClient::fromProtoFileGroupTask)
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
            .map(GrpcRemoteReconcileExecutorClient::fromProtoFileResult)
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

  private <T extends AbstractStub<T>> T withHeaders(T stub, String correlationId) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadata(correlationId)));
  }

  private Metadata metadata(String correlationId) {
    Metadata metadata = new Metadata();
    metadata.put(CORRELATION_ID, correlationId == null ? "" : correlationId);
    staticToken.ifPresent(value -> metadata.put(authHeaderKey(), withBearerPrefix(value)));
    return metadata;
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }

  private static String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
  }

  private static String correlationId(RemoteLeasedJob lease) {
    return "reconcile-job-" + lease.lease().jobId;
  }
}
