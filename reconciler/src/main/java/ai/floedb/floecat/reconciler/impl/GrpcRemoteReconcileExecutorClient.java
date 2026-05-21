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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
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
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

  private final Optional<String> workerAuthHeaderName;
  private final boolean workerAuthRequired;
  private final ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  private final Optional<String> workerControlHost;
  private final int workerControlPort;
  private final boolean workerControlPlainText;
  private final int workerControlMaxInboundMessageSize;
  private final Object workerControlLock = new Object();
  private volatile ManagedChannel workerControlChannel;
  private volatile ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub
      workerControlStub;

  @Inject
  GrpcRemoteReconcileExecutorClient(
      @ConfigProperty(name = "floecat.interceptor.session.header")
          Optional<String> sessionHeaderName,
      @ConfigProperty(name = "floecat.reconciler.authorization.header")
          Optional<String> authorizationHeaderName,
      @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
          boolean workerAuthRequired,
      @ConfigProperty(name = "floecat.reconciler.worker-control.grpc.host")
          Optional<String> workerControlHost,
      @ConfigProperty(name = "floecat.reconciler.worker-control.grpc.port", defaultValue = "9100")
          int workerControlPort,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.plain-text",
              defaultValue = "true")
          boolean workerControlPlainText,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.max-inbound-message-size",
              defaultValue = "0")
          int workerControlMaxInboundMessageSize,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        sessionHeaderName,
        authorizationHeaderName,
        workerAuthRequired,
        workerControlHost,
        workerControlPort,
        workerControlPlainText,
        workerControlMaxInboundMessageSize,
        reconcileWorkerAuthProvider,
        true);
  }

  GrpcRemoteReconcileExecutorClient(
      String workerAuthHeaderName, ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        Optional.ofNullable(workerAuthHeaderName),
        Optional.empty(),
        true,
        Optional.empty(),
        9100,
        true,
        0,
        reconcileWorkerAuthProvider,
        true);
  }

  GrpcRemoteReconcileExecutorClient(
      String workerAuthHeaderName,
      boolean workerAuthRequired,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        Optional.ofNullable(workerAuthHeaderName),
        Optional.empty(),
        workerAuthRequired,
        Optional.empty(),
        9100,
        true,
        0,
        reconcileWorkerAuthProvider,
        true);
  }

  private GrpcRemoteReconcileExecutorClient(
      Optional<String> sessionHeaderName,
      Optional<String> authorizationHeaderName,
      boolean workerAuthRequired,
      Optional<String> workerControlHost,
      int workerControlPort,
      boolean workerControlPlainText,
      int workerControlMaxInboundMessageSize,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      boolean ignored) {
    this.workerAuthHeaderName =
        ReconcileRpcAuthHeaderSupport.resolveHeaderName(sessionHeaderName, authorizationHeaderName);
    this.workerAuthRequired = workerAuthRequired;
    this.reconcileWorkerAuthProvider = reconcileWorkerAuthProvider;
    this.workerControlHost = workerControlHost.map(String::trim).filter(value -> !value.isBlank());
    this.workerControlPort = workerControlPort;
    this.workerControlPlainText = workerControlPlainText;
    this.workerControlMaxInboundMessageSize = Math.max(0, workerControlMaxInboundMessageSize);
  }

  @GrpcClient("floecat")
  ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub executorControl;

  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;

  @PreDestroy
  void destroy() {
    resetWorkerControlChannel();
  }

  @Override
  public Optional<RemoteLeasedJob> lease(
      ReconcileJobStore.LeaseRequest request, String leaseClientId) {
    ReconcileJobStore.LeaseRequest effective =
        request == null ? ReconcileJobStore.LeaseRequest.all() : request;
    var response =
        invokeWorkerControlRetryable(
            "leaseReconcileJob",
            "reconcile-lease-"
                + (leaseClientId == null || leaseClientId.isBlank() ? "aggregate" : leaseClientId),
            stub ->
                stub.leaseReconcileJob(
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
                        .build()));
    if (!response.getFound()) {
      return Optional.empty();
    }
    return Optional.of(new RemoteLeasedJob(fromProtoLease(response.getJob())));
  }

  @Override
  public void start(RemoteLeasedJob lease, String executorId) {
    invokeWorkerControlOnce(
        "startLeasedReconcileJob",
        correlationId(lease),
        stub ->
            stub.startLeasedReconcileJob(
                StartLeasedReconcileJobRequest.newBuilder()
                    .setJobId(lease.lease().jobId)
                    .setLeaseEpoch(lease.lease().leaseEpoch)
                    .setExecutorId(executorId)
                    .build()));
  }

  @Override
  public LeaseHeartbeat renew(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "renewReconcileLease",
            correlationId(lease),
            stub ->
                stub.renewReconcileLease(
                    RenewReconcileLeaseRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
        invokeWorkerControlRetryable(
            "reportReconcileProgress",
            correlationId(lease),
            stub ->
                stub.reportReconcileProgress(
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
                        .build()));
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
        invokeWorkerControlOnce(
            "completeLeasedReconcileJob",
            correlationId(lease),
            stub ->
                stub.completeLeasedReconcileJob(
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
                        .build()));
    return new CompletionResult(response.getAccepted());
  }

  @Override
  public boolean cancellationRequested(RemoteLeasedJob lease) {
    return invokeWorkerControlRetryable(
        "getReconcileCancellation",
        correlationId(lease),
        stub ->
            stub.getReconcileCancellation(
                    GetReconcileCancellationRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .build())
                .getCancellationRequested());
  }

  public StandalonePlanConnectorPayload getPlanConnectorInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanConnectorInput",
            correlationId(lease),
            stub ->
                stub.getLeasedPlanConnectorInput(
                    GetLeasedPlanConnectorInputRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
    return invokeWorkerControlOnce(
        "submitLeasedPlanConnectorResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanConnectorResult(
                    SubmitLeasedPlanConnectorResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success.build())
                        .build())
                .getAccepted());
  }

  public boolean submitPlanConnectorFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return invokeWorkerControlOnce(
        "submitLeasedPlanConnectorResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanConnectorResult(
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
                .getAccepted());
  }

  public StandalonePlanTablePayload getPlanTableInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanTableInput",
            correlationId(lease),
            stub ->
                stub.getLeasedPlanTableInput(
                    GetLeasedPlanTableInputRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
      RemoteLeasedJob lease,
      List<PlannedSnapshotJob> snapshotJobs,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    SubmitLeasedPlanTableResultRequest.Success.Builder success =
        SubmitLeasedPlanTableResultRequest.Success.newBuilder()
            .setTablesScanned(tablesScanned)
            .setTablesChanged(tablesChanged)
            .setErrors(errors)
            .setSnapshotsProcessed(snapshotsProcessed)
            .setStatsProcessed(statsProcessed);
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
    return invokeWorkerControlOnce(
        "submitLeasedPlanTableResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanTableResult(
                    SubmitLeasedPlanTableResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success.build())
                        .build())
                .getAccepted());
  }

  public boolean submitPlanTableFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return invokeWorkerControlOnce(
        "submitLeasedPlanTableResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanTableResult(
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
                .getAccepted());
  }

  public StandalonePlanViewPayload getPlanViewInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanViewInput",
            correlationId(lease),
            stub ->
                stub.getLeasedPlanViewInput(
                    GetLeasedPlanViewInputRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
        invokeWorkerControlOnce(
            "submitLeasedPlanViewResult",
            correlationId(lease),
            stub ->
                stub.submitLeasedPlanViewResult(
                    SubmitLeasedPlanViewResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success.build())
                        .build()));
    return new RemotePlannerWorkerClient.PlanViewSubmitResult(
        response.getAccepted(), response.getViewsChanged());
  }

  public boolean submitPlanViewFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return invokeWorkerControlOnce(
        "submitLeasedPlanViewResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanViewResult(
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
                .getAccepted());
  }

  public StandalonePlanSnapshotPayload getPlanSnapshotInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanSnapshotInput",
            correlationId(lease),
            stub ->
                stub.getLeasedPlanSnapshotInput(
                    GetLeasedPlanSnapshotInputRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
      RemoteLeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs,
      List<TargetStatsRecord> directStats) {
    ReconcileSnapshotTask effectiveSnapshotTask =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    ReconcileSnapshotTask persistedSnapshotTask =
        effectiveSnapshotTask.completionMode() == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS
            ? snapshotPlanBlobStore.persistDirectStats(
                lease.lease().accountId, lease.lease().jobId, effectiveSnapshotTask, directStats)
            : snapshotPlanBlobStore.persistPlan(
                lease.lease().accountId, lease.lease().jobId, effectiveSnapshotTask, fileGroupJobs);
    SubmitLeasedPlanSnapshotResultRequest.Success.Builder success =
        SubmitLeasedPlanSnapshotResultRequest.Success.newBuilder();
    success.setSnapshotTask(toProtoSnapshotTask(persistedSnapshotTask));
    return invokeWorkerControlOnce(
        "submitLeasedPlanSnapshotResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanSnapshotResult(
                    SubmitLeasedPlanSnapshotResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success.build())
                        .build())
                .getAccepted());
  }

  public boolean submitPlanSnapshotFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    return invokeWorkerControlOnce(
        "submitLeasedPlanSnapshotResult",
        correlationId(lease),
        stub ->
            stub.submitLeasedPlanSnapshotResult(
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
                .getAccepted());
  }

  public StandaloneFileGroupExecutionPayload getExecution(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedFileGroupExecution",
            correlationId(lease),
            stub ->
                stub.getLeasedFileGroupExecution(
                    GetLeasedFileGroupExecutionRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
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
                    .collect(java.util.stream.Collectors.toSet()),
                fromProtoDefaultColumnScope(execution.getCapturePolicy().getDefaultColumnScope()),
                execution.getCapturePolicy().getMaxDefaultColumns())
            : ReconcileCapturePolicy.empty());
  }

  public boolean submitSuccess(RemoteLeasedJob lease, StandaloneFileGroupExecutionResult result) {
    StandaloneFileGroupExecutionResult.FileStatsBlobManifest statsBlobManifest =
        result.fileStatsBlobManifest() == null
            ? StandaloneFileGroupExecutionResult.FileStatsBlobManifest.empty()
            : result.fileStatsBlobManifest();
    SubmitLeasedFileGroupExecutionResultRequest.Success.Builder success =
        SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
            .setResultId(result.resultId() == null ? "" : result.resultId());
    if (!statsBlobManifest.isEmpty()) {
      success
          .setFileStatsBlobUri(statsBlobManifest.blobUri())
          .setFileStatsRecordCount(statsBlobManifest.recordCount());
    } else {
      success.addAllStatsRecords(result.statsRecords());
    }
    for (var artifact : result.preUploadedIndexArtifacts()) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      success.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact.newBuilder()
              .setRecord(artifact.record())
              .setContentType(artifact.contentType() == null ? "" : artifact.contentType())
              .setUploadedArtifactUri(
                  artifact.uploadedArtifactUri() == null ? "" : artifact.uploadedArtifactUri())
              .build());
    }
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
    String resultId = result.resultId() == null ? "" : result.resultId().trim();
    return invokeWorkerControl(
        "submitLeasedFileGroupExecutionResult",
        correlationId(lease),
        !resultId.isBlank(),
        stub ->
            stub.submitLeasedFileGroupExecutionResult(
                    SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success.build())
                        .build())
                .getAccepted());
  }

  public boolean submitFailure(RemoteLeasedJob lease, String resultId, String message) {
    String stableResultId = resultId == null ? "" : resultId.trim();
    return invokeWorkerControl(
        "submitLeasedFileGroupExecutionResult",
        correlationId(lease),
        !stableResultId.isBlank(),
        stub ->
            stub.submitLeasedFileGroupExecutionResult(
                    SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setFailure(
                            SubmitLeasedFileGroupExecutionResultRequest.Failure.newBuilder()
                                .setResultId(stableResultId)
                                .setMessage(message == null ? "" : message)
                                .build())
                        .build())
                .getAccepted());
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
                    .collect(java.util.stream.Collectors.toSet()),
                fromProtoDefaultColumnScope(scope.getCapturePolicy().getDefaultColumnScope()),
                scope.getCapturePolicy().getMaxDefaultColumns())
            : ReconcileCapturePolicy.empty(),
        scope.hasSnapshotSelection()
            ? fromProtoSnapshotSelection(scope.getSnapshotSelection())
            : ReconcileSnapshotSelection.unspecified());
  }

  private static ReconcileSnapshotSelection fromProtoSnapshotSelection(
      ai.floedb.floecat.reconciler.rpc.SnapshotSelection selection) {
    if (selection == null) {
      return ReconcileSnapshotSelection.unspecified();
    }
    return switch (selection.getKind()) {
      case SSK_CURRENT -> ReconcileSnapshotSelection.current();
      case SSK_LATEST_N -> ReconcileSnapshotSelection.latestN(selection.getLatestN());
      case SSK_EXPLICIT ->
          ReconcileSnapshotSelection.explicit(
              selection.getSnapshotIdsList().stream().map(Long::valueOf).toList());
      case SSK_ALL -> ReconcileSnapshotSelection.all();
      case SSK_UNSPECIFIED, UNRECOGNIZED -> ReconcileSnapshotSelection.unspecified();
    };
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
    if (effectiveScope.hasSnapshotSelection()) {
      builder.setSnapshotSelection(toProtoSnapshotSelection(effectiveScope.snapshotSelection()));
    }
    return builder.build();
  }

  private static ai.floedb.floecat.reconciler.rpc.SnapshotSelection toProtoSnapshotSelection(
      ReconcileSnapshotSelection selection) {
    ReconcileSnapshotSelection effective =
        selection == null ? ReconcileSnapshotSelection.unspecified() : selection;
    var builder = ai.floedb.floecat.reconciler.rpc.SnapshotSelection.newBuilder();
    switch (effective.kind()) {
      case CURRENT ->
          builder.setKind(ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind.SSK_CURRENT);
      case LATEST_N ->
          builder
              .setKind(ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind.SSK_LATEST_N)
              .setLatestN(effective.latestN());
      case EXPLICIT ->
          builder
              .setKind(ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind.SSK_EXPLICIT)
              .addAllSnapshotIds(effective.snapshotIds());
      case ALL -> builder.setKind(ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind.SSK_ALL);
      case UNSPECIFIED ->
          builder.setKind(ai.floedb.floecat.reconciler.rpc.SnapshotSelectionKind.SSK_UNSPECIFIED);
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
        .setDefaultColumnScope(toProtoDefaultColumnScope(effective.defaultColumnScope()))
        .setMaxDefaultColumns(effective.maxDefaultColumns())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.DefaultColumnScope toProtoDefaultColumnScope(
      ReconcileCapturePolicy.DefaultColumnScope scope) {
    return switch (scope == null ? ReconcileCapturePolicy.DefaultColumnScope.FIRST_N : scope) {
      case ALL -> ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_ALL;
      case EXPLICIT_ONLY -> ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_EXPLICIT_ONLY;
      case FIRST_N -> ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_FIRST_N;
    };
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

  private static ReconcileCapturePolicy.DefaultColumnScope fromProtoDefaultColumnScope(
      ai.floedb.floecat.reconciler.rpc.DefaultColumnScope scope) {
    return switch (scope) {
      case DCS_ALL -> ReconcileCapturePolicy.DefaultColumnScope.ALL;
      case DCS_EXPLICIT_ONLY -> ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
      case DCS_FIRST_N, DCS_UNSPECIFIED, UNRECOGNIZED ->
          ReconcileCapturePolicy.DefaultColumnScope.FIRST_N;
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
        .setFileGroupPlanBlobUri(effective.fileGroupPlanBlobUri())
        .setFileGroupCount(effective.fileGroupCount())
        .setDirectStatsBlobUri(effective.directStatsBlobUri())
        .setDirectStatsRecordCount(effective.directStatsRecordCount())
        .setCompletionMode(
            switch (effective.completionMode()) {
              case DIRECT_STATS ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.CompletionMode
                      .RSCM_DIRECT_STATS;
              case FILE_GROUPS ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.CompletionMode
                      .RSCM_FILE_GROUPS;
            })
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
        snapshotTask.getFileGroupPlanRecorded(),
        switch (snapshotTask.getCompletionMode()) {
          case RSCM_DIRECT_STATS -> ReconcileSnapshotTask.CompletionMode.DIRECT_STATS;
          case RSCM_FILE_GROUPS, RSCM_UNSPECIFIED, UNRECOGNIZED ->
              ReconcileSnapshotTask.CompletionMode.FILE_GROUPS;
        },
        snapshotTask.getFileGroupPlanBlobUri(),
        snapshotTask.getFileGroupCount(),
        snapshotTask.getDirectStatsBlobUri(),
        snapshotTask.getDirectStatsRecordCount());
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
        0,
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

  private ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub controlStub() {
    if (workerControlHost.isEmpty()) {
      return executorControl;
    }
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub existing = workerControlStub;
    if (existing != null) {
      return existing;
    }
    synchronized (workerControlLock) {
      if (workerControlStub != null) {
        return workerControlStub;
      }
      ManagedChannelBuilder<?> builder =
          ManagedChannelBuilder.forAddress(workerControlHost.orElseThrow(), workerControlPort);
      if (workerControlPlainText) {
        builder.usePlaintext();
      }
      if (workerControlMaxInboundMessageSize > 0) {
        builder.maxInboundMessageSize(workerControlMaxInboundMessageSize);
      }
      ManagedChannel channel = builder.build();
      workerControlChannel = channel;
      workerControlStub = ReconcileExecutorControlGrpc.newBlockingStub(channel);
      return workerControlStub;
    }
  }

  private void resetWorkerControlChannel() {
    ManagedChannel channel = null;
    synchronized (workerControlLock) {
      if (workerControlHost.isEmpty()) {
        return;
      }
      channel = workerControlChannel;
      workerControlChannel = null;
      workerControlStub = null;
    }
    if (channel != null) {
      channel.shutdownNow();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private <T> T invokeWorkerControlRetryable(
      String operation,
      String correlationId,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    return invokeWorkerControl(operation, correlationId, true, invocation);
  }

  private <T> T invokeWorkerControlOnce(
      String operation,
      String correlationId,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    return invokeWorkerControl(operation, correlationId, false, invocation);
  }

  private <T> T invokeWorkerControl(
      String operation,
      String correlationId,
      boolean retryOnTransportFailure,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    RuntimeException lastError = null;
    int maxAttempts = retryOnTransportFailure ? 2 : 1;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return invocation.apply(withHeaders(controlStub(), correlationId));
      } catch (RuntimeException error) {
        lastError = error;
        boolean transportFailure = isTransportFailure(error);
        if (transportFailure) {
          LOG.warnf(
              error,
              "worker-control rpc transport failure op=%s attempt=%d; recreating channel",
              operation,
              attempt);
          resetWorkerControlChannel();
        }
        if (!retryOnTransportFailure || !transportFailure || attempt >= maxAttempts) {
          throw error;
        }
      }
    }
    throw lastError == null ? new IllegalStateException("worker-control rpc failed") : lastError;
  }

  private static boolean isTransportFailure(Throwable error) {
    Throwable current = error;
    java.util.HashSet<Throwable> seen = new java.util.HashSet<>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError) {
        return switch (statusError.getStatus().getCode()) {
          case UNAVAILABLE, INTERNAL, UNKNOWN, DEADLINE_EXCEEDED -> true;
          default -> false;
        };
      }
      current = current.getCause();
    }
    return false;
  }

  private <T extends AbstractStub<T>> T withHeaders(T stub, String correlationId) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadata(correlationId)));
  }

  Metadata metadata(String correlationId) {
    Metadata metadata = new Metadata();
    metadata.put(CORRELATION_ID, correlationId == null ? "" : correlationId);
    attachWorkerAuthorization(metadata);
    return metadata;
  }

  private void attachWorkerAuthorization(Metadata metadata) {
    if (workerAuthHeaderName.isEmpty()) {
      return;
    }
    Optional<String> authorization = reconcileWorkerAuthProvider.authorizationHeader();
    if (authorization.isEmpty()) {
      if (!workerAuthRequired) {
        return;
      }
      throw new IllegalStateException(
          "Reconcile worker authorization header is required but no worker auth configuration is available");
    }
    metadata.put(headerKey(workerAuthHeaderName.orElseThrow()), authorization.orElseThrow());
  }

  private static Metadata.Key<String> headerKey(String headerName) {
    if ("authorization".equalsIgnoreCase(headerName)) {
      return AUTHORIZATION;
    }
    return ReconcileRpcAuthHeaderSupport.headerKey(headerName);
  }

  private static String correlationId(RemoteLeasedJob lease) {
    return "reconcile-job-" + lease.lease().jobId;
  }
}
