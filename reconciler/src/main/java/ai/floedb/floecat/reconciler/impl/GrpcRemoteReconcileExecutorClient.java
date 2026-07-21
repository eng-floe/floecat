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
import ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact;
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
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
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
        RemoteFileGroupWorkerClient,
        RemoteSnapshotFinalizeWorkerClient {
  private static final Logger LOG = Logger.getLogger(GrpcRemoteReconcileExecutorClient.class);

  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ACCOUNT =
      Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  // File-group result chunks drive per-record persistence and partial aggregate merges on the
  // service side, so they need a lower target than finalize-result chunks.
  private static final int FILE_GROUP_RESULT_CHUNK_TARGET_BYTES = 128 * 1024;
  private static final int SNAPSHOT_FINALIZE_RESULT_CHUNK_TARGET_BYTES = 512 * 1024;
  private static final int PLAN_CHILD_JOB_CHUNK_TARGET_BYTES = 128 * 1024;
  private static final int DEFAULT_PLAN_TABLE_CHILD_JOB_CHUNK_MAX_COUNT = 8;

  private final Optional<String> workerAuthHeaderName;
  private final boolean workerAuthRequired;
  private final ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  private final String workerControlHost;
  private final int workerControlPort;
  private final boolean workerControlPlainText;
  private final int workerControlMaxInboundMessageSize;
  private final long workerControlDefaultDeadlineMs;
  private final long workerControlLeaseDeadlineMs;
  private final long workerControlMutationDeadlineMs;
  private final long workerControlKeepAliveTimeMs;
  private final long workerControlKeepAliveTimeoutMs;
  private final boolean workerControlKeepAliveWithoutCalls;
  private final int planTableChildJobChunkMaxCount;
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
      @ConfigProperty(name = "quarkus.grpc.clients.floecat.host") String defaultWorkerControlHost,
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
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.deadline-ms",
              defaultValue = "120000")
          long workerControlDefaultDeadlineMs,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.lease-deadline-ms",
              defaultValue = "120000")
          long workerControlLeaseDeadlineMs,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.mutation-deadline-ms",
              defaultValue = "120000")
          long workerControlMutationDeadlineMs,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.keep-alive-time-ms",
              defaultValue = "30000")
          long workerControlKeepAliveTimeMs,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.keep-alive-timeout-ms",
              defaultValue = "10000")
          long workerControlKeepAliveTimeoutMs,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.grpc.keep-alive-without-calls",
              defaultValue = "false")
          boolean workerControlKeepAliveWithoutCalls,
      @ConfigProperty(
              name = "floecat.reconciler.worker-control.plan-table-child-job-chunk-max-count",
              defaultValue = "8")
          int planTableChildJobChunkMaxCount,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        sessionHeaderName,
        authorizationHeaderName,
        workerAuthRequired,
        workerControlHost,
        defaultWorkerControlHost,
        workerControlPort,
        workerControlPlainText,
        workerControlMaxInboundMessageSize,
        workerControlDefaultDeadlineMs,
        workerControlLeaseDeadlineMs,
        workerControlMutationDeadlineMs,
        workerControlKeepAliveTimeMs,
        workerControlKeepAliveTimeoutMs,
        workerControlKeepAliveWithoutCalls,
        planTableChildJobChunkMaxCount,
        reconcileWorkerAuthProvider,
        true);
  }

  GrpcRemoteReconcileExecutorClient(
      String workerAuthHeaderName, ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        Optional.ofNullable(workerAuthHeaderName),
        Optional.empty(),
        true,
        Optional.of("127.0.0.1"),
        "127.0.0.1",
        9100,
        true,
        0,
        120_000L,
        120_000L,
        120_000L,
        30_000L,
        10_000L,
        false,
        DEFAULT_PLAN_TABLE_CHILD_JOB_CHUNK_MAX_COUNT,
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
        Optional.of("127.0.0.1"),
        "127.0.0.1",
        9100,
        true,
        0,
        120_000L,
        120_000L,
        120_000L,
        30_000L,
        10_000L,
        false,
        DEFAULT_PLAN_TABLE_CHILD_JOB_CHUNK_MAX_COUNT,
        reconcileWorkerAuthProvider,
        true);
  }

  GrpcRemoteReconcileExecutorClient(
      String workerAuthHeaderName,
      String workerControlHost,
      int workerControlPort,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider) {
    this(
        Optional.ofNullable(workerAuthHeaderName),
        Optional.empty(),
        true,
        Optional.ofNullable(workerControlHost),
        "127.0.0.1",
        workerControlPort,
        true,
        0,
        120_000L,
        120_000L,
        120_000L,
        30_000L,
        10_000L,
        false,
        DEFAULT_PLAN_TABLE_CHILD_JOB_CHUNK_MAX_COUNT,
        reconcileWorkerAuthProvider,
        true);
  }

  private GrpcRemoteReconcileExecutorClient(
      Optional<String> sessionHeaderName,
      Optional<String> authorizationHeaderName,
      boolean workerAuthRequired,
      Optional<String> workerControlHost,
      String defaultWorkerControlHost,
      int workerControlPort,
      boolean workerControlPlainText,
      int workerControlMaxInboundMessageSize,
      long workerControlDefaultDeadlineMs,
      long workerControlLeaseDeadlineMs,
      long workerControlMutationDeadlineMs,
      long workerControlKeepAliveTimeMs,
      long workerControlKeepAliveTimeoutMs,
      boolean workerControlKeepAliveWithoutCalls,
      int planTableChildJobChunkMaxCount,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      boolean ignored) {
    this.workerAuthHeaderName =
        ReconcileRpcAuthHeaderSupport.resolveHeaderName(sessionHeaderName, authorizationHeaderName);
    this.workerAuthRequired = workerAuthRequired;
    this.reconcileWorkerAuthProvider = reconcileWorkerAuthProvider;
    this.workerControlHost =
        workerControlHost
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseGet(() -> requireWorkerControlHost(defaultWorkerControlHost));
    this.workerControlPort = workerControlPort;
    this.workerControlPlainText = workerControlPlainText;
    this.workerControlMaxInboundMessageSize = Math.max(0, workerControlMaxInboundMessageSize);
    this.workerControlDefaultDeadlineMs = Math.max(1_000L, workerControlDefaultDeadlineMs);
    this.workerControlLeaseDeadlineMs = Math.max(1_000L, workerControlLeaseDeadlineMs);
    this.workerControlMutationDeadlineMs = Math.max(1_000L, workerControlMutationDeadlineMs);
    this.workerControlKeepAliveTimeMs = Math.max(1_000L, workerControlKeepAliveTimeMs);
    this.workerControlKeepAliveTimeoutMs = Math.max(1_000L, workerControlKeepAliveTimeoutMs);
    this.workerControlKeepAliveWithoutCalls = workerControlKeepAliveWithoutCalls;
    this.planTableChildJobChunkMaxCount = Math.max(1, planTableChildJobChunkMaxCount);
  }

  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;

  @PreDestroy
  void destroy() {
    resetWorkerControlChannel(true);
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
            null,
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
    invokeWorkerControlMutationOnce(
        "startLeasedReconcileJob",
        correlationId(lease),
        lease.lease().accountId,
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
            lease.lease().accountId,
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
            lease.lease().accountId,
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
        invokeWorkerControlMutationOnce(
            "completeLeasedReconcileJob",
            correlationId(lease),
            lease.lease().accountId,
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
        lease.lease().accountId,
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
            lease.lease().accountId,
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
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanConnectorResult",
          correlationId(lease),
          lease.lease().accountId,
          stub ->
              stub.submitLeasedPlanConnectorResult(
                      SubmitLeasedPlanConnectorResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setSuccess(success.build())
                          .build())
                  .getAccepted());
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanConnectorResult", error);
    }
  }

  public boolean submitPlanConnectorFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanConnectorResult",
          correlationId(lease),
          lease.lease().accountId,
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
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanConnectorResult", error);
    }
  }

  public StandalonePlanTablePayload getPlanTableInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanTableInput",
            correlationId(lease),
            lease.lease().accountId,
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
    List<ai.floedb.floecat.reconciler.rpc.PlannedSnapshotPlanJob> protoSnapshotJobs =
        new ArrayList<>();
    for (PlannedSnapshotJob snapshotJob :
        snapshotJobs == null ? List.<PlannedSnapshotJob>of() : snapshotJobs) {
      if (snapshotJob == null || snapshotJob.snapshotTask() == null) {
        continue;
      }
      protoSnapshotJobs.add(
          ai.floedb.floecat.reconciler.rpc.PlannedSnapshotPlanJob.newBuilder()
              .setScope(toProtoScope(snapshotJob.scope(), lease.lease()))
              .setSnapshotTask(toProtoSnapshotTask(snapshotJob.snapshotTask()))
              .build());
    }
    List<List<ai.floedb.floecat.reconciler.rpc.PlannedSnapshotPlanJob>> chunks =
        chunksBySerializedSizeAndCount(
            protoSnapshotJobs, PLAN_CHILD_JOB_CHUNK_TARGET_BYTES, planTableChildJobChunkMaxCount);
    try {
      for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
        int submittedChunkIndex = chunkIndex;
        List<ai.floedb.floecat.reconciler.rpc.PlannedSnapshotPlanJob> chunk =
            chunks.get(chunkIndex);
        boolean accepted =
            invokeWorkerControlMutationOnce(
                "submitLeasedPlanTableResult",
                correlationId(lease),
                lease.lease().accountId,
                stub ->
                    stub.submitLeasedPlanTableResult(
                            SubmitLeasedPlanTableResultRequest.newBuilder()
                                .setJobId(lease.lease().jobId)
                                .setLeaseEpoch(lease.lease().leaseEpoch)
                                .setChunk(
                                    SubmitLeasedPlanTableResultRequest.Chunk.newBuilder()
                                        .setChunkIndex(submittedChunkIndex)
                                        .addAllSnapshotJobs(chunk)
                                        .build())
                                .build())
                        .getAccepted());
        if (!accepted) {
          return false;
        }
      }
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanTableResult", error);
    }
    SubmitLeasedPlanTableResultRequest.Success.Builder success =
        SubmitLeasedPlanTableResultRequest.Success.newBuilder()
            .setTablesScanned(tablesScanned)
            .setTablesChanged(tablesChanged)
            .setErrors(errors)
            .setSnapshotsProcessed(snapshotsProcessed)
            .setStatsProcessed(statsProcessed)
            .setChunkCount(chunks.size());
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanTableResult",
          correlationId(lease),
          lease.lease().accountId,
          stub ->
              stub.submitLeasedPlanTableResult(
                      SubmitLeasedPlanTableResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setSuccess(success.build())
                          .build())
                  .getAccepted());
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanTableResult", error);
    }
  }

  public boolean submitPlanTableFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanTableResult",
          correlationId(lease),
          lease.lease().accountId,
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
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanTableResult", error);
    }
  }

  public StandalonePlanViewPayload getPlanViewInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanViewInput",
            correlationId(lease),
            lease.lease().accountId,
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
    SubmitLeasedPlanViewResultResponse response;
    try {
      response =
          invokeWorkerControlMutationOnce(
              "submitLeasedPlanViewResult",
              correlationId(lease),
              lease.lease().accountId,
              stub ->
                  stub.submitLeasedPlanViewResult(
                      SubmitLeasedPlanViewResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setSuccess(success.build())
                          .build()));
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanViewResult", error);
    }
    return new RemotePlannerWorkerClient.PlanViewSubmitResult(
        response.getAccepted(), response.getViewsChanged());
  }

  public boolean submitPlanViewFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanViewResult",
          correlationId(lease),
          lease.lease().accountId,
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
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanViewResult", error);
    }
  }

  public StandalonePlanSnapshotPayload getPlanSnapshotInput(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedPlanSnapshotInput",
            correlationId(lease),
            lease.lease().accountId,
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
    List<List<ai.floedb.floecat.reconciler.rpc.PlannedFileGroupPlanJob>> chunks = List.of();
    if (persistedSnapshotTask.completionMode()
        == ReconcileSnapshotTask.CompletionMode.FILE_GROUPS) {
      List<ai.floedb.floecat.reconciler.rpc.PlannedFileGroupPlanJob> protoFileGroupJobs =
          new ArrayList<>();
      for (PlannedFileGroupJob fileGroupJob :
          fileGroupJobs == null ? List.<PlannedFileGroupJob>of() : fileGroupJobs) {
        if (fileGroupJob == null || fileGroupJob.fileGroupTask() == null) {
          continue;
        }
        protoFileGroupJobs.add(
            ai.floedb.floecat.reconciler.rpc.PlannedFileGroupPlanJob.newBuilder()
                .setScope(toProtoScope(fileGroupJob.scope(), lease.lease()))
                .setFileGroupTask(toProtoFileGroupTask(fileGroupJob.fileGroupTask()))
                .build());
      }
      try {
        chunks = chunksBySerializedSize(protoFileGroupJobs, PLAN_CHILD_JOB_CHUNK_TARGET_BYTES);
        for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
          int submittedChunkIndex = chunkIndex;
          List<ai.floedb.floecat.reconciler.rpc.PlannedFileGroupPlanJob> chunk =
              chunks.get(chunkIndex);
          boolean accepted =
              invokeWorkerControlMutationOnce(
                  "submitLeasedPlanSnapshotResult",
                  correlationId(lease),
                  lease.lease().accountId,
                  stub ->
                      stub.submitLeasedPlanSnapshotResult(
                              SubmitLeasedPlanSnapshotResultRequest.newBuilder()
                                  .setJobId(lease.lease().jobId)
                                  .setLeaseEpoch(lease.lease().leaseEpoch)
                                  .setChunk(
                                      SubmitLeasedPlanSnapshotResultRequest.Chunk.newBuilder()
                                          .setSnapshotTask(
                                              toProtoSnapshotTask(persistedSnapshotTask))
                                          .setChunkIndex(submittedChunkIndex)
                                          .addAllFileGroupJobs(chunk)
                                          .build())
                                  .build())
                          .getAccepted());
          if (!accepted) {
            return false;
          }
        }
      } catch (RuntimeException error) {
        throw leasePreconditionOrOriginal("submitLeasedPlanSnapshotResult", error);
      }
    }
    SubmitLeasedPlanSnapshotResultRequest.Success.Builder success =
        SubmitLeasedPlanSnapshotResultRequest.Success.newBuilder();
    success
        .setSnapshotTask(toProtoSnapshotTask(persistedSnapshotTask))
        .setChunkCount(chunks.size());
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanSnapshotResult",
          correlationId(lease),
          lease.lease().accountId,
          stub ->
              stub.submitLeasedPlanSnapshotResult(
                      SubmitLeasedPlanSnapshotResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setSuccess(success.build())
                          .build())
                  .getAccepted());
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanSnapshotResult", error);
    }
  }

  public boolean submitPlanSnapshotFailure(
      RemoteLeasedJob lease,
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message) {
    try {
      return invokeWorkerControlMutationOnce(
          "submitLeasedPlanSnapshotResult",
          correlationId(lease),
          lease.lease().accountId,
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
    } catch (RuntimeException error) {
      throw leasePreconditionOrOriginal("submitLeasedPlanSnapshotResult", error);
    }
  }

  public StandaloneFileGroupExecutionPayload getExecution(RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedFileGroupExecution",
            correlationId(lease),
            lease.lease().accountId,
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
        execution.getStorageLocation(),
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

  public boolean submitSuccess(
      RemoteLeasedJob lease,
      StandaloneFileGroupExecutionPayload payload,
      StandaloneFileGroupExecutionResult result) {
    String resultId = result.resultId() == null ? "" : result.resultId().trim();
    List<LeasedFileGroupIndexArtifact> indexArtifacts = toProtoIndexArtifacts(result);
    List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> chunks =
        chunkFileGroupResult(resultId, result.statsRecords(), indexArtifacts);
    boolean retryable = !resultId.isBlank();
    for (SubmitLeasedFileGroupExecutionResultRequest.Chunk chunk : chunks) {
      boolean accepted =
          invokeWorkerControl(
              "submitLeasedFileGroupExecutionResult",
              correlationId(lease),
              lease.lease().accountId,
              retryable,
              stub ->
                  stub.submitLeasedFileGroupExecutionResult(
                          SubmitLeasedFileGroupExecutionResultRequest.newBuilder()
                              .setJobId(lease.lease().jobId)
                              .setLeaseEpoch(lease.lease().leaseEpoch)
                              .setChunk(chunk)
                              .build())
                      .getAccepted());
      if (!accepted) {
        return false;
      }
    }
    ReconcileFileGroupTask plannedTask =
        payload == null
            ? (lease.lease().fileGroupTask == null
                ? ReconcileFileGroupTask.empty()
                : lease.lease().fileGroupTask)
            : ReconcileFileGroupTask.of(
                payload.planId(),
                payload.groupId(),
                payload.tableId() == null ? "" : payload.tableId().getId(),
                payload.snapshotId(),
                payload.plannedFilePaths());
    SubmitLeasedFileGroupExecutionResultRequest.Success.Builder success =
        SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
            .setResultId(resultId)
            .setChunkCount(chunks.size())
            .addAllFileResults(
                FileGroupExecutionSupport.fileResultsForSuccess(
                        plannedTask, result.statsRecords(), result.stagedIndexArtifacts())
                    .stream()
                    .map(GrpcRemoteReconcileExecutorClient::toProtoFileResult)
                    .toList());
    return invokeWorkerControl(
        "submitLeasedFileGroupExecutionResult",
        correlationId(lease),
        lease.lease().accountId,
        retryable,
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
    if (stableResultId.isBlank()) {
      return invokeWorkerControlMutationOnce(
          "submitLeasedFileGroupExecutionResult",
          correlationId(lease),
          lease.lease().accountId,
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
    return invokeWorkerControlRetryable(
        "submitLeasedFileGroupExecutionResult",
        correlationId(lease),
        lease.lease().accountId,
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

  @Override
  public boolean submitSnapshotFinalizeSuccess(
      RemoteLeasedJob lease, String resultId, List<TargetStatsRecord> statsRecords) {
    String stableResultId = resultId == null ? "" : resultId.trim();
    boolean retryable = !stableResultId.isBlank();
    for (SubmitLeasedSnapshotFinalizeResultRequest.Chunk chunk :
        chunkSnapshotFinalizeResult(stableResultId, statsRecords)) {
      boolean accepted =
          invokeWorkerControl(
              "submitLeasedSnapshotFinalizeResult",
              correlationId(lease),
              lease.lease().accountId,
              retryable,
              stub ->
                  stub.submitLeasedSnapshotFinalizeResult(
                          SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
                              .setJobId(lease.lease().jobId)
                              .setLeaseEpoch(lease.lease().leaseEpoch)
                              .setChunk(chunk)
                              .build())
                      .getAccepted());
      if (!accepted) {
        return false;
      }
    }
    return invokeWorkerControl(
        "submitLeasedSnapshotFinalizeResult",
        correlationId(lease),
        lease.lease().accountId,
        retryable,
        stub ->
            stub.submitLeasedSnapshotFinalizeResult(
                    SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(
                            SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
                                .setResultId(stableResultId)
                                .build())
                        .build())
                .getAccepted());
  }

  private static List<LeasedFileGroupIndexArtifact> toProtoIndexArtifacts(
      StandaloneFileGroupExecutionResult result) {
    List<LeasedFileGroupIndexArtifact> out = new ArrayList<>();
    for (var artifact : result.stagedIndexArtifacts()) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      out.add(
          LeasedFileGroupIndexArtifact.newBuilder()
              .setRecord(artifact.record())
              .setContent(
                  artifact.content() == null
                      ? ByteString.EMPTY
                      : ByteString.copyFrom(artifact.content()))
              .setContentType(artifact.contentType() == null ? "" : artifact.contentType())
              .build());
    }
    return List.copyOf(out);
  }

  private static List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> chunkFileGroupResult(
      String resultId,
      List<TargetStatsRecord> statsRecords,
      List<LeasedFileGroupIndexArtifact> indexArtifacts) {
    List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> out = new ArrayList<>();
    int chunkIndex = 0;
    List<TargetStatsRecord> currentStats = new ArrayList<>();
    List<LeasedFileGroupIndexArtifact> currentArtifacts = new ArrayList<>();
    int currentBytes = 0;
    for (TargetStatsRecord record :
        statsRecords == null ? List.<TargetStatsRecord>of() : statsRecords) {
      if (record == null) {
        continue;
      }
      int itemBytes = estimatedChunkItemBytes(record);
      if (currentBytes > 0 && currentBytes + itemBytes > FILE_GROUP_RESULT_CHUNK_TARGET_BYTES) {
        out.add(buildFileGroupChunk(resultId, chunkIndex++, currentStats, currentArtifacts));
        currentStats = new ArrayList<>();
        currentArtifacts = new ArrayList<>();
        currentBytes = 0;
      }
      currentStats.add(record);
      currentBytes += itemBytes;
    }
    for (LeasedFileGroupIndexArtifact artifact :
        indexArtifacts == null ? List.<LeasedFileGroupIndexArtifact>of() : indexArtifacts) {
      if (artifact == null) {
        continue;
      }
      int itemBytes = estimatedChunkItemBytes(artifact);
      if (currentBytes > 0 && currentBytes + itemBytes > FILE_GROUP_RESULT_CHUNK_TARGET_BYTES) {
        out.add(buildFileGroupChunk(resultId, chunkIndex++, currentStats, currentArtifacts));
        currentStats = new ArrayList<>();
        currentArtifacts = new ArrayList<>();
        currentBytes = 0;
      }
      currentArtifacts.add(artifact);
      currentBytes += itemBytes;
    }
    if (!currentStats.isEmpty() || !currentArtifacts.isEmpty()) {
      out.add(buildFileGroupChunk(resultId, chunkIndex, currentStats, currentArtifacts));
    }
    return List.copyOf(out);
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Chunk buildFileGroupChunk(
      String resultId,
      int chunkIndex,
      List<TargetStatsRecord> statsRecords,
      List<LeasedFileGroupIndexArtifact> indexArtifacts) {
    return SubmitLeasedFileGroupExecutionResultRequest.Chunk.newBuilder()
        .setResultId(resultId)
        .setChunkIndex(chunkIndex)
        .addAllStatsRecords(statsRecords)
        .addAllIndexArtifacts(indexArtifacts)
        .build();
  }

  private static List<SubmitLeasedSnapshotFinalizeResultRequest.Chunk> chunkSnapshotFinalizeResult(
      String resultId, List<TargetStatsRecord> statsRecords) {
    List<SubmitLeasedSnapshotFinalizeResultRequest.Chunk> out = new ArrayList<>();
    int chunkIndex = 0;
    List<TargetStatsRecord> current = new ArrayList<>();
    int currentBytes = 0;
    for (TargetStatsRecord record :
        statsRecords == null ? List.<TargetStatsRecord>of() : statsRecords) {
      if (record == null) {
        continue;
      }
      int itemBytes = estimatedChunkItemBytes(record);
      if (currentBytes > 0
          && currentBytes + itemBytes > SNAPSHOT_FINALIZE_RESULT_CHUNK_TARGET_BYTES) {
        out.add(
            SubmitLeasedSnapshotFinalizeResultRequest.Chunk.newBuilder()
                .setResultId(resultId)
                .setChunkIndex(chunkIndex++)
                .addAllStatsRecords(current)
                .build());
        current = new ArrayList<>();
        currentBytes = 0;
      }
      current.add(record);
      currentBytes += itemBytes;
    }
    if (!current.isEmpty()) {
      out.add(
          SubmitLeasedSnapshotFinalizeResultRequest.Chunk.newBuilder()
              .setResultId(resultId)
              .setChunkIndex(chunkIndex)
              .addAllStatsRecords(current)
              .build());
    }
    return List.copyOf(out);
  }

  private static <T extends MessageLite> List<List<T>> chunksBySerializedSize(
      List<T> items, int targetBytes) {
    return chunksBySerializedSizeAndCount(items, targetBytes, Integer.MAX_VALUE);
  }

  private static <T extends MessageLite> List<List<T>> chunksBySerializedSizeAndCount(
      List<T> items, int targetBytes, int maxCount) {
    List<List<T>> out = new ArrayList<>();
    List<T> current = new ArrayList<>();
    int currentBytes = 0;
    int effectiveTargetBytes = Math.max(1, targetBytes);
    int effectiveMaxCount = Math.max(1, maxCount);
    for (T item : items == null ? List.<T>of() : items) {
      if (item == null) {
        continue;
      }
      int itemBytes = estimatedChunkItemBytes(item);
      if (!current.isEmpty()
          && (currentBytes + itemBytes > effectiveTargetBytes
              || current.size() >= effectiveMaxCount)) {
        out.add(List.copyOf(current));
        current = new ArrayList<>();
        currentBytes = 0;
      }
      current.add(item);
      currentBytes += itemBytes;
    }
    if (!current.isEmpty()) {
      out.add(List.copyOf(current));
    }
    return List.copyOf(out);
  }

  private static int estimatedChunkItemBytes(MessageLite message) {
    return Math.max(1, message.getSerializedSize()) + 32;
  }

  private static RuntimeException leasePreconditionOrOriginal(
      String operation, RuntimeException error) {
    if (ReconcileLeaseGrpcStatus.isLeasePreconditionFailure(error)) {
      return new RemoteLeasePreconditionFailedException(operation, error);
    }
    return error;
  }

  @Override
  public boolean submitSnapshotFinalizeFailure(
      RemoteLeasedJob lease, String resultId, String message) {
    String stableResultId = resultId == null ? "" : resultId.trim();
    return invokeWorkerControl(
        "submitLeasedSnapshotFinalizeResult",
        correlationId(lease),
        lease.lease().accountId,
        !stableResultId.isBlank(),
        stub ->
            stub.submitLeasedSnapshotFinalizeResult(
                    SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setFailure(
                            SubmitLeasedSnapshotFinalizeResultRequest.Failure.newBuilder()
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
      ai.floedb.floecat.capture.rpc.CaptureOutput output) {
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

  private static ai.floedb.floecat.capture.rpc.CapturePolicy toProtoCapturePolicy(
      ReconcileCapturePolicy capturePolicy) {
    ReconcileCapturePolicy effective =
        capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    return ai.floedb.floecat.capture.rpc.CapturePolicy.newBuilder()
        .addAllColumns(
            effective.columns().stream()
                .map(
                    column ->
                        ai.floedb.floecat.capture.rpc.CaptureColumnPolicy.newBuilder()
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

  private static ai.floedb.floecat.capture.rpc.DefaultColumnScope toProtoDefaultColumnScope(
      ReconcileCapturePolicy.DefaultColumnScope scope) {
    return switch (scope == null ? ReconcileCapturePolicy.DefaultColumnScope.FIRST_N : scope) {
      case ALL -> ai.floedb.floecat.capture.rpc.DefaultColumnScope.DCS_ALL;
      case EXPLICIT_ONLY -> ai.floedb.floecat.capture.rpc.DefaultColumnScope.DCS_EXPLICIT_ONLY;
      case FIRST_N -> ai.floedb.floecat.capture.rpc.DefaultColumnScope.DCS_FIRST_N;
    };
  }

  private static ai.floedb.floecat.capture.rpc.CaptureOutput toProtoCaptureOutput(
      ReconcileCapturePolicy.Output output) {
    return switch (output) {
      case TABLE_STATS -> ai.floedb.floecat.capture.rpc.CaptureOutput.CO_TABLE_STATS;
      case FILE_STATS -> ai.floedb.floecat.capture.rpc.CaptureOutput.CO_FILE_STATS;
      case COLUMN_STATS -> ai.floedb.floecat.capture.rpc.CaptureOutput.CO_COLUMN_STATS;
      case PARQUET_PAGE_INDEX -> ai.floedb.floecat.capture.rpc.CaptureOutput.CO_PARQUET_PAGE_INDEX;
    };
  }

  private static ReconcileCapturePolicy.DefaultColumnScope fromProtoDefaultColumnScope(
      ai.floedb.floecat.capture.rpc.DefaultColumnScope scope) {
    return switch (scope) {
      case DCS_ALL -> ReconcileCapturePolicy.DefaultColumnScope.ALL;
      case DCS_EXPLICIT_ONLY -> ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
      case DCS_FIRST_N, DCS_UNSPECIFIED -> ReconcileCapturePolicy.DefaultColumnScope.FIRST_N;
      case UNRECOGNIZED -> throw new IllegalArgumentException("default column scope");
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
        .setSourceFileCount(effective.sourceFileCount())
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
        .addAllPartialAggregateRecords(effective.partialAggregateRecords())
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
        snapshotTask.getSourceFileCount(),
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
            .toList(),
        fileGroupTask.getPartialAggregateRecordsList());
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
    ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub existing = workerControlStub;
    if (existing != null) {
      return existing;
    }
    synchronized (workerControlLock) {
      if (workerControlStub != null) {
        return workerControlStub;
      }
      ManagedChannel channel = newWorkerControlChannel();
      workerControlChannel = channel;
      workerControlStub = workerControlStub(channel);
      return workerControlStub;
    }
  }

  ManagedChannel newWorkerControlChannel() {
    ManagedChannelBuilder<?> builder =
        ManagedChannelBuilder.forAddress(workerControlHost, workerControlPort);
    if (workerControlPlainText) {
      builder.usePlaintext();
    }
    if (workerControlMaxInboundMessageSize > 0) {
      builder.maxInboundMessageSize(workerControlMaxInboundMessageSize);
    }
    builder.keepAliveTime(workerControlKeepAliveTimeMs, TimeUnit.MILLISECONDS);
    builder.keepAliveTimeout(workerControlKeepAliveTimeoutMs, TimeUnit.MILLISECONDS);
    builder.keepAliveWithoutCalls(workerControlKeepAliveWithoutCalls);
    return builder.build();
  }

  ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub workerControlStub(
      ManagedChannel channel) {
    return ReconcileExecutorControlGrpc.newBlockingStub(channel);
  }

  private void resetWorkerControlChannel() {
    resetWorkerControlChannel(false);
  }

  private void resetWorkerControlChannel(boolean force) {
    ManagedChannel channel = null;
    synchronized (workerControlLock) {
      channel = workerControlChannel;
      workerControlChannel = null;
      workerControlStub = null;
    }
    closeWorkerControlChannel(channel, force);
  }

  void closeWorkerControlChannel(ManagedChannel channel, boolean force) {
    if (channel == null) {
      return;
    }
    if (force) {
      channel.shutdownNow();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return;
    }
    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private <T> T invokeWorkerControlRetryable(
      String operation,
      String correlationId,
      String accountId,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    return invokeWorkerControl(operation, correlationId, accountId, true, invocation);
  }

  private <T> T invokeWorkerControlMutationOnce(
      String operation,
      String correlationId,
      String accountId,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    ManagedChannel channel = null;
    try {
      channel = newWorkerControlChannel();
      return invocation.apply(
          withHeaders(workerControlStub(channel), correlationId, accountId)
              .withDeadlineAfter(deadlineMsFor(operation), TimeUnit.MILLISECONDS));
    } catch (RuntimeException error) {
      if (isTransportFailure(error)) {
        logWorkerControlTransportFailure(operation, "dedicated", 1, error);
      }
      throw error;
    } finally {
      closeWorkerControlChannel(channel, false);
    }
  }

  private <T> T invokeWorkerControl(
      String operation,
      String correlationId,
      String accountId,
      boolean retryOnTransportFailure,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    RuntimeException lastError = null;
    int maxAttempts = retryOnTransportFailure ? 2 : 1;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return invocation.apply(
            withHeaders(controlStub(), correlationId, accountId)
                .withDeadlineAfter(deadlineMsFor(operation), TimeUnit.MILLISECONDS));
      } catch (RuntimeException error) {
        lastError = error;
        boolean transportFailure = isTransportFailure(error);
        if (transportFailure) {
          logWorkerControlTransportFailure(operation, "cached", attempt, error);
          resetWorkerControlChannel();
        }
        if (!retryOnTransportFailure || !transportFailure || attempt >= maxAttempts) {
          throw error;
        }
      }
    }
    throw lastError == null ? new IllegalStateException("worker-control rpc failed") : lastError;
  }

  void logWorkerControlTransportFailure(
      String operation, String path, int attempt, RuntimeException error) {
    LOG.debugf(
        error,
        "worker-control rpc transport failure op=%s path=%s attempt=%d",
        operation,
        path,
        attempt);
  }

  private long deadlineMsFor(String operation) {
    if (operation == null || operation.isBlank()) {
      return workerControlDefaultDeadlineMs;
    }
    return switch (operation) {
      case "renewReconcileLease", "getReconcileCancellation", "reportReconcileProgress" ->
          workerControlLeaseDeadlineMs;
      case "submitLeasedFileGroupExecutionResult",
          "submitLeasedSnapshotFinalizeResult",
          "completeLeasedReconcileJob" ->
          workerControlMutationDeadlineMs;
      default -> workerControlDefaultDeadlineMs;
    };
  }

  private static boolean isTransportFailure(Throwable error) {
    Throwable current = error;
    java.util.HashSet<Throwable> seen = new java.util.HashSet<>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError) {
        return switch (statusError.getStatus().getCode()) {
          case UNAVAILABLE, INTERNAL, UNKNOWN, DEADLINE_EXCEEDED, CANCELLED -> true;
          default -> false;
        };
      }
      current = current.getCause();
    }
    return false;
  }

  private <T extends AbstractStub<T>> T withHeaders(
      T stub, String correlationId, String accountId) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadata(correlationId, accountId)));
  }

  Metadata metadata(String correlationId, String accountId) {
    Metadata metadata = new Metadata();
    metadata.put(CORRELATION_ID, correlationId == null ? "" : correlationId);
    if (accountId != null && !accountId.isBlank()) {
      metadata.put(ACCOUNT, accountId);
    }
    attachWorkerAuthorization(metadata, accountId);
    return metadata;
  }

  private void attachWorkerAuthorization(Metadata metadata, String accountId) {
    if (!workerAuthRequired) {
      return;
    }
    if (workerAuthHeaderName.isEmpty()) {
      return;
    }
    Optional<String> authorization = reconcileWorkerAuthProvider.authorizationHeader(accountId);
    if (authorization.isEmpty()) {
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

  static String requireWorkerControlHost(String host) {
    if (host == null || host.isBlank()) {
      throw new IllegalStateException("Worker-control gRPC host must be configured");
    }
    return host.trim();
  }
}
