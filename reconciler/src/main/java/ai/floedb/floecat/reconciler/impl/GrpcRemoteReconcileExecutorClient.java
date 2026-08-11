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

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan.DeltaDeletionVector;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
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
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.rpc.CommitLeasedFileGroupResultRequest;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.FileGroupArtifactBundleDescriptor;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.reconciler.rpc.GetLeasedFileGroupExecutionRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanConnectorInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanSnapshotInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanTableInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedPlanViewInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetLeasedSnapshotFinalizeInputRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ListLeasedSnapshotFileGroupResultsRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureKind;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryClass;
import ai.floedb.floecat.reconciler.rpc.ReconcileFailureRetryDisposition;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanConnectorResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanSnapshotResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanTableResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedPlanViewResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  @Inject BlobStore blobStore;

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
    SubmitLeasedPlanConnectorResultRequest request =
        SubmitLeasedPlanConnectorResultRequest.newBuilder()
            .setJobId(lease.lease().jobId)
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setSuccess(success.build())
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanConnectorResult",
          "PLAN_CONNECTOR",
          "success",
          lease,
          request,
          stub -> stub.submitLeasedPlanConnectorResult(request).getAccepted());
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
    SubmitLeasedPlanConnectorResultRequest request =
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
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanConnectorResult",
          "PLAN_CONNECTOR",
          "failure",
          lease,
          request,
          stub -> stub.submitLeasedPlanConnectorResult(request).getAccepted());
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
        SubmitLeasedPlanTableResultRequest request =
            SubmitLeasedPlanTableResultRequest.newBuilder()
                .setJobId(lease.lease().jobId)
                .setLeaseEpoch(lease.lease().leaseEpoch)
                .setChunk(
                    SubmitLeasedPlanTableResultRequest.Chunk.newBuilder()
                        .setChunkIndex(submittedChunkIndex)
                        .addAllSnapshotJobs(chunk)
                        .build())
                .build();
        boolean accepted =
            invokePlannerMutationOnce(
                "submitLeasedPlanTableResult",
                "PLAN_TABLE",
                "chunk-" + (submittedChunkIndex + 1) + "-of-" + chunks.size(),
                lease,
                request,
                stub -> stub.submitLeasedPlanTableResult(request).getAccepted());
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
    SubmitLeasedPlanTableResultRequest request =
        SubmitLeasedPlanTableResultRequest.newBuilder()
            .setJobId(lease.lease().jobId)
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setSuccess(success.build())
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanTableResult",
          "PLAN_TABLE",
          "success",
          lease,
          request,
          stub -> stub.submitLeasedPlanTableResult(request).getAccepted());
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
    SubmitLeasedPlanTableResultRequest request =
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
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanTableResult",
          "PLAN_TABLE",
          "failure",
          lease,
          request,
          stub -> stub.submitLeasedPlanTableResult(request).getAccepted());
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
    SubmitLeasedPlanViewResultRequest request =
        SubmitLeasedPlanViewResultRequest.newBuilder()
            .setJobId(lease.lease().jobId)
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setSuccess(success.build())
            .build();
    SubmitLeasedPlanViewResultResponse response;
    try {
      response =
          invokePlannerMutationOnce(
              "submitLeasedPlanViewResult",
              "PLAN_VIEW",
              "success",
              lease,
              request,
              stub -> stub.submitLeasedPlanViewResult(request));
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
    SubmitLeasedPlanViewResultRequest request =
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
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanViewResult",
          "PLAN_VIEW",
          "failure",
          lease,
          request,
          stub -> stub.submitLeasedPlanViewResult(request).getAccepted());
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
    return submitPlanSnapshotSuccess(lease, snapshotTask, fileGroupJobs, directStats, null);
  }

  @Override
  public boolean submitAppendOnlyPlanSnapshotSuccess(
      RemoteLeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs,
      List<TargetStatsRecord> directStats,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase) {
    return submitPlanSnapshotSuccess(
        lease, snapshotTask, fileGroupJobs, directStats, appendOnlyBase);
  }

  private boolean submitPlanSnapshotSuccess(
      RemoteLeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      List<PlannedFileGroupJob> fileGroupJobs,
      List<TargetStatsRecord> directStats,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase) {
    ReconcileSnapshotTask effectiveSnapshotTask =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    ReconcileSnapshotTask persistedSnapshotTask =
        effectiveSnapshotTask.completionMode() == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS
            ? snapshotPlanBlobStore.persistDirectStats(
                lease.lease().accountId, lease.lease().jobId, effectiveSnapshotTask, directStats)
            : appendOnlyBase == null
                ? snapshotPlanBlobStore.persistPlan(
                    lease.lease().accountId,
                    lease.lease().jobId,
                    effectiveSnapshotTask,
                    fileGroupJobs)
                : snapshotPlanBlobStore.persistPlan(
                    lease.lease().accountId,
                    lease.lease().jobId,
                    effectiveSnapshotTask,
                    fileGroupJobs,
                    appendOnlyBase);
    SubmitLeasedPlanSnapshotResultRequest.Success.Builder success =
        SubmitLeasedPlanSnapshotResultRequest.Success.newBuilder();
    success.setSnapshotTask(toProtoSnapshotTask(persistedSnapshotTask)).setChunkCount(0);
    SubmitLeasedPlanSnapshotResultRequest request =
        SubmitLeasedPlanSnapshotResultRequest.newBuilder()
            .setJobId(lease.lease().jobId)
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setSuccess(success.build())
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanSnapshotResult",
          "PLAN_SNAPSHOT",
          "success",
          lease,
          request,
          stub -> stub.submitLeasedPlanSnapshotResult(request).getAccepted());
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
    SubmitLeasedPlanSnapshotResultRequest request =
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
            .build();
    try {
      return invokePlannerMutationOnce(
          "submitLeasedPlanSnapshotResult",
          "PLAN_SNAPSHOT",
          "failure",
          lease,
          request,
          stub -> stub.submitLeasedPlanSnapshotResult(request).getAccepted());
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
        execution.getResultPayloadUri(),
        execution.getStatsObjectPrefix(),
        execution.getFilePathsList(),
        execution.getExecutionSchemaJson(),
        execution.getFileExecutionPlansList().stream()
            .map(GrpcRemoteReconcileExecutorClient::fromProtoFileExecutionPlan)
            .toList(),
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
                execution.getCapturePolicy().getMaxDefaultColumns(),
                execution.getCapturePolicy().getPropertiesMap())
            : ReconcileCapturePolicy.empty(),
        execution.hasIndexPredecessor()
            ? new StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor(
                execution.getIndexPredecessor().getGenerationId(),
                execution.getIndexPredecessor().getActivePointerVersion(),
                execution.getIndexPredecessor().getCaptureManifestUri(),
                execution.getIndexPredecessor().getCaptureManifestPointerVersion())
            : null,
        execution.getPredecessorIndexArtifactsList());
  }

  public boolean submitSuccess(
      RemoteLeasedJob lease,
      StandaloneFileGroupExecutionPayload payload,
      StandaloneFileGroupExecutionResult result) {
    String resultId = result.resultId() == null ? "" : result.resultId().trim();
    if (resultId.isBlank()) {
      throw new IllegalArgumentException("file-group result_id is required");
    }
    if (payload == null
        || payload.resultPayloadUri().isBlank()
        || payload.statsObjectPrefix().isBlank()) {
      throw new IllegalArgumentException(
          "leased file-group result_payload_uri and stats_object_prefix are required");
    }
    validateIndexArtifactCoverage(payload, result);
    List<IndexArtifactRecord> publishedIndexRecords = commitIndexArtifacts(payload, result);
    List<String> realizedIndexSelectors =
        publishedIndexRecords.stream()
            .flatMap(record -> persistedIndexSelectors(record).stream())
            .distinct()
            .sorted()
            .toList();
    List<TargetStatsRecord> partialAggregates = result.partialAggregateRecords();
    ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference reuseBundle =
        publishReuseBundle(payload, result.publishedFileStatsRecords(), publishedIndexRecords);
    StatsObjectDescriptor bundleArtifact = reuseBundle.getArtifact();
    List<StatsObjectDescriptor> fileStatsObjects =
        reuseBundle.getFileStatsList().stream()
            .map(
                metadata ->
                    bundleArtifact.toBuilder()
                        .setTargetStorageId(
                            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
                                ai.floedb.floecat.stats.identity.StatsTargetIdentity.fileTarget(
                                    metadata.getFilePath())))
                        .build())
            .toList();
    List<StatsObjectDescriptor> indexArtifacts =
        reuseBundle.getIndexArtifactsList().stream()
            .map(
                metadata ->
                    bundleArtifact.toBuilder()
                        .setTargetStorageId("file:" + metadata.getFilePath())
                        .build())
            .toList();
    ReconcileFileGroupTask plannedTask =
        ReconcileFileGroupTask.of(
            payload.planId(),
            payload.groupId(),
            payload.tableId() == null ? "" : payload.tableId().getId(),
            payload.snapshotId(),
            payload.plannedFilePaths());
    List<ReconcileFileResult> fileResults =
        FileGroupExecutionSupport.fileResultsForSuccess(
            plannedTask, fileStatsObjects, result.stagedIndexArtifacts());
    FileGroupResultPayload.Builder packedPayloadBuilder =
        FileGroupResultPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId(lease.lease().accountId)
            .setConnectorId(lease.lease().connectorId)
            .setParentJobId(lease.lease().parentJobId)
            .setFileGroupJobId(lease.lease().jobId)
            .setPlanId(payload.planId())
            .setGroupId(payload.groupId())
            .setTableId(payload.tableId() == null ? "" : payload.tableId().getId())
            .setSnapshotId(payload.snapshotId())
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setResultId(resultId)
            .addAllFileResults(
                fileResults.stream()
                    .map(GrpcRemoteReconcileExecutorClient::toProtoFileResult)
                    .toList())
            .addAllPartialAggregateRecords(partialAggregates)
            .addAllIndexArtifacts(indexArtifacts)
            .addAllFileStats(fileStatsObjects)
            .addAllRealizedIndexSelectors(realizedIndexSelectors)
            .addAllRealizedStatsSelectors(result.realizedStatsSelectors())
            .setReusableArtifactBundle(reuseBundle);
    if (payload.capturePageIndex()) {
      packedPayloadBuilder.setIndexPredecessor(toProtoIndexPredecessor(payload.indexPredecessor()));
    }
    FileGroupResultPayload packedPayload = packedPayloadBuilder.build();
    byte[] packedBytes = packedPayload.toByteArray();
    blobStore.put(payload.resultPayloadUri(), packedBytes, "application/x-protobuf");
    String artifactReferencesSha256 =
        ArtifactReferenceDigest.sha256(fileStatsObjects, indexArtifacts);
    FileGroupResultDescriptor.Builder descriptorBuilder =
        FileGroupResultDescriptor.newBuilder()
            .setFormatVersion(1)
            .setAccountId(lease.lease().accountId)
            .setConnectorId(lease.lease().connectorId)
            .setParentJobId(lease.lease().parentJobId)
            .setFileGroupJobId(lease.lease().jobId)
            .setPlanId(payload.planId())
            .setGroupId(payload.groupId())
            .setTableId(payload.tableId() == null ? "" : payload.tableId().getId())
            .setSnapshotId(payload.snapshotId())
            .setLeaseEpoch(lease.lease().leaseEpoch)
            .setResultId(resultId)
            .setPayloadUri(payload.resultPayloadUri())
            .setPayloadBytes(packedBytes.length)
            .setPayloadSha256(ByteString.copyFrom(sha256(packedBytes)))
            .setPlannedFileCount(plannedTask.filePaths().size())
            .setSucceededFileCount(fileResults.size())
            .setPartialAggregateRecordCount(partialAggregates.size())
            .setIndexArtifactCount(indexArtifacts.size())
            .setStatsObjectPrefix(payload.statsObjectPrefix())
            .setFileStatsRecordCount(fileStatsObjects.size())
            .setArtifactReferencesSha256(
                ByteString.copyFrom(HexFormat.of().parseHex(artifactReferencesSha256)))
            .setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()));
    if (payload.capturePageIndex()) {
      descriptorBuilder.setIndexPredecessor(toProtoIndexPredecessor(payload.indexPredecessor()));
    }
    FileGroupResultDescriptor descriptor = descriptorBuilder.build();
    CommitLeasedFileGroupResultRequest.Success success =
        CommitLeasedFileGroupResultRequest.Success.newBuilder()
            .setResultId(resultId)
            .setResultDescriptor(descriptor)
            .setArtifactBundle(
                FileGroupArtifactBundleDescriptor.newBuilder()
                    .setArtifact(reuseBundle.getArtifact())
                    .addAllFileStatsTargetStorageIds(
                        fileStatsObjects.stream()
                            .map(StatsObjectDescriptor::getTargetStorageId)
                            .toList())
                    .addAllIndexArtifactTargetStorageIds(
                        indexArtifacts.stream()
                            .map(StatsObjectDescriptor::getTargetStorageId)
                            .toList()))
            .build();
    return invokeWorkerControl(
        "commitLeasedFileGroupResult",
        correlationId(lease),
        lease.lease().accountId,
        true,
        stub ->
            stub.commitLeasedFileGroupResult(
                    CommitLeasedFileGroupResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setSuccess(success)
                        .build())
                .getAccepted());
  }

  private ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference publishReuseBundle(
      StandaloneFileGroupExecutionPayload execution,
      List<TargetStatsRecord> stats,
      List<IndexArtifactRecord> indexes) {
    Map<String, TargetStatsRecord> uniqueStats = new java.util.TreeMap<>();
    stats.forEach(
        record ->
            uniqueStats.putIfAbsent(
                ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(record.getTarget()),
                record));
    Map<String, IndexArtifactRecord> uniqueIndexes = new java.util.TreeMap<>();
    indexes.forEach(
        record ->
            uniqueIndexes.putIfAbsent(
                record.hasTarget() && record.getTarget().hasFile()
                    ? record.getTarget().getFile().getFilePath()
                    : "",
                record));
    var payload =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addAllFileStats(uniqueStats.values())
            .addAllIndexArtifacts(uniqueIndexes.values())
            .build();
    byte[] bytes = payload.toByteArray();
    byte[] digest = sha256(bytes);
    String uri =
        execution.statsObjectPrefix()
            + ReusableArtifactBundleUris.BUNDLE_DIRECTORY
            + HexFormat.of().formatHex(digest)
            + ".pb";
    blobStore.put(uri, bytes, "application/x-protobuf");
    var reference =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:" + execution.groupId())
                    .setPayloadUri(uri)
                    .setPayloadBytes(bytes.length)
                    .setPayloadSha256(ByteString.copyFrom(digest)));
    for (TargetStatsRecord record : uniqueStats.values()) {
      reference.addFileStats(
          ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
              .setFilePath(
                  record.hasTarget() && record.getTarget().hasFile()
                      ? record.getTarget().getFile().getFilePath()
                      : "")
              .setSourceFingerprint(
                  record.getPropertiesOrDefault(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, ""))
              .setStatsCaptureSignature(
                  record.getPropertiesOrDefault(FileArtifactReuse.STATS_SIGNATURE_PROPERTY, ""))
              .addAllRealizedStatsSelectors(
                  FileArtifactReuse.decodeSelectors(
                      record.getPropertiesOrDefault(
                          FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, ""))));
    }
    for (IndexArtifactRecord record : uniqueIndexes.values()) {
      reference.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
              .setFilePath(
                  record.hasTarget() && record.getTarget().hasFile()
                      ? record.getTarget().getFile().getFilePath()
                      : "")
              .setSourceFingerprint(
                  record.getPropertiesOrDefault(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, ""))
              .setIndexCaptureSignature(
                  record.getPropertiesOrDefault(FileArtifactReuse.INDEX_SIGNATURE_PROPERTY, ""))
              .addAllRealizedIndexSelectors(
                  persistedIndexSelectors(record).stream().sorted().toList()));
    }
    return reference.build();
  }

  public boolean submitFailure(RemoteLeasedJob lease, String resultId, String message) {
    String stableResultId = resultId == null ? "" : resultId.trim();
    if (stableResultId.isBlank()) {
      return invokeWorkerControlMutationOnce(
          "commitLeasedFileGroupResult",
          correlationId(lease),
          lease.lease().accountId,
          stub ->
              stub.commitLeasedFileGroupResult(
                      CommitLeasedFileGroupResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setFailure(
                              CommitLeasedFileGroupResultRequest.Failure.newBuilder()
                                  .setResultId(stableResultId)
                                  .setMessage(message == null ? "" : message)
                                  .build())
                          .build())
                  .getAccepted());
    }
    return invokeWorkerControlRetryable(
        "commitLeasedFileGroupResult",
        correlationId(lease),
        lease.lease().accountId,
        stub ->
            stub.commitLeasedFileGroupResult(
                    CommitLeasedFileGroupResultRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .setFailure(
                            CommitLeasedFileGroupResultRequest.Failure.newBuilder()
                                .setResultId(stableResultId)
                                .setMessage(message == null ? "" : message)
                                .build())
                        .build())
                .getAccepted());
  }

  @Override
  public StandaloneSnapshotFinalizeExecutionPayload getSnapshotFinalizeInput(
      RemoteLeasedJob lease) {
    var response =
        invokeWorkerControlRetryable(
            "getLeasedSnapshotFinalizeInput",
            correlationId(lease),
            lease.lease().accountId,
            stub ->
                stub.getLeasedSnapshotFinalizeInput(
                    GetLeasedSnapshotFinalizeInputRequest.newBuilder()
                        .setJobId(lease.lease().jobId)
                        .setLeaseEpoch(lease.lease().leaseEpoch)
                        .build()));
    var input = response.getInput();
    var finalizeMode = input.getFinalizeMode();
    if (finalizeMode
            != ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                .FZM_FILE_GROUPS_NON_EMPTY
        && finalizeMode
            != ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                .FZM_EXPLICIT_EMPTY
        && finalizeMode
            != ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                .FZM_APPEND_ONLY) {
      throw new IllegalArgumentException("remote descriptor finalizer requires file-group input");
    }
    if ((finalizeMode
                == ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                    .FZM_FILE_GROUPS_NON_EMPTY
            && input.getFileGroupCount() == 0)
        || (finalizeMode
                == ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                    .FZM_EXPLICIT_EMPTY
            && (input.getFileGroupCount() != 0 || input.getSourceFileCount() != 0))
        || (finalizeMode
                == ai.floedb.floecat.reconciler.rpc.LeasedSnapshotFinalizeInput.FinalizeMode
                    .FZM_APPEND_ONLY
            && input.getSourceFileCount() == 0)) {
      throw new IllegalArgumentException(
          "snapshot finalize mode does not match file-group coverage");
    }
    return new StandaloneSnapshotFinalizeExecutionPayload(
        input.getJobId(),
        input.getLeaseEpoch(),
        input.getParentJobId(),
        input.getTableId(),
        input.getSnapshotId(),
        input.getFullRescan(),
        input.getSourceFileCount(),
        input.getSnapshotPlanUri(),
        input.getFileGroupCount(),
        input.getStatsObjectPrefix(),
        input.getDurableCaptureManifestPrefix(),
        input.getReusableArtifactIndexObjectPrefix(),
        input.getStatsGenerationManifestUri(),
        input.getIndexGenerationCaptureManifestPrefix(),
        input.hasIndexPredecessor()
            ? fromProtoIndexPredecessor(input.getIndexPredecessor())
            : null);
  }

  @Override
  public List<ReconcileFileGroupResultDescriptor> listSnapshotFileGroupResults(
      RemoteLeasedJob lease) {
    ReconcileSnapshotTask snapshotTask = lease.lease().snapshotTask;
    if (snapshotTask == null) {
      throw new IllegalArgumentException("snapshot finalize lease is missing its snapshot task");
    }
    int expectedFileGroups = Math.max(0, snapshotTask.fileGroupCount());
    // The finalizer API returns the complete descriptor set, so its resident memory is bounded by
    // the file-group count persisted in the leased snapshot plan. Reject a server overrun instead
    // of allowing an inconsistent or malicious response to grow this list without bound.
    List<ReconcileFileGroupResultDescriptor> descriptors = new ArrayList<>();
    Set<String> seenPageTokens = new HashSet<>();
    String pageToken = "";
    do {
      if (!seenPageTokens.add(pageToken)) {
        throw new IllegalStateException(
            "snapshot file-group result paging repeated token: " + pageToken);
      }
      String requestedPageToken = pageToken;
      var response =
          invokeWorkerControlRetryable(
              "listLeasedSnapshotFileGroupResults",
              correlationId(lease),
              lease.lease().accountId,
              stub ->
                  stub.listLeasedSnapshotFileGroupResults(
                      ListLeasedSnapshotFileGroupResultsRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setPageSize(200)
                          .setPageToken(requestedPageToken)
                          .build()));
      if (response.getDescriptorsCount() > expectedFileGroups - descriptors.size()) {
        throw new IllegalStateException(
            "snapshot file-group result count exceeds planned count " + expectedFileGroups);
      }
      descriptors.addAll(
          response.getDescriptorsList().stream()
              .map(GrpcRemoteReconcileExecutorClient::fromProtoFileGroupResultDescriptor)
              .toList());
      pageToken = response.getNextPageToken();
    } while (!pageToken.isBlank());
    return List.copyOf(descriptors);
  }

  private static ReconcileFileGroupResultDescriptor fromProtoFileGroupResultDescriptor(
      FileGroupResultDescriptor descriptor) {
    return new ReconcileFileGroupResultDescriptor(
        descriptor.getFormatVersion(),
        descriptor.getAccountId(),
        descriptor.getConnectorId(),
        descriptor.getParentJobId(),
        descriptor.getFileGroupJobId(),
        descriptor.getPlanId(),
        descriptor.getGroupId(),
        descriptor.getTableId(),
        descriptor.getSnapshotId(),
        descriptor.getLeaseEpoch(),
        descriptor.getResultId(),
        descriptor.getPayloadUri(),
        descriptor.getPayloadBytes(),
        java.util.Base64.getEncoder().encodeToString(descriptor.getPayloadSha256().toByteArray()),
        descriptor.getPlannedFileCount(),
        descriptor.getSucceededFileCount(),
        descriptor.getFailedFileCount(),
        descriptor.getSkippedFileCount(),
        descriptor.getPartialAggregateRecordCount(),
        descriptor.getIndexArtifactCount(),
        descriptor.getStatsObjectPrefix(),
        descriptor.getFileStatsRecordCount(),
        HexFormat.of().formatHex(descriptor.getArtifactReferencesSha256().toByteArray()),
        descriptor.hasIndexPredecessor()
            ? fromProtoIndexPredecessor(descriptor.getIndexPredecessor())
            : null,
        descriptor.hasCreatedAt() ? Timestamps.toMillis(descriptor.getCreatedAt()) : 0L);
  }

  @Override
  public PreparedSnapshotFinalizeSuccess prepareSnapshotFinalizeSuccess(
      RemoteLeasedJob lease,
      String resultId,
      String statsObjectPrefix,
      String durableCaptureManifestPrefix,
      String reusableArtifactIndexObjectPrefix,
      String statsGenerationManifestUri,
      String indexGenerationCaptureManifestPrefix,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<StatsObjectDescriptor> fileStats,
      List<TargetStatsRecord> finalStats,
      List<StatsObjectDescriptor> indexArtifacts,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference>
          reusableArtifactBundles,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor finalizeIndexPredecessor) {
    return prepareSnapshotFinalizeSuccess(
        blobStore,
        lease.lease(),
        resultId,
        statsObjectPrefix,
        durableCaptureManifestPrefix,
        reusableArtifactIndexObjectPrefix,
        statsGenerationManifestUri,
        indexGenerationCaptureManifestPrefix,
        sourceFileCount,
        fileGroups,
        fileStats,
        finalStats,
        indexArtifacts,
        reusableArtifactBundles,
        realizedStatsSelectors,
        realizedIndexSelectors,
        finalizeIndexPredecessor,
        null);
  }

  @Override
  public PreparedSnapshotFinalizeSuccess prepareAppendOnlySnapshotFinalizeSuccess(
      RemoteLeasedJob lease,
      String resultId,
      String statsObjectPrefix,
      String durableCaptureManifestPrefix,
      String reusableArtifactIndexObjectPrefix,
      String statsGenerationManifestUri,
      String indexGenerationCaptureManifestPrefix,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<StatsObjectDescriptor> fileStats,
      List<TargetStatsRecord> finalStats,
      List<StatsObjectDescriptor> indexArtifacts,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference>
          reusableArtifactBundles,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor finalizeIndexPredecessor,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase) {
    return prepareSnapshotFinalizeSuccess(
        blobStore,
        lease.lease(),
        resultId,
        statsObjectPrefix,
        durableCaptureManifestPrefix,
        reusableArtifactIndexObjectPrefix,
        statsGenerationManifestUri,
        indexGenerationCaptureManifestPrefix,
        sourceFileCount,
        fileGroups,
        fileStats,
        finalStats,
        indexArtifacts,
        reusableArtifactBundles,
        realizedStatsSelectors,
        realizedIndexSelectors,
        finalizeIndexPredecessor,
        appendOnlyBase);
  }

  static PreparedSnapshotFinalizeSuccess prepareSnapshotFinalizeSuccess(
      BlobStore blobStore,
      ReconcileJobStore.LeasedJob leasedJob,
      String resultId,
      String statsObjectPrefix,
      String durableCaptureManifestPrefix,
      String reusableArtifactIndexObjectPrefix,
      String statsGenerationManifestUri,
      String indexGenerationCaptureManifestPrefix,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<StatsObjectDescriptor> fileStats,
      List<TargetStatsRecord> finalStats,
      List<StatsObjectDescriptor> indexArtifacts,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference>
          reusableArtifactBundles,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor finalizeIndexPredecessor,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase) {
    String stableResultId = resultId == null ? "" : resultId.trim();
    String stableStatsObjectPrefix = statsObjectPrefix == null ? "" : statsObjectPrefix.trim();
    String stableDurableManifestPrefix =
        durableCaptureManifestPrefix == null ? "" : durableCaptureManifestPrefix.trim();
    String stableReusableIndexPrefix =
        reusableArtifactIndexObjectPrefix == null ? "" : reusableArtifactIndexObjectPrefix.trim();
    String stableStatsGenerationManifestUri =
        statsGenerationManifestUri == null ? "" : statsGenerationManifestUri.trim();
    String stableIndexGenerationManifestPrefix =
        indexGenerationCaptureManifestPrefix == null
            ? ""
            : indexGenerationCaptureManifestPrefix.trim();
    if (stableResultId.isBlank()) {
      throw new IllegalArgumentException("resultId is required");
    }
    if (stableStatsObjectPrefix.isBlank()) {
      throw new IllegalArgumentException("statsObjectPrefix is required");
    }
    if (stableDurableManifestPrefix.isBlank()) {
      throw new IllegalArgumentException("durableCaptureManifestPrefix is required");
    }
    if (stableReusableIndexPrefix.isBlank()) {
      throw new IllegalArgumentException("reusableArtifactIndexObjectPrefix is required");
    }
    if (stableStatsGenerationManifestUri.isBlank()) {
      throw new IllegalArgumentException("statsGenerationManifestUri is required");
    }
    if (stableIndexGenerationManifestPrefix.isBlank()) {
      throw new IllegalArgumentException("indexGenerationCaptureManifestPrefix is required");
    }
    ReconcileSnapshotTask snapshotTask = leasedJob.snapshotTask;
    List<TargetStatsRecord> records =
        finalStats == null
            ? List.of()
            : finalStats.stream().filter(java.util.Objects::nonNull).toList();
    List<StatsObjectDescriptor> stableFileStats =
        fileStats == null
            ? List.of()
            : fileStats.stream().filter(java.util.Objects::nonNull).toList();
    List<ReconcileFileGroupResultDescriptor> stableFileGroups =
        fileGroups == null
            ? List.of()
            : fileGroups.stream().filter(java.util.Objects::nonNull).toList();
    List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> stableReuseBundles =
        reusableArtifactBundles == null
            ? List.of()
            : reusableArtifactBundles.stream().filter(java.util.Objects::nonNull).toList();
    List<String> stableRealizedIndexSelectors =
        realizedIndexSelectors == null
            ? List.of()
            : realizedIndexSelectors.stream()
                .filter(selector -> selector != null && !selector.isBlank())
                .map(String::trim)
                .distinct()
                .sorted()
                .toList();
    List<String> stableRealizedStatsSelectors =
        realizedStatsSelectors == null
            ? List.of()
            : realizedStatsSelectors.stream()
                .filter(selector -> selector != null && !selector.isBlank())
                .map(String::trim)
                .distinct()
                .sorted()
                .toList();
    int inheritedFileCount = appendOnlyBase == null ? 0 : appendOnlyBase.sourceFileCount();
    if (stableFileGroups.size() != snapshotTask.fileGroupCount()
        || stableFileGroups.stream()
                    .mapToInt(ReconcileFileGroupResultDescriptor::succeededFileCount)
                    .sum()
                + inheritedFileCount
            != sourceFileCount) {
      throw new IllegalArgumentException(
          "file-group descriptors do not cover the planned snapshot files");
    }
    ReconcileCapturePolicy capturePolicy =
        leasedJob.scope == null ? ReconcileCapturePolicy.empty() : leasedJob.scope.capturePolicy();
    ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor indexPredecessor =
        consistentIndexPredecessor(
            stableFileGroups, finalizeIndexPredecessor, capturePolicy.requestsIndexes());
    List<StatsObjectDescriptor> finalStatsObjects =
        records.stream()
            .map(record -> publishStatsObject(blobStore, stableStatsObjectPrefix, record))
            .toList();
    int currentFileStats =
        Math.toIntExact(
            stableReuseBundles.stream()
                .flatMap(bundle -> bundle.getFileStatsList().stream())
                .map(ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata::getFilePath)
                .distinct()
                .count());
    int currentIndexArtifacts =
        Math.toIntExact(
            stableReuseBundles.stream()
                .flatMap(bundle -> bundle.getIndexArtifactsList().stream())
                .map(ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata::getFilePath)
                .distinct()
                .count());
    int totalFileStats =
        Math.addExact(
            appendOnlyBase == null ? 0 : appendOnlyBase.fileStatsRecordCount(), currentFileStats);
    int totalIndexArtifacts =
        Math.addExact(
            appendOnlyBase == null ? 0 : appendOnlyBase.indexArtifactCount(),
            currentIndexArtifacts);
    SnapshotCaptureManifest.Builder manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(leasedJob.accountId)
            .setConnectorId(leasedJob.connectorId)
            .setParentJobId(leasedJob.parentJobId)
            .setFinalizeJobId(leasedJob.jobId)
            .setTableId(snapshotTask.tableId())
            .setSnapshotId(snapshotTask.snapshotId())
            .setLeaseEpoch(leasedJob.leaseEpoch)
            .setResultId(stableResultId)
            .setCapturePolicy(toProtoCapturePolicy(capturePolicy))
            .addAllFinalStats(finalStatsObjects)
            .setSourceFileCount(sourceFileCount)
            .setFileStatsRecordCount(totalFileStats)
            .setPartialAggregateRecordCount(
                stableFileGroups.stream()
                    .mapToInt(ReconcileFileGroupResultDescriptor::partialAggregateRecordCount)
                    .sum())
            .setFinalStatsRecordCount(records.size())
            .setIndexArtifactCount(totalIndexArtifacts)
            .addAllReusableArtifactBundles(stableReuseBundles)
            .setReusableArtifactBundlesComplete(true)
            .addAllRealizedIndexSelectors(stableRealizedIndexSelectors)
            .addAllRealizedStatsSelectors(stableRealizedStatsSelectors);
    if (indexPredecessor != null) {
      manifest.setIndexPredecessor(toProtoIndexPredecessor(indexPredecessor));
    }
    if (appendOnlyBase != null) {
      manifest.setAppendOnlyBase(
          ai.floedb.floecat.reconciler.rpc.AppendOnlySnapshotBase.newBuilder()
              .setFormatVersion(1)
              .setSnapshotId(appendOnlyBase.snapshotId())
              .setManifestUri(appendOnlyBase.manifestUri())
              .setManifestBytes(appendOnlyBase.manifestBytes())
              .setManifestSha256(
                  com.google.protobuf.ByteString.copyFrom(appendOnlyBase.manifestSha256Bytes()))
              .setSourceFileCount(appendOnlyBase.sourceFileCount())
              .setFileStatsRecordCount(appendOnlyBase.fileStatsRecordCount())
              .setIndexArtifactCount(appendOnlyBase.indexArtifactCount())
              .setChainDepth(appendOnlyBase.chainDepth())
              .setStatsGenerationId(appendOnlyBase.statsGenerationId())
              .setIndexGenerationId(appendOnlyBase.indexGenerationId())
              .setReusableArtifactIndex(appendOnlyBase.reusableArtifactIndex()));
    }
    ReusableArtifactIndexReference baseIndex =
        appendOnlyBase == null
            ? ReusableArtifactIndexStore.emptyReference()
            : appendOnlyBase.reusableArtifactIndex();
    ReusableArtifactIndexReference reusableArtifactIndex =
        new ReusableArtifactIndexStore(blobStore)
            .append(stableReusableIndexPrefix, baseIndex, stableReuseBundles);
    if (reusableArtifactIndex.getFileStatsRecordCount() != totalFileStats
        || reusableArtifactIndex.getIndexArtifactCount() != totalIndexArtifacts) {
      throw new IllegalArgumentException(
          "reusable artifact index counts do not match the capture manifest");
    }
    manifest.setReusableArtifactIndex(reusableArtifactIndex);
    Set<String> indexedStatsTargets = new HashSet<>();
    for (ReconcileFileGroupResultDescriptor fileGroup : stableFileGroups) {
      manifest.addFileGroups(toProtoFileGroupResultDescriptor(fileGroup));
    }
    int declaredFileStats =
        stableFileGroups.stream()
            .mapToInt(ReconcileFileGroupResultDescriptor::fileStatsRecordCount)
            .sum();
    if (declaredFileStats < stableFileStats.size()) {
      throw new IllegalArgumentException("unique file stats exceed file-group descriptor counts");
    }
    for (StatsObjectDescriptor statsObject : stableFileStats) {
      if (statsObject == null) {
        throw new IllegalArgumentException("invalid file stats object metadata");
      }
      String targetStorageId = statsObject.getTargetStorageId();
      if (targetStorageId.isBlank() || !indexedStatsTargets.add(targetStorageId)) {
        throw new IllegalArgumentException(
            "duplicate target in snapshot capture manifest: " + targetStorageId);
      }
    }
    for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
      String targetStorageId =
          ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
              records.get(recordIndex).getTarget());
      if (!indexedStatsTargets.add(targetStorageId)) {
        throw new IllegalArgumentException(
            "duplicate target in snapshot capture manifest: " + targetStorageId);
      }
    }
    byte[] manifestBytes = manifest.build().toByteArray();
    byte[] manifestSha256 = sha256(manifestBytes);
    String stableManifestUri =
        stableDurableManifestPrefix + HexFormat.of().formatHex(manifestSha256) + ".pb";
    LOG.infof(
        "Persisting snapshot capture manifest uri=%s bytes=%d fileGroups=%d reuseBundles=%d",
        stableManifestUri,
        manifestBytes.length,
        stableFileGroups.size(),
        stableReuseBundles.size());
    blobStore.putImmutable(stableManifestUri, manifestBytes, "application/x-protobuf");
    if (capturePolicy.requestsIndexes()) {
      blobStore.putImmutable(
          stableIndexGenerationManifestPrefix + HexFormat.of().formatHex(manifestSha256) + ".pb",
          manifestBytes,
          "application/x-protobuf");
    }
    blobStore.putImmutable(
        stableStatsGenerationManifestUri,
        StringValue.of("full-rescan-" + leasedJob.parentJobId).toByteArray(),
        "application/x-protobuf");
    LOG.infof(
        "Persisted snapshot capture manifest uri=%s bytes=%d",
        stableManifestUri, manifestBytes.length);
    SnapshotCaptureManifestDescriptor manifestDescriptor =
        SnapshotCaptureManifestDescriptor.newBuilder()
            .setFormatVersion(1)
            .setAccountId(leasedJob.accountId)
            .setConnectorId(leasedJob.connectorId)
            .setParentJobId(leasedJob.parentJobId)
            .setFinalizeJobId(leasedJob.jobId)
            .setTableId(snapshotTask.tableId())
            .setSnapshotId(snapshotTask.snapshotId())
            .setLeaseEpoch(leasedJob.leaseEpoch)
            .setResultId(stableResultId)
            .setManifestUri(stableManifestUri)
            .setManifestBytes(manifestBytes.length)
            .setManifestSha256(ByteString.copyFrom(manifestSha256))
            .setFileGroupCount(stableFileGroups.size())
            .setSourceFileCount(sourceFileCount)
            .setStatsRecordCount(totalFileStats + records.size())
            .setIndexArtifactCount(totalIndexArtifacts)
            .build();
    return new PreparedSnapshotFinalizeSuccess(stableResultId, manifestDescriptor);
  }

  @Override
  public boolean submitSnapshotFinalizeSuccess(
      RemoteLeasedJob lease, PreparedSnapshotFinalizeSuccess prepared) {
    try {
      return invokeWorkerControl(
          "submitLeasedSnapshotFinalizeResult",
          correlationId(lease),
          lease.lease().accountId,
          true,
          stub ->
              stub.submitLeasedSnapshotFinalizeResult(
                      SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
                          .setJobId(lease.lease().jobId)
                          .setLeaseEpoch(lease.lease().leaseEpoch)
                          .setSuccess(
                              SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
                                  .setResultId(prepared.resultId())
                                  .setManifestDescriptor(prepared.manifestDescriptor())
                                  .build())
                          .build())
                  .getAccepted());
    } catch (RuntimeException error) {
      RuntimeException classified =
          leasePreconditionOrOriginal("submitLeasedSnapshotFinalizeResult", error);
      if (isTransportFailure(classified)) {
        throw new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
            "snapshot finalizer result submission outcome is unknown",
            classified);
      }
      throw classified;
    }
  }

  private static FileGroupResultDescriptor toProtoFileGroupResultDescriptor(
      ReconcileFileGroupResultDescriptor descriptor) {
    FileGroupResultDescriptor.Builder out =
        FileGroupResultDescriptor.newBuilder()
            .setFormatVersion(descriptor.formatVersion())
            .setAccountId(descriptor.accountId())
            .setConnectorId(descriptor.connectorId())
            .setParentJobId(descriptor.parentJobId())
            .setFileGroupJobId(descriptor.fileGroupJobId())
            .setPlanId(descriptor.planId())
            .setGroupId(descriptor.groupId())
            .setTableId(descriptor.tableId())
            .setSnapshotId(descriptor.snapshotId())
            .setLeaseEpoch(descriptor.leaseEpoch())
            .setResultId(descriptor.resultId())
            .setPayloadUri(descriptor.payloadUri())
            .setPayloadBytes(descriptor.payloadBytes())
            .setPayloadSha256(
                ByteString.copyFrom(Base64.getDecoder().decode(descriptor.payloadSha256())))
            .setPlannedFileCount(descriptor.plannedFileCount())
            .setSucceededFileCount(descriptor.succeededFileCount())
            .setFailedFileCount(descriptor.failedFileCount())
            .setSkippedFileCount(descriptor.skippedFileCount())
            .setPartialAggregateRecordCount(descriptor.partialAggregateRecordCount())
            .setIndexArtifactCount(descriptor.indexArtifactCount())
            .setStatsObjectPrefix(descriptor.statsObjectPrefix())
            .setFileStatsRecordCount(descriptor.fileStatsRecordCount())
            .setArtifactReferencesSha256(
                ByteString.copyFrom(
                    HexFormat.of().parseHex(descriptor.artifactReferencesSha256())));
    if (descriptor.indexPredecessor() != null) {
      out.setIndexPredecessor(toProtoIndexPredecessor(descriptor.indexPredecessor()));
    }
    if (descriptor.createdAtMs() > 0L) {
      out.setCreatedAt(Timestamps.fromMillis(descriptor.createdAtMs()));
    }
    return out.build();
  }

  private static ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor
      consistentIndexPredecessor(
          List<ReconcileFileGroupResultDescriptor> fileGroups,
          ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor finalizePredecessor,
          boolean required) {
    ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor = null;
    for (ReconcileFileGroupResultDescriptor fileGroup : fileGroups) {
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor candidate =
          fileGroup.indexPredecessor();
      if (candidate == null) {
        if (required) {
          throw new IllegalArgumentException(
              "index file-group descriptor is missing its predecessor");
        }
        continue;
      }
      if (predecessor == null) {
        predecessor = candidate;
      } else if (!predecessor.equals(candidate)) {
        throw new IllegalArgumentException(
            "index file-group descriptors have inconsistent predecessors");
      }
    }
    if (predecessor != null
        && finalizePredecessor != null
        && !predecessor.equals(finalizePredecessor)) {
      throw new IllegalArgumentException(
          "snapshot finalizer predecessor does not match file-group predecessors");
    }
    if (predecessor == null) {
      predecessor = finalizePredecessor;
    }
    if (required && predecessor == null) {
      throw new IllegalArgumentException("index capture manifest is missing its predecessor");
    }
    return required ? predecessor : null;
  }

  private static ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor
      toProtoIndexPredecessor(
          StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor predecessor) {
    return ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor.newBuilder()
        .setGenerationId(predecessor.generationId())
        .setActivePointerVersion(predecessor.activePointerVersion())
        .setCaptureManifestUri(predecessor.captureManifestUri())
        .setCaptureManifestPointerVersion(predecessor.captureManifestPointerVersion())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor
      toProtoIndexPredecessor(
          ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor) {
    return ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor.newBuilder()
        .setGenerationId(predecessor.generationId())
        .setActivePointerVersion(predecessor.activePointerVersion())
        .setCaptureManifestUri(predecessor.captureManifestUri())
        .setCaptureManifestPointerVersion(predecessor.captureManifestPointerVersion())
        .build();
  }

  private static ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor
      fromProtoIndexPredecessor(
          ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor predecessor) {
    return new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
        predecessor.getGenerationId(),
        predecessor.getActivePointerVersion(),
        predecessor.getCaptureManifestUri(),
        predecessor.getCaptureManifestPointerVersion());
  }

  private List<IndexArtifactRecord> commitIndexArtifacts(
      StandaloneFileGroupExecutionPayload payload, StandaloneFileGroupExecutionResult result) {
    Set<String> reusedIndexFiles =
        payload.fileExecutionPlans().stream()
            .filter(ReconcileFileExecutionPlan::reusesIndexArtifact)
            .map(ReconcileFileExecutionPlan::filePath)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    List<IndexArtifactRecord> out = new ArrayList<>();
    for (var artifact : result.stagedIndexArtifacts()) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      String uri = artifact.record().getArtifactUri();
      if (uri == null || uri.isBlank()) {
        throw new IllegalArgumentException("index artifact_uri is required for direct publication");
      }
      byte[] content = artifact.content();
      String filePath = artifact.record().getTarget().getFile().getFilePath();
      if (reusedIndexFiles.contains(filePath)) {
        if (content != null && content.length > 0) {
          throw new IllegalArgumentException("reused index artifact must not contain staged bytes");
        }
        out.add(artifact.record());
        continue;
      }
      if (content != null && content.length > 0) {
        blobStore.put(
            uri,
            content,
            artifact.contentType() == null || artifact.contentType().isBlank()
                ? "application/x-parquet"
                : artifact.contentType());
      }
      var header =
          blobStore
              .head(uri)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "index artifact object is not committed: " + uri));
      if (header.getContentLength() <= 0L) {
        throw new IllegalArgumentException("index artifact object is empty: " + uri);
      }
      var record = artifact.record().toBuilder().setContentEtag(header.getEtag()).build();
      out.add(record);
    }
    return List.copyOf(out);
  }

  static void validateIndexArtifactCoverage(
      StandaloneFileGroupExecutionPayload payload, StandaloneFileGroupExecutionResult result) {
    ReconcileCapturePolicy policy =
        payload.capturePolicy() == null ? ReconcileCapturePolicy.empty() : payload.capturePolicy();
    if (!policy.requestsIndexes()) {
      return;
    }
    Set<String> planned = new HashSet<>(payload.plannedFilePaths());
    Set<String> captured = new HashSet<>();
    Set<String> requiredSelectors = policy.selectorsForIndex();
    boolean defaultSelection =
        requiredSelectors.isEmpty()
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    Set<String> resolvedDefaultSelectors = null;
    for (var artifact : result.stagedIndexArtifacts()) {
      var record = artifact == null ? null : artifact.record();
      if (record == null
          || !record.hasTarget()
          || !record.getTarget().hasFile()
          || record.getState() != IndexArtifactState.IAS_READY) {
        throw new IllegalArgumentException(
            "index capture requires one ready artifact per planned file");
      }
      String filePath = record.getTarget().getFile().getFilePath();
      if (!planned.contains(filePath) || !captured.add(filePath)) {
        throw new IllegalArgumentException(
            "index artifact targets do not match the planned file group");
      }
      Set<String> persistedSelectors = persistedIndexSelectors(record);
      if (!requiredSelectors.isEmpty()
          && !FileArtifactReuse.coversExplicitSelectors(persistedSelectors, requiredSelectors)) {
        throw new IllegalArgumentException(
            "index artifact does not cover the explicitly requested selectors");
      }
      if (defaultSelection && persistedSelectors.isEmpty()) {
        throw new IllegalArgumentException(
            "index artifact does not report its resolved default column coverage");
      }
      if (defaultSelection) {
        Set<String> persistedSelectorIdentities =
            FileArtifactReuse.selectorIdentities(persistedSelectors);
        if (resolvedDefaultSelectors == null) {
          resolvedDefaultSelectors = persistedSelectorIdentities;
        } else if (!resolvedDefaultSelectors.equals(persistedSelectorIdentities)) {
          throw new IllegalArgumentException(
              "index artifacts report inconsistent resolved default column coverage");
        }
      }
      if (defaultSelection
          && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
          && realizedIndexColumnCount(persistedSelectors) > policy.maxDefaultColumns()) {
        throw new IllegalArgumentException(
            "index artifact exceeds the requested default column limit");
      }
    }
    if (!captured.equals(planned)) {
      throw new IllegalArgumentException(
          "index capture requires one ready artifact per planned file");
    }
  }

  private static int realizedIndexColumnCount(Set<String> selectors) {
    return FileArtifactReuse.realizedColumnCount(selectors);
  }

  private static Set<String> persistedIndexSelectors(
      ai.floedb.floecat.catalog.rpc.IndexArtifactRecord record) {
    return Set.copyOf(
        FileArtifactReuse.decodeSelectors(
            record.getPropertiesOrDefault(FileArtifactReuse.INDEXED_COLUMNS_PROPERTY, "")));
  }

  private static StatsObjectDescriptor publishStatsObject(
      BlobStore blobStore, String statsObjectPrefix, TargetStatsRecord record) {
    TargetStatsRecord canonical =
        ai.floedb.floecat.stats.identity.TargetStatsRecords.canonicalize(record);
    byte[] bytes = canonical.toByteArray();
    String storageId =
        ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(canonical.getTarget());
    String uri =
        statsObjectPrefix
            + HexFormat.of().formatHex(sha256(storageId.getBytes(StandardCharsets.UTF_8)))
            + "/"
            + HexFormat.of().formatHex(sha256(bytes))
            + ".pb";
    blobStore.put(uri, bytes, "application/x-protobuf");
    return StatsObjectDescriptor.newBuilder()
        .setTargetStorageId(storageId)
        .setPayloadUri(uri)
        .setPayloadBytes(bytes.length)
        .setPayloadSha256(ByteString.copyFrom(sha256(bytes)))
        .build();
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
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
                scope.getCapturePolicy().getMaxDefaultColumns(),
                scope.getCapturePolicy().getPropertiesMap())
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
        .putAllProperties(effective.properties())
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
    ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.Builder builder =
        ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.newBuilder()
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
            .setSourceRevision(effective.sourceRevision())
            .setMetadataFingerprint(effective.metadataFingerprint())
            .addAllRequestedCoverage(effective.requestedCoverage())
            .setCompletionMode(
                switch (effective.completionMode()) {
                  case DIRECT_STATS ->
                      ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.CompletionMode
                          .RSCM_DIRECT_STATS;
                  case FILE_GROUPS ->
                      ai.floedb.floecat.reconciler.rpc.ReconcileSnapshotTask.CompletionMode
                          .RSCM_FILE_GROUPS;
                });
    if (effective.indexPredecessor() != null) {
      builder.setIndexPredecessor(toProtoIndexPredecessor(effective.indexPredecessor()));
    }
    return builder.build();
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
        .setExecutionSchemaJson(effective.executionSchemaJson())
        .addAllFileExecutionPlans(
            effective.fileExecutionPlans().stream()
                .map(GrpcRemoteReconcileExecutorClient::toProtoFileExecutionPlan)
                .toList())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.FileExecutionPlan toProtoFileExecutionPlan(
      ReconcileFileExecutionPlan plan) {
    var builder =
        ai.floedb.floecat.reconciler.rpc.FileExecutionPlan.newBuilder()
            .setFilePath(plan.filePath())
            .setFileSizeInBytes(plan.fileSizeInBytes())
            .setPartitionDataJson(plan.partitionDataJson())
            .setFileFormat(plan.fileFormat())
            .setPartitionSpecId(plan.partitionSpecId())
            .setContentIdentity(plan.contentIdentity())
            .setSourceFingerprint(plan.sourceFingerprint())
            .setIndexSourceFingerprint(plan.indexSourceFingerprint())
            .setStatsCaptureSignature(plan.statsCaptureSignature())
            .setIndexCaptureSignature(plan.indexCaptureSignature())
            .putAllAuxiliaryStatsFingerprints(plan.auxiliaryStatsFingerprints())
            .addAllIcebergDeleteFiles(
                plan.icebergDeleteFiles().stream()
                    .map(GrpcRemoteReconcileExecutorClient::toProtoIcebergDeleteFile)
                    .toList())
            .addAllReusableArtifactBundleSelections(
                plan.reusableArtifactBundleSelections().stream()
                    .map(GrpcRemoteReconcileExecutorClient::toProtoBundleSelection)
                    .toList());
    if (plan.deletionVector() != null) {
      DeltaDeletionVector dv = plan.deletionVector();
      var dvBuilder =
          ai.floedb.floecat.reconciler.rpc.DeltaDeletionVector.newBuilder()
              .setStorageType(dv.storageType())
              .setPathOrInlineDv(dv.pathOrInlineDv())
              .setSizeInBytes(dv.sizeInBytes())
              .setCardinality(dv.cardinality());
      if (dv.offset() != null) {
        dvBuilder.setOffset(dv.offset());
      }
      builder.setDeletionVector(dvBuilder);
    }
    return builder.build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleSelection
      toProtoBundleSelection(ReusableArtifactBundleSelection selection) {
    return ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleSelection.newBuilder()
        .setArtifact(
            StatsObjectDescriptor.newBuilder()
                .setTargetStorageId(selection.targetStorageId())
                .setPayloadUri(selection.payloadUri())
                .setPayloadBytes(selection.payloadBytes())
                .setPayloadSha256(ByteString.copyFrom(selection.payloadSha256())))
        .addAllStatsFilePaths(selection.statsFilePaths())
        .addAllIndexFilePaths(selection.indexFilePaths())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile toProtoIcebergDeleteFile(
      ReconcileFileExecutionPlan.IcebergDeleteFile deleteFile) {
    return ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile.newBuilder()
        .setFilePath(deleteFile.filePath())
        .setFileSizeInBytes(deleteFile.fileSizeInBytes())
        .setContent(
            switch (deleteFile.content()) {
              case POSITION ->
                  ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile.Content.IDFC_POSITION;
              case EQUALITY ->
                  ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile.Content.IDFC_EQUALITY;
              case UNSPECIFIED ->
                  ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile.Content.IDFC_UNSPECIFIED;
            })
        .setPartitionSpecId(deleteFile.partitionSpecId())
        .addAllEqualityFieldIds(deleteFile.equalityFieldIds())
        .setContentIdentity(deleteFile.contentIdentity())
        .build();
  }

  private static ReconcileFileExecutionPlan fromProtoFileExecutionPlan(
      ai.floedb.floecat.reconciler.rpc.FileExecutionPlan plan) {
    DeltaDeletionVector dv =
        plan.hasDeletionVector()
            ? new DeltaDeletionVector(
                plan.getDeletionVector().getStorageType(),
                plan.getDeletionVector().getPathOrInlineDv(),
                plan.getDeletionVector().hasOffset() ? plan.getDeletionVector().getOffset() : null,
                plan.getDeletionVector().getSizeInBytes(),
                plan.getDeletionVector().getCardinality())
            : null;
    return ReconcileFileExecutionPlan.of(
            plan.getFilePath(),
            plan.getFileSizeInBytes(),
            plan.getPartitionDataJson(),
            dv,
            plan.getFileFormat(),
            plan.getPartitionSpecId(),
            plan.getIcebergDeleteFilesList().stream()
                .map(GrpcRemoteReconcileExecutorClient::fromProtoIcebergDeleteFile)
                .toList(),
            plan.getContentIdentity())
        .withReuseBundleSelections(
            plan.getSourceFingerprint(),
            plan.getIndexSourceFingerprint(),
            plan.getStatsCaptureSignature(),
            plan.getIndexCaptureSignature(),
            plan.getAuxiliaryStatsFingerprintsMap(),
            plan.getReusableArtifactBundleSelectionsList().stream()
                .map(GrpcRemoteReconcileExecutorClient::fromProtoBundleSelection)
                .toList());
  }

  private static ReusableArtifactBundleSelection fromProtoBundleSelection(
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleSelection selection) {
    StatsObjectDescriptor artifact = selection.getArtifact();
    return new ReusableArtifactBundleSelection(
        artifact.getTargetStorageId(),
        artifact.getPayloadUri(),
        artifact.getPayloadBytes(),
        artifact.getPayloadSha256().toByteArray(),
        selection.getStatsFilePathsList(),
        selection.getIndexFilePathsList());
  }

  private static ReconcileFileExecutionPlan.IcebergDeleteFile fromProtoIcebergDeleteFile(
      ai.floedb.floecat.reconciler.rpc.IcebergDeleteFile deleteFile) {
    ReconcileFileExecutionPlan.IcebergDeleteContent content =
        switch (deleteFile.getContent()) {
          case IDFC_POSITION -> ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION;
          case IDFC_EQUALITY -> ReconcileFileExecutionPlan.IcebergDeleteContent.EQUALITY;
          default -> ReconcileFileExecutionPlan.IcebergDeleteContent.UNSPECIFIED;
        };
    return new ReconcileFileExecutionPlan.IcebergDeleteFile(
        deleteFile.getFilePath(),
        deleteFile.getFileSizeInBytes(),
        content,
        deleteFile.getPartitionSpecId(),
        deleteFile.getEqualityFieldIdsList(),
        deleteFile.getContentIdentity());
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
        List.of(),
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
        snapshotTask.getDirectStatsRecordCount(),
        snapshotTask.getSourceRevision(),
        snapshotTask.getMetadataFingerprint(),
        snapshotTask.getRequestedCoverageList(),
        snapshotTask.hasIndexPredecessor()
            ? fromProtoIndexPredecessor(snapshotTask.getIndexPredecessor())
            : null);
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
        fileGroupTask.getFilePathsCount(),
        "",
        0,
        fileGroupTask.getFilePathsList(),
        List.of(),
        List.of(),
        fileGroupTask.getExecutionSchemaJson(),
        fileGroupTask.getFileExecutionPlansList().stream()
            .map(GrpcRemoteReconcileExecutorClient::fromProtoFileExecutionPlan)
            .toList());
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

  private <T> T invokePlannerMutationOnce(
      String operation,
      String jobKind,
      String resultPart,
      RemoteLeasedJob lease,
      MessageLite request,
      Function<ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub, T> invocation) {
    long startedNanos = System.nanoTime();
    try {
      T result =
          invokeWorkerControlMutationOnce(
              operation, correlationId(lease), lease.lease().accountId, invocation);
      logPlannerSubmission(lease, jobKind, resultPart, request, startedNanos, "succeeded");
      return result;
    } catch (RuntimeException error) {
      logPlannerSubmission(lease, jobKind, resultPart, request, startedNanos, "failed");
      throw error;
    }
  }

  private static void logPlannerSubmission(
      RemoteLeasedJob lease,
      String jobKind,
      String resultPart,
      MessageLite request,
      long startedNanos,
      String outcome) {
    LOG.infof(
        "planner grpc submission timing jobId=%s jobKind=%s resultPart=%s"
            + " rpcRequestBytes=%d rpcMs=%.3f outcome=%s",
        lease.lease().jobId,
        jobKind,
        resultPart,
        request.getSerializedSize(),
        (System.nanoTime() - startedNanos) / 1_000_000.0,
        outcome);
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
      case "commitLeasedFileGroupResult",
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
