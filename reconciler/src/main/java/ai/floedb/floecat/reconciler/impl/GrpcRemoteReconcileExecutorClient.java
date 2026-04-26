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
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
class GrpcRemoteReconcileExecutorClient implements RemoteReconcileExecutorClient {
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
  public Optional<RemoteLeasedJob> lease(ReconcileExecutor executor) {
    var response =
        withHeaders(executorControl, "reconcile-lease-" + executor.id())
            .leaseReconcileJob(
                LeaseReconcileJobRequest.newBuilder()
                    .setExecutorId(executor.id())
                    .addAllExecutionClasses(
                        executor.supportedExecutionClasses().stream()
                            .map(GrpcRemoteReconcileExecutorClient::toProtoExecutionClass)
                            .toList())
                    .addAllLanes(executor.supportedLanes())
                    .addAllJobKinds(
                        executor.supportedJobKinds().stream()
                            .map(GrpcRemoteReconcileExecutorClient::toProtoJobKind)
                            .toList())
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
    if (lease.jobKind == ReconcileJobKind.PLAN_SNAPSHOT) {
      LOG.infof(
          "fromProtoLease PLAN_SNAPSHOT jobId=%s connectorId=%s protoTableId=%s protoSnapshotId=%d mappedTableId=%s mappedSnapshotId=%d source=%s.%s fileGroups=%d",
          lease.jobId,
          lease.connectorId,
          job.getSnapshotTask().getTableId(),
          job.getSnapshotTask().getSnapshotId(),
          lease.snapshotTask == null ? "" : lease.snapshotTask.tableId(),
          lease.snapshotTask == null ? 0L : lease.snapshotTask.snapshotId(),
          lease.snapshotTask == null ? "" : lease.snapshotTask.sourceNamespace(),
          lease.snapshotTask == null ? "" : lease.snapshotTask.sourceTable(),
          lease.snapshotTask == null ? 0 : lease.snapshotTask.fileGroups().size());
    }
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
        scope.getDestinationTableId(),
        scope.getDestinationViewId(),
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

  private static ReconcileCompletionState toProtoCompletionState(
      RemoteLeasedJob.CompletionState state) {
    return switch (state) {
      case SUCCEEDED -> ReconcileCompletionState.RCS_SUCCEEDED;
      case FAILED -> ReconcileCompletionState.RCS_FAILED;
      case CANCELLED -> ReconcileCompletionState.RCS_CANCELLED;
    };
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileJobKind toProtoJobKind(
      ReconcileJobKind jobKind) {
    return switch (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind) {
      case PLAN_CONNECTOR -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_CONNECTOR;
      case PLAN_TABLE -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_TABLE;
      case PLAN_VIEW -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_VIEW;
      case PLAN_SNAPSHOT -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_SNAPSHOT;
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
            .toList());
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

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
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
