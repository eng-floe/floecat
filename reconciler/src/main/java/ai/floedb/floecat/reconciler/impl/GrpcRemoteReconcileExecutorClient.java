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
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
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

@ApplicationScoped
class GrpcRemoteReconcileExecutorClient implements RemoteReconcileExecutorClient {
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
      long scanned,
      long changed,
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
                    .setTablesScanned(scanned)
                    .setTablesChanged(changed)
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
      long scanned,
      long changed,
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
                    .setTablesScanned(scanned)
                    .setTablesChanged(changed)
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
    return new ReconcileJobStore.LeasedJob(
        job.getJobId(),
        connectorId.getAccountId(),
        connectorId.getId(),
        job.getFullRescan(),
        fromProtoCaptureMode(job.getMode()),
        fromProtoScope(job.getScope()),
        fromProtoExecutionPolicy(job.getExecutionPolicy()),
        job.getLeaseEpoch(),
        job.getPinnedExecutorId(),
        job.getExecutorId());
  }

  private static CaptureMode fromProtoCaptureMode(
      ai.floedb.floecat.reconciler.rpc.CaptureMode captureMode) {
    return switch (captureMode) {
      case CM_METADATA_ONLY -> CaptureMode.METADATA_ONLY;
      case CM_STATS_ONLY -> CaptureMode.STATS_ONLY;
      case CM_METADATA_AND_STATS, CM_UNSPECIFIED, UNRECOGNIZED -> CaptureMode.METADATA_AND_STATS;
    };
  }

  private static ReconcileScope fromProtoScope(
      ai.floedb.floecat.reconciler.rpc.CaptureScope scope) {
    if (scope == null) {
      return ReconcileScope.empty();
    }
    return ReconcileScope.of(
        scope.getDestinationNamespacePathsList().stream()
            .map(NamespacePath::getSegmentsList)
            .map(java.util.List::copyOf)
            .toList(),
        scope.getDestinationTableDisplayName(),
        scope.getDestinationTableColumnsList());
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
