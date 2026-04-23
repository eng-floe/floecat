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
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationResponse;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.LeasedReconcileJob;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControl;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseResponse;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressResponse;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.StartLeasedReconcileJobResponse;
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

  @Override
  public Uni<LeaseReconcileJobResponse> leaseReconcileJob(LeaseReconcileJobRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");
              String corr = principalContext.getCorrelationId();
              String executorId = mustNonEmpty(request.getExecutorId(), "executor_id", corr);
              var leaseRequest =
                  ReconcileJobStore.LeaseRequest.of(
                      executionClassesFrom(request),
                      lanesFrom(request),
                      Set.of(executorId),
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
                case RCS_FAILED ->
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
                case RCS_CANCELLED ->
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
                case RCS_UNSPECIFIED, UNRECOGNIZED ->
                    throw GrpcErrors.invalidArgument(
                        corr, null, java.util.Map.of("field", "state"));
              }
              return CompleteLeasedReconcileJobResponse.newBuilder().setAccepted(true).build();
            }),
        correlationId());
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
        case RJK_EXEC_TABLE -> jobKinds.add(ReconcileJobKind.EXEC_TABLE);
        case RJK_EXEC_VIEW -> jobKinds.add(ReconcileJobKind.EXEC_VIEW);
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
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileJobKind toProtoJobKind(
      ReconcileJobKind jobKind) {
    return switch (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind) {
      case PLAN_CONNECTOR -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_CONNECTOR;
      case EXEC_TABLE -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_TABLE;
      case EXEC_VIEW -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_VIEW;
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

  private static ai.floedb.floecat.reconciler.rpc.CaptureMode toProtoCaptureMode(
      CaptureMode captureMode) {
    return switch (captureMode == null ? CaptureMode.METADATA_AND_STATS : captureMode) {
      case METADATA_ONLY -> ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_ONLY;
      case STATS_ONLY -> ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_STATS_ONLY;
      case METADATA_AND_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_METADATA_AND_STATS;
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
    var builder =
        ai.floedb.floecat.reconciler.rpc.CaptureScope.newBuilder()
            .setConnectorId(
                ResourceId.newBuilder()
                    .setAccountId(lease.accountId)
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .setId(lease.connectorId)
                    .build())
            .setDestinationTableId(
                scope.destinationTableId() == null ? "" : scope.destinationTableId())
            .setDestinationViewId(
                scope.destinationViewId() == null ? "" : scope.destinationViewId());
    for (String namespaceId : scope.destinationNamespaceIds()) {
      builder.addDestinationNamespaceIds(namespaceId);
    }
    for (ReconcileScope.ScopedStatsRequest request : scope.destinationStatsRequests()) {
      builder.addDestinationStatsRequests(
          ai.floedb.floecat.reconciler.rpc.ScopedStatsRequest.newBuilder()
              .setTableId(request.tableId())
              .setSnapshotId(request.snapshotId())
              .setTargetSpec(request.targetSpec())
              .addAllColumnSelectors(request.columnSelectors())
              .build());
    }
    return builder.build();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
