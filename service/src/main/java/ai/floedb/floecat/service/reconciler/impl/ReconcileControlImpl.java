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

import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.ReconcileMode;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowResponse;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsResponse;
import ai.floedb.floecat.reconciler.rpc.JobState;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsRequest;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileControl;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class ReconcileControlImpl extends BaseServiceImpl implements ReconcileControl {

  @Inject ConnectorRepository connectorRepo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;
  @Inject ReconcileCancellationRegistry cancellations;
  @Inject ReconcilerSettingsStore settings;
  @Inject Observability observability;

  private static final Logger LOG = Logger.getLogger(ReconcileControl.class);

  @Override
  public Uni<CaptureNowResponse> captureNow(CaptureNowRequest request) {
    var L = LogHelper.start(LOG, "CaptureNow");
    String trigger = captureNowTrigger(request);

    return mapFailures(
            run(
                () -> {
                  try {
                    var pc = principalProvider.get();
                    authz.require(pc, "connector.manage");
                    var corr = pc.getCorrelationId();

                    var connectorId =
                        scopedConnectorId(
                            pc.getAccountId(), connectorIdFromScope(request.getScope()), corr);
                    validateSnapshotScope(request.getScope(), corr);
                    connectorRepo
                        .getById(connectorId)
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    corr,
                                    GeneratedErrorMessages.MessageKey.CONNECTOR,
                                    Map.of("id", connectorId.getId())));

                    var result =
                        reconcilerService.reconcile(
                            pc,
                            connectorId,
                            request.getFullRescan(),
                            scopeFromCaptureScope(request.getScope()),
                            mapCaptureMode(request.getMode()));
                    if (!result.ok()) {
                      if (result.error != null) {
                        throw new RuntimeException("sync capture failed", result.error);
                      }
                      throw new IllegalStateException("sync capture failed");
                    }
                    observeReconcileCounter(
                        ServiceMetrics.Reconcile.CAPTURE_NOW,
                        "capture_now",
                        "success",
                        trigger,
                        null);
                    return CaptureNowResponse.newBuilder()
                        .setTablesScanned(result.scanned)
                        .setTablesChanged(result.changed)
                        .setErrors(result.errors)
                        .build();
                  } catch (RuntimeException e) {
                    observeReconcileCounter(
                        ServiceMetrics.Reconcile.CAPTURE_NOW,
                        "capture_now",
                        "error",
                        trigger,
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<StartCaptureResponse> startCapture(StartCaptureRequest request) {
    var L = LogHelper.start(LOG, "StartCapture");
    String trigger = startCaptureTrigger(request);

    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    var correlationId = principalContext.getCorrelationId();

                    authz.require(
                        principalContext, List.of("connector.manage", "connector.create"));

                    var connectorId =
                        scopedConnectorId(
                            principalContext.getAccountId(),
                            connectorIdFromScope(request.getScope()),
                            correlationId);
                    validateSnapshotScope(request.getScope(), correlationId);
                    connectorRepo
                        .getById(connectorId)
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId,
                                    GeneratedErrorMessages.MessageKey.CONNECTOR,
                                    Map.of("id", connectorId.getId())));

                    var jobId =
                        jobs.enqueuePlan(
                            connectorId.getAccountId(),
                            connectorId.getId(),
                            request.getFullRescan(),
                            mapCaptureMode(request.getMode()),
                            scopeFromRequest(request),
                            mapExecutionPolicy(request.getExecutionPolicy()),
                            "");
                    observeReconcileCounter(
                        ServiceMetrics.Reconcile.START_CAPTURE,
                        "start_capture",
                        "success",
                        trigger,
                        null);
                    return StartCaptureResponse.newBuilder().setJobId(jobId).build();
                  } catch (RuntimeException e) {
                    observeReconcileCounter(
                        ServiceMetrics.Reconcile.START_CAPTURE,
                        "start_capture",
                        "error",
                        trigger,
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest request) {
    var L = LogHelper.start(LOG, "GetReconcileJob");

    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    var correlationId = principalContext.getCorrelationId();

                    authz.require(principalContext, "connector.manage");

                    var job =
                        jobs.get(principalContext.getAccountId(), request.getJobId())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId,
                                        GeneratedErrorMessages.MessageKey.JOB,
                                        Map.of("id", request.getJobId())));
                    job = aggregateIfPlanJob(principalContext.getAccountId(), job);

                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.GET_JOB, "get_reconcile_job", "success", null);
                    return toResponse(job);
                  } catch (RuntimeException e) {
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.GET_JOB,
                        "get_reconcile_job",
                        "error",
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ListReconcileJobsResponse> listReconcileJobs(ListReconcileJobsRequest request) {
    var L = LogHelper.start(LOG, "ListReconcileJobs");
    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    authz.require(principalContext, "connector.manage");
                    int pageSize =
                        request.hasPage() && request.getPage().getPageSize() > 0
                            ? request.getPage().getPageSize()
                            : 100;
                    String pageToken = request.hasPage() ? request.getPage().getPageToken() : "";
                    Set<String> states = new HashSet<>();
                    for (JobState state : request.getStatesList()) {
                      if (state != JobState.JS_UNSPECIFIED) {
                        states.add(fromProtoState(state));
                      }
                    }
                    var page =
                        jobs.list(
                            principalContext.getAccountId(),
                            pageSize,
                            pageToken,
                            request.getConnectorId(),
                            states);

                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.LIST_JOBS, "list_reconcile_jobs", "success", null);
                    var builder = ListReconcileJobsResponse.newBuilder();
                    for (var job : page.jobs) {
                      builder.addJobs(
                          toResponse(aggregateIfPlanJob(principalContext.getAccountId(), job)));
                    }
                    builder.setPage(
                        PageResponse.newBuilder()
                            .setNextPageToken(page.nextPageToken)
                            .setTotalSize(0)
                            .build());
                    return builder.build();
                  } catch (RuntimeException e) {
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.LIST_JOBS,
                        "list_reconcile_jobs",
                        "error",
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CancelReconcileJobResponse> cancelReconcileJob(CancelReconcileJobRequest request) {
    var L = LogHelper.start(LOG, "CancelReconcileJob");
    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    var corr = principalContext.getCorrelationId();
                    authz.require(principalContext, "connector.manage");
                    var cancelled =
                        jobs.cancel(
                            principalContext.getAccountId(),
                            request.getJobId(),
                            request.getReason());
                    if (cancelled.isEmpty()) {
                      throw GrpcErrors.notFound(
                          corr,
                          GeneratedErrorMessages.MessageKey.JOB,
                          Map.of("id", request.getJobId()));
                    }
                    var cancelledJob = cancelled.get();
                    cancelChildJobs(
                        principalContext.getAccountId(), cancelledJob, request.getReason());
                    cancelled =
                        jobs.get(principalContext.getAccountId(), request.getJobId())
                            .map(job -> aggregateIfPlanJob(principalContext.getAccountId(), job))
                            .or(
                                () ->
                                    Optional.of(
                                        aggregateIfPlanJob(
                                            principalContext.getAccountId(), cancelledJob)));
                    if ("JS_CANCELLING".equals(cancelled.get().state)) {
                      cancellations.requestCancel(cancelled.get().jobId);
                    }
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.CANCEL_JOB,
                        "cancel_reconcile_job",
                        "success",
                        null);
                    return CancelReconcileJobResponse.newBuilder()
                        .setCancelled(
                            "JS_CANCELLED".equals(cancelled.get().state)
                                || "JS_CANCELLING".equals(cancelled.get().state))
                        .setJob(toResponse(cancelled.get()))
                        .build();
                  } catch (RuntimeException e) {
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.CANCEL_JOB,
                        "cancel_reconcile_job",
                        "error",
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetReconcilerSettingsResponse> getReconcilerSettings(
      GetReconcilerSettingsRequest request) {
    var L = LogHelper.start(LOG, "GetReconcilerSettings");
    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    authz.require(principalContext, "connector.manage");
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.GET_SETTINGS,
                        "get_reconciler_settings",
                        "success",
                        null);
                    return GetReconcilerSettingsResponse.newBuilder()
                        .setAutoEnabled(settings.isAutoEnabled())
                        .setDefaultInterval(
                            com.google.protobuf.Duration.newBuilder()
                                .setSeconds(settings.defaultIntervalMs() / 1000L)
                                .setNanos((int) (settings.defaultIntervalMs() % 1000L) * 1_000_000)
                                .build())
                        .setDefaultMode(settings.defaultMode())
                        .setFinishedJobRetention(
                            com.google.protobuf.Duration.newBuilder()
                                .setSeconds(settings.finishedJobRetentionMs() / 1000L)
                                .setNanos(
                                    (int) (settings.finishedJobRetentionMs() % 1000L) * 1_000_000)
                                .build())
                        .build();
                  } catch (RuntimeException e) {
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.GET_SETTINGS,
                        "get_reconciler_settings",
                        "error",
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateReconcilerSettingsResponse> updateReconcilerSettings(
      UpdateReconcilerSettingsRequest request) {
    var L = LogHelper.start(LOG, "UpdateReconcilerSettings");
    return mapFailures(
            run(
                () -> {
                  try {
                    var principalContext = principalProvider.get();
                    var corr = principalContext.getCorrelationId();
                    authz.require(principalContext, "connector.manage");
                    Boolean autoEnabled =
                        request.hasAutoEnabled() ? request.getAutoEnabled() : null;
                    Long defaultIntervalMs = null;
                    ReconcileMode defaultMode = null;
                    Long finishedJobRetentionMs = null;
                    if (request.hasDefaultInterval()) {
                      Duration d =
                          Duration.ofSeconds(request.getDefaultInterval().getSeconds())
                              .plusNanos(request.getDefaultInterval().getNanos());
                      defaultIntervalMs = d.toMillis();
                      if (defaultIntervalMs <= 0L) {
                        throw GrpcErrors.invalidArgument(
                            corr, null, Map.of("field", "default_interval"));
                      }
                    }
                    if (request.hasDefaultMode()) {
                      defaultMode = request.getDefaultMode();
                      if (defaultMode == ReconcileMode.RM_UNSPECIFIED) {
                        throw GrpcErrors.invalidArgument(
                            corr, null, Map.of("field", "default_mode"));
                      }
                    }
                    if (request.hasFinishedJobRetention()) {
                      Duration d =
                          Duration.ofSeconds(request.getFinishedJobRetention().getSeconds())
                              .plusNanos(request.getFinishedJobRetention().getNanos());
                      finishedJobRetentionMs = d.toMillis();
                      if (finishedJobRetentionMs <= 0L) {
                        throw GrpcErrors.invalidArgument(
                            corr, null, Map.of("field", "finished_job_retention"));
                      }
                    }
                    settings.update(
                        autoEnabled, defaultIntervalMs, defaultMode, finishedJobRetentionMs);
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.UPDATE_SETTINGS,
                        "update_reconciler_settings",
                        "success",
                        null);
                    return UpdateReconcilerSettingsResponse.newBuilder()
                        .setAutoEnabled(settings.isAutoEnabled())
                        .setDefaultInterval(
                            com.google.protobuf.Duration.newBuilder()
                                .setSeconds(settings.defaultIntervalMs() / 1000L)
                                .setNanos((int) (settings.defaultIntervalMs() % 1000L) * 1_000_000)
                                .build())
                        .setDefaultMode(settings.defaultMode())
                        .setFinishedJobRetention(
                            com.google.protobuf.Duration.newBuilder()
                                .setSeconds(settings.finishedJobRetentionMs() / 1000L)
                                .setNanos(
                                    (int) (settings.finishedJobRetentionMs() % 1000L) * 1_000_000)
                                .build())
                        .build();
                  } catch (RuntimeException e) {
                    observeReconcileRequestCounter(
                        ServiceMetrics.Reconcile.UPDATE_SETTINGS,
                        "update_reconciler_settings",
                        "error",
                        normalizeReason(e));
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static ReconcileScope scopeFromRequest(StartCaptureRequest request) {
    return request == null ? ReconcileScope.empty() : scopeFromCaptureScope(request.getScope());
  }

  private static ReconcileExecutionPolicy mapExecutionPolicy(
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

  private static ReconcileScope scopeFromCaptureScope(CaptureScope scope) {
    if (scope == null) {
      return ReconcileScope.empty();
    }
    var namespaces =
        scope.getDestinationNamespacePathsList().stream()
            .map(NamespacePath::getSegmentsList)
            .map(List::copyOf)
            .toList();
    return ReconcileScope.of(
        namespaces,
        scope.getDestinationTableDisplayName(),
        scope.getDestinationTableColumnsList(),
        scope.getDestinationSnapshotIdsList());
  }

  private static ResourceId connectorIdFromScope(CaptureScope scope) {
    return scope == null ? ResourceId.getDefaultInstance() : scope.getConnectorId();
  }

  private static void validateSnapshotScope(CaptureScope scope, String corr) {
    if (scope == null || scope.getDestinationSnapshotIdsList().isEmpty()) {
      return;
    }
    if (scope.getDestinationNamespacePathsCount() != 1) {
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", "scope.destination_namespace_paths"));
    }
    if (scope.getDestinationTableDisplayName() == null
        || scope.getDestinationTableDisplayName().isBlank()) {
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", "scope.destination_table_display_name"));
    }
  }

  private static CaptureMode mapCaptureMode(ai.floedb.floecat.reconciler.rpc.CaptureMode mode) {
    return switch (mode) {
      case CM_METADATA_ONLY -> CaptureMode.METADATA_ONLY;
      case CM_STATS_ONLY -> CaptureMode.STATS_ONLY;
      case CM_METADATA_AND_STATS, CM_UNSPECIFIED, UNRECOGNIZED -> CaptureMode.METADATA_AND_STATS;
    };
  }

  private ResourceId scopedConnectorId(String accountId, ResourceId connectorId, String corr) {
    ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", corr);
    return connectorId.toBuilder().setAccountId(accountId).build();
  }

  private static JobState toProtoState(String state) {
    if (state == null) {
      return JobState.JS_UNSPECIFIED;
    }
    return switch (state) {
      case "JS_QUEUED" -> JobState.JS_QUEUED;
      case "JS_RUNNING" -> JobState.JS_RUNNING;
      case "JS_SUCCEEDED" -> JobState.JS_SUCCEEDED;
      case "JS_FAILED" -> JobState.JS_FAILED;
      case "JS_CANCELLING" -> JobState.JS_CANCELLING;
      case "JS_CANCELLED" -> JobState.JS_CANCELLED;
      default -> JobState.JS_UNSPECIFIED;
    };
  }

  private static String fromProtoState(JobState state) {
    return switch (state) {
      case JS_QUEUED -> "JS_QUEUED";
      case JS_RUNNING -> "JS_RUNNING";
      case JS_SUCCEEDED -> "JS_SUCCEEDED";
      case JS_FAILED -> "JS_FAILED";
      case JS_CANCELLING -> "JS_CANCELLING";
      case JS_CANCELLED -> "JS_CANCELLED";
      default -> "";
    };
  }

  private ReconcileJobStore.ReconcileJob aggregateIfPlanJob(
      String accountId, ReconcileJobStore.ReconcileJob job) {
    if (job == null || job.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return job;
    }
    List<ReconcileJobStore.ReconcileJob> children = childJobsFor(accountId, job);
    if (children.isEmpty()) {
      return job;
    }

    String state = aggregateState(job, children);
    String message =
        children.stream()
            .filter(child -> child.message != null && !child.message.isBlank())
            .filter(child -> !"JS_SUCCEEDED".equals(child.state))
            .map(child -> child.jobId + ": " + child.message)
            .findFirst()
            .orElse(job.message);
    long startedAtMs =
        children.stream()
            .mapToLong(child -> child.startedAtMs)
            .filter(v -> v > 0L)
            .min()
            .orElse(job.startedAtMs);
    long finishedAtMs =
        isTerminalState(state)
            ? children.stream()
                .mapToLong(child -> child.finishedAtMs)
                .max()
                .orElse(job.finishedAtMs)
            : 0L;
    long tablesScanned = children.stream().mapToLong(child -> child.tablesScanned).sum();
    long tablesChanged = children.stream().mapToLong(child -> child.tablesChanged).sum();
    long errors = children.stream().mapToLong(child -> child.errors).sum();
    long snapshotsProcessed = children.stream().mapToLong(child -> child.snapshotsProcessed).sum();
    long statsProcessed = children.stream().mapToLong(child -> child.statsProcessed).sum();
    String executorId =
        children.stream()
            .map(child -> child.executorId == null ? "" : child.executorId)
            .filter(v -> !v.isBlank())
            .findFirst()
            .orElse(job.executorId);

    return new ReconcileJobStore.ReconcileJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        state,
        message,
        startedAtMs,
        finishedAtMs,
        tablesScanned,
        tablesChanged,
        errors,
        job.fullRescan,
        job.captureMode,
        snapshotsProcessed,
        statsProcessed,
        job.scope,
        job.executionPolicy,
        executorId,
        job.jobKind,
        job.tableTask,
        job.parentJobId);
  }

  private List<ReconcileJobStore.ReconcileJob> childJobsFor(
      String accountId, ReconcileJobStore.ReconcileJob planJob) {
    if (planJob == null || planJob.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return List.of();
    }
    List<ReconcileJobStore.ReconcileJob> out = new ArrayList<>();
    String nextToken = "";
    do {
      var page = jobs.list(accountId, 200, nextToken, planJob.connectorId, Set.of());
      for (var candidate : page.jobs) {
        if (planJob.jobId.equals(candidate.parentJobId)) {
          out.add(candidate);
        }
      }
      nextToken = page.nextPageToken;
    } while (nextToken != null && !nextToken.isBlank());
    return List.copyOf(out);
  }

  private void cancelChildJobs(
      String accountId, ReconcileJobStore.ReconcileJob job, String reason) {
    if (job == null || job.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return;
    }
    for (var child : childJobsFor(accountId, job)) {
      var cancelled = jobs.cancel(accountId, child.jobId, reason);
      if (cancelled.isPresent() && "JS_CANCELLING".equals(cancelled.get().state)) {
        cancellations.requestCancel(cancelled.get().jobId);
      }
    }
  }

  private static String aggregateState(
      ReconcileJobStore.ReconcileJob planJob, List<ReconcileJobStore.ReconcileJob> children) {
    if (children.stream().anyMatch(child -> "JS_CANCELLING".equals(child.state))) {
      return "JS_CANCELLING";
    }
    if (children.stream().anyMatch(child -> "JS_RUNNING".equals(child.state))) {
      return "JS_RUNNING";
    }
    if (children.stream().anyMatch(child -> "JS_QUEUED".equals(child.state))) {
      return "JS_QUEUED";
    }
    if (children.stream().anyMatch(child -> "JS_FAILED".equals(child.state))) {
      return "JS_FAILED";
    }
    if (children.stream().anyMatch(child -> "JS_CANCELLED".equals(child.state))) {
      return "JS_CANCELLED";
    }
    if ("JS_FAILED".equals(planJob.state) || "JS_CANCELLED".equals(planJob.state)) {
      return planJob.state;
    }
    return "JS_SUCCEEDED";
  }

  private static boolean isTerminalState(String state) {
    return "JS_SUCCEEDED".equals(state)
        || "JS_FAILED".equals(state)
        || "JS_CANCELLED".equals(state);
  }

  private static GetReconcileJobResponse toResponse(ReconcileJobStore.ReconcileJob job) {
    return GetReconcileJobResponse.newBuilder()
        .setJobId(job.jobId)
        .setConnectorId(job.connectorId)
        .setState(toProtoState(job.state))
        .setMessage(job.message == null ? "" : job.message)
        .setStartedAt(Timestamps.fromMillis(job.startedAtMs))
        .setFinishedAt(
            job.finishedAtMs == 0
                ? Timestamps.fromMillis(0)
                : Timestamps.fromMillis(job.finishedAtMs))
        .setTablesScanned(job.tablesScanned)
        .setTablesChanged(job.tablesChanged)
        .setErrors(job.errors)
        .setFullRescan(job.fullRescan)
        .setSnapshotsProcessed(job.snapshotsProcessed)
        .setStatsProcessed(job.statsProcessed)
        .setDurationMs(durationMs(job))
        .setExecutionPolicy(toProtoExecutionPolicy(job.executionPolicy))
        .setExecutorId(job.executorId == null ? "" : job.executorId)
        .setKind(toProtoJobKind(job.jobKind))
        .setParentJobId(job.parentJobId == null ? "" : job.parentJobId)
        .setTableTask(toProtoTableTask(job.tableTask))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileJobKind toProtoJobKind(
      ReconcileJobKind jobKind) {
    return switch (jobKind == null ? ReconcileJobKind.EXEC_CONNECTOR : jobKind) {
      case PLAN_CONNECTOR -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_PLAN_CONNECTOR;
      case EXEC_TABLE -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_TABLE;
      case EXEC_CONNECTOR -> ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_CONNECTOR;
    };
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileTableTask toProtoTableTask(
      ReconcileTableTask tableTask) {
    ReconcileTableTask effective = tableTask == null ? ReconcileTableTask.empty() : tableTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileTableTask.newBuilder()
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceTable(effective.sourceTable())
        .setDestinationTableDisplayName(effective.destinationTableDisplayName())
        .build();
  }

  private static long durationMs(ReconcileJobStore.ReconcileJob job) {
    long start = Math.max(0L, job.startedAtMs);
    if (start <= 0L) {
      return 0L;
    }
    long end = job.finishedAtMs > 0L ? job.finishedAtMs : System.currentTimeMillis();
    return Math.max(0L, end - start);
  }

  private void observeReconcileCounter(
      ai.floedb.floecat.telemetry.MetricId metric,
      String operation,
      String result,
      String trigger,
      String reason) {
    if (observability == null) {
      return;
    }
    if (reason == null || reason.isBlank()) {
      observability.counter(
          metric,
          1.0,
          Tag.of(TagKey.COMPONENT, "service"),
          Tag.of(TagKey.OPERATION, operation),
          Tag.of(TagKey.RESULT, result),
          Tag.of(TagKey.TRIGGER, trigger));
      return;
    }
    observability.counter(
        metric,
        1.0,
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, operation),
        Tag.of(TagKey.RESULT, result),
        Tag.of(TagKey.TRIGGER, trigger),
        Tag.of(TagKey.REASON, reason));
  }

  private void observeReconcileRequestCounter(
      ai.floedb.floecat.telemetry.MetricId metric, String operation, String result, String reason) {
    if (observability == null) {
      return;
    }
    if (reason == null || reason.isBlank()) {
      observability.counter(
          metric,
          1.0,
          Tag.of(TagKey.COMPONENT, "service"),
          Tag.of(TagKey.OPERATION, operation),
          Tag.of(TagKey.RESULT, result));
      return;
    }
    observability.counter(
        metric,
        1.0,
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, operation),
        Tag.of(TagKey.RESULT, result),
        Tag.of(TagKey.REASON, reason));
  }

  private static String startCaptureTrigger(StartCaptureRequest request) {
    if (request == null) {
      return "unknown";
    }
    CaptureScope scope = request.getScope();
    boolean scoped = isScoped(scope);
    return scoped ? "scoped" : "manual";
  }

  private static String captureNowTrigger(CaptureNowRequest request) {
    if (request == null) {
      return "unknown";
    }
    CaptureScope scope = request.getScope();
    boolean scoped = isScoped(scope);
    if (!scoped) {
      return "manual";
    }
    return switch (request.getMode()) {
      case CM_METADATA_ONLY -> "scoped_metadata";
      case CM_STATS_ONLY -> "scoped_stats_only";
      case CM_METADATA_AND_STATS, CM_UNSPECIFIED, UNRECOGNIZED -> "scoped_stats";
    };
  }

  private static boolean isScoped(CaptureScope scope) {
    if (scope == null) {
      return false;
    }
    return !scope.getDestinationNamespacePathsList().isEmpty()
        || (scope.getDestinationTableDisplayName() != null
            && !scope.getDestinationTableDisplayName().isBlank())
        || !scope.getDestinationTableColumnsList().isEmpty()
        || !scope.getDestinationSnapshotIdsList().isEmpty();
  }

  private static String normalizeReason(Throwable t) {
    if (t == null) {
      return "unknown";
    }
    String simple = t.getClass().getSimpleName();
    if (simple == null || simple.isBlank()) {
      return "runtime_exception";
    }
    return simple
        .replaceAll("([a-z])([A-Z])", "$1_$2")
        .replaceAll("[^A-Za-z0-9_]+", "_")
        .toLowerCase(java.util.Locale.ROOT);
  }
}
