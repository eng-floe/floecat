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
import ai.floedb.floecat.reconciler.impl.ReconcileExecutorRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
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
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@GrpcService
public class ReconcileControlImpl extends BaseServiceImpl implements ReconcileControl {

  @Inject ConnectorRepository connectorRepo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;
  @Inject ReconcileCancellationRegistry cancellations;
  @Inject ReconcileExecutorRegistry executorRegistry;
  @Inject ReconcilerSettingsStore settings;
  @Inject Observability observability;

  private static final Logger LOG = Logger.getLogger(ReconcileControl.class);
  private static final long CAPTURE_NOW_POLL_MS = 100L;

  @ConfigProperty(name = "floecat.reconciler.capture-now.default-wait", defaultValue = "10s")
  Duration captureNowDefaultWait = Duration.ofSeconds(10);

  @ConfigProperty(name = "floecat.reconciler.capture-now.max-wait", defaultValue = "30s")
  Duration captureNowMaxWait = Duration.ofSeconds(30);

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
                        validatedConnectorId(pc.getAccountId(), request.getScope(), corr);
                    var jobId =
                        enqueueCapture(
                            connectorId,
                            request.getFullRescan(),
                            mapCaptureMode(request.getMode()),
                            scopeFromCaptureScope(request.getScope()),
                            ReconcileExecutionPolicy.of(
                                ReconcileExecutionClass.INTERACTIVE, "", Map.of()));
                    var job =
                        waitForTerminalJob(
                            pc.getAccountId(),
                            jobId,
                            corr,
                            effectiveCaptureNowWait(request),
                            Context.current());
                    observeReconcileCounter(
                        ServiceMetrics.Reconcile.CAPTURE_NOW,
                        "capture_now",
                        "success",
                        trigger,
                        null);
                    return CaptureNowResponse.newBuilder()
                        .setTablesScanned(job.tablesScanned)
                        .setTablesChanged(job.tablesChanged)
                        .setViewsScanned(job.viewsScanned)
                        .setViewsChanged(job.viewsChanged)
                        .setErrors(job.errors)
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
                    connectorRepo
                        .getById(connectorId)
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId,
                                    GeneratedErrorMessages.MessageKey.CONNECTOR,
                                    Map.of("id", connectorId.getId())));

                    var jobId =
                        enqueueCapture(
                            connectorId,
                            request.getFullRescan(),
                            mapCaptureMode(request.getMode()),
                            scopeFromRequest(request),
                            mapExecutionPolicy(request.getExecutionPolicy()));
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
        namespaces, scope.getDestinationTableDisplayName(), scope.getDestinationTableColumnsList());
  }

  private static ResourceId connectorIdFromScope(CaptureScope scope) {
    return scope == null ? ResourceId.getDefaultInstance() : scope.getConnectorId();
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

  private ResourceId validatedConnectorId(String accountId, CaptureScope scope, String corr) {
    var connectorId = scopedConnectorId(accountId, connectorIdFromScope(scope), corr);
    connectorRepo
        .getById(connectorId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    corr,
                    GeneratedErrorMessages.MessageKey.CONNECTOR,
                    Map.of("id", connectorId.getId())));
    return connectorId;
  }

  private String enqueueCapture(
      ResourceId connectorId,
      boolean fullRescan,
      CaptureMode mode,
      ReconcileScope scope,
      ReconcileExecutionPolicy executionPolicy) {
    ensureExecutorsAvailable();
    return jobs.enqueuePlan(
        connectorId.getAccountId(),
        connectorId.getId(),
        fullRescan,
        mode,
        scope,
        executionPolicy,
        "");
  }

  private void ensureExecutorsAvailable() {
    if (executorRegistry != null
        && !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_CONNECTOR)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("No enabled reconcile executor is available for PLAN_CONNECTOR jobs")
          .asRuntimeException();
    }
    if (executorRegistry != null
        && !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_TABLE)
        && !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_VIEW)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "No enabled reconcile executor is available for EXEC_TABLE or EXEC_VIEW jobs")
          .asRuntimeException();
    }
  }

  private ReconcileJobStore.ReconcileJob waitForTerminalJob(
      String accountId, String jobId, String corr, Duration waitBudget, Context grpcContext) {
    long deadlineNanos = System.nanoTime() + waitBudget.toNanos();
    Deadline grpcDeadline = grpcContext.getDeadline();
    while (true) {
      if (grpcContext.isCancelled()) {
        requestCaptureNowCancellation(
            accountId,
            jobId,
            grpcDeadline != null && grpcDeadline.isExpired()
                ? "capture_now timed out while waiting for completion"
                : "capture_now caller cancelled while waiting for completion");
        throw captureNowCancelledOrTimedOut(corr, grpcDeadline, waitBudget, true);
      }
      var job =
          jobs.get(accountId, jobId)
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          corr, GeneratedErrorMessages.MessageKey.JOB, Map.of("id", jobId)));
      var effectiveJob = aggregateIfPlanJob(accountId, job);
      switch (effectiveJob.state) {
        case "JS_SUCCEEDED" -> {
          return effectiveJob;
        }
        case "JS_FAILED" ->
            throw new IllegalStateException(
                "capture job failed"
                    + (effectiveJob.message == null || effectiveJob.message.isBlank()
                        ? ""
                        : ": " + effectiveJob.message));
        case "JS_CANCELLED", "JS_CANCELLING" ->
            throw new IllegalStateException(
                "capture job cancelled"
                    + (effectiveJob.message == null || effectiveJob.message.isBlank()
                        ? ""
                        : ": " + effectiveJob.message));
        default -> {
          if (System.nanoTime() >= deadlineNanos) {
            requestCaptureNowCancellation(
                accountId, jobId, "capture_now timed out while waiting for completion");
            throw captureNowTimeout(corr, waitBudget, true);
          }
          sleepForCaptureNowPoll(
              accountId, jobId, grpcContext, corr, waitBudget, grpcDeadline, deadlineNanos);
        }
      }
    }
  }

  private void sleepForCaptureNowPoll(
      String accountId,
      String jobId,
      Context grpcContext,
      String corr,
      Duration waitBudget,
      Deadline grpcDeadline,
      long deadlineNanos) {
    try {
      long remainingMillis =
          Duration.ofNanos(Math.max(0L, deadlineNanos - System.nanoTime())).toMillis();
      if (remainingMillis <= 0) {
        requestCaptureNowCancellation(
            accountId, jobId, "capture_now timed out while waiting for completion");
        throw captureNowTimeout(corr, waitBudget, true);
      }
      Thread.sleep(Math.min(CAPTURE_NOW_POLL_MS, remainingMillis));
      if (grpcContext.isCancelled()) {
        requestCaptureNowCancellation(
            accountId,
            jobId,
            grpcDeadline != null && grpcDeadline.isExpired()
                ? "capture_now timed out while waiting for completion"
                : "capture_now caller cancelled while waiting for completion");
        throw captureNowCancelledOrTimedOut(corr, grpcDeadline, waitBudget, true);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("capture wait interrupted", e);
    }
  }

  private void requestCaptureNowCancellation(String accountId, String jobId, String reason) {
    jobs.cancel(accountId, jobId, reason)
        .filter(cancelled -> "JS_CANCELLING".equals(cancelled.state))
        .ifPresent(cancelled -> cancellations.requestCancel(cancelled.jobId));
  }

  private Duration effectiveCaptureNowWait(CaptureNowRequest request) {
    Duration requested =
        request != null && request.hasMaxWait()
            ? fromProtoDuration(request.getMaxWait())
            : captureNowDefaultWait;
    if (requested.isZero() || requested.isNegative()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("capture_now max_wait must be greater than 0")
          .asRuntimeException();
    }
    return requested.compareTo(captureNowMaxWait) > 0 ? captureNowMaxWait : requested;
  }

  private static Duration fromProtoDuration(com.google.protobuf.Duration duration) {
    return Duration.ofSeconds(duration.getSeconds()).plusNanos(duration.getNanos());
  }

  private static StatusRuntimeException captureNowCancelledOrTimedOut(
      String corr, Deadline grpcDeadline, Duration waitBudget, boolean cancellationRequested) {
    if (grpcDeadline != null && grpcDeadline.isExpired()) {
      return captureNowTimeout(corr, waitBudget, cancellationRequested);
    }
    return Status.CANCELLED
        .withDescription(
            cancellationRequested
                ? "capture_now cancelled before completion; cancellation was requested for the queued/running job"
                : "capture_now cancelled before completion")
        .asRuntimeException();
  }

  private static StatusRuntimeException captureNowTimeout(
      String corr, Duration waitBudget, boolean cancellationRequested) {
    return Status.DEADLINE_EXCEEDED
        .withDescription(
            "capture_now did not complete within "
                + waitBudget.toSeconds()
                + "s"
                + (cancellationRequested
                    ? "; cancellation was requested for the queued/running job"
                    : "")
                + "; use StartCapture for async execution")
        .asRuntimeException();
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
    boolean planTerminatedFirst = "JS_FAILED".equals(job.state) || "JS_CANCELLED".equals(job.state);
    String message =
        planTerminatedFirst
            ? job.message
            : children.stream()
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
            ? Math.max(
                job.finishedAtMs,
                children.stream().mapToLong(child -> child.finishedAtMs).max().orElse(0L))
            : 0L;
    long tablesScanned = children.stream().mapToLong(child -> child.tablesScanned).sum();
    long tablesChanged = children.stream().mapToLong(child -> child.tablesChanged).sum();
    long viewsScanned = children.stream().mapToLong(child -> child.viewsScanned).sum();
    long viewsChanged = children.stream().mapToLong(child -> child.viewsChanged).sum();
    long errors = job.errors + children.stream().mapToLong(child -> child.errors).sum();
    long snapshotsProcessed =
        job.snapshotsProcessed
            + children.stream().mapToLong(child -> child.snapshotsProcessed).sum();
    long statsProcessed =
        job.statsProcessed + children.stream().mapToLong(child -> child.statsProcessed).sum();
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
        viewsScanned,
        viewsChanged,
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
        job.viewTask,
        job.parentJobId);
  }

  private List<ReconcileJobStore.ReconcileJob> childJobsFor(
      String accountId, ReconcileJobStore.ReconcileJob planJob) {
    if (planJob == null || planJob.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return List.of();
    }
    return jobs.childJobs(accountId, planJob.jobId);
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
    if ("JS_FAILED".equals(planJob.state) || "JS_CANCELLED".equals(planJob.state)) {
      return planJob.state;
    }
    if ("JS_CANCELLING".equals(planJob.state)) {
      return "JS_CANCELLING";
    }
    if ("JS_RUNNING".equals(planJob.state)) {
      return "JS_RUNNING";
    }
    if ("JS_QUEUED".equals(planJob.state)) {
      return "JS_QUEUED";
    }
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
        .setViewsScanned(job.viewsScanned)
        .setViewsChanged(job.viewsChanged)
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
        .setViewTask(toProtoViewTask(job.viewTask))
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
        .setDestinationTableDisplayName(effective.destinationTableDisplayName())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileViewTask toProtoViewTask(
      ReconcileViewTask viewTask) {
    ReconcileViewTask effective = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    return ai.floedb.floecat.reconciler.rpc.ReconcileViewTask.newBuilder()
        .setSourceNamespace(effective.sourceNamespace())
        .setSourceView(effective.sourceView())
        .setDestinationNamespace(effective.destinationNamespace())
        .setDestinationViewDisplayName(effective.destinationViewDisplayName())
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
        || !scope.getDestinationTableColumnsList().isEmpty();
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
