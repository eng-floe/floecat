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

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RemotePlannerReconcileExecutor implements ReconcileExecutor {
  private final ReconcilerService reconcilerService;
  private final RemotePlannerWorkerClient workerClient;
  private final boolean enabled;

  @Inject
  public RemotePlannerReconcileExecutor(
      ReconcilerService reconcilerService,
      RemotePlannerWorkerClient workerClient,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-planner.enabled",
              defaultValue = "false")
          boolean enabled) {
    this.reconcilerService = reconcilerService;
    this.workerClient = workerClient;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "remote_planner_worker";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 10;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR);
  }

  @Override
  public Set<String> supportedLanes() {
    return Set.of();
  }

  @Override
  public boolean supportsLane(String lane) {
    return true;
  }

  @Override
  public boolean supports(ReconcileJobStore.LeasedJob lease) {
    return lease != null && lease.jobKind == ReconcileJobKind.PLAN_CONNECTOR;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
    if (lease == null || lease.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }

    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    StandalonePlanConnectorPayload payload = workerClient.getPlanConnectorInput(remoteLease);
    ResourceId connectorId = payload.connectorId();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.planner")
            .setCorrelationId("reconcile-plan-" + lease.jobId)
            .build();

    long planned = 0L;
    long tablesPlanned = 0L;
    long viewsPlanned = 0L;

    List<PlannedTableJob> tableJobs = new java.util.ArrayList<>();
    List<PlannedViewJob> viewJobs = new java.util.ArrayList<>();

    try {
      ReconcileScope scope = payload.scope() == null ? ReconcileScope.empty() : payload.scope();
      if (!includesMetadata(payload.captureMode()) && scope.hasViewFilter()) {
        throw new IllegalArgumentException(
            "capture-only reconcile is not valid for view reconcile");
      }
      if (!includesMetadata(payload.captureMode())
          && scope.hasCaptureRequestFilter()
          && scope.hasNamespaceFilter()) {
        throw new IllegalArgumentException(
            "capture-only scoped capture requests cannot be combined with namespace scope");
      }
      if (scope.hasTableFilter()) {
        var tableTasks = reconcilerService.planTableTasks(principal, connectorId, scope, null);
        if (tableTasks.isEmpty()) {
          return ExecutionResult.terminalFailure(
              0,
              0,
              0,
              0,
              1,
              0,
              0,
              "No tables matched scope: " + scope.destinationTableId(),
              new IllegalArgumentException(
                  "No tables matched scope: " + scope.destinationTableId()));
        }
        var task = tableTasks.getFirst();
        if (context.shouldStop().getAsBoolean()) {
          return cancelled(tablesPlanned, viewsPlanned);
        }
        tableJobs.add(
            new PlannedTableJob(
                ReconcileScope.of(
                    List.of(),
                    scope.destinationTableId(),
                    scope.destinationCaptureRequests(),
                    scope.capturePolicy()),
                task));
        planned++;
        tablesPlanned++;
        context
            .progressListener()
            .onProgress(
                tablesPlanned,
                0,
                viewsPlanned,
                0,
                0,
                0,
                0,
                "Planned table " + task.sourceNamespace() + "." + task.sourceTable());
      } else if (scope.hasViewFilter()) {
        List<ReconcileViewTask> plannedViews =
            reconcilerService.planViewTasks(principal, connectorId, scope, null);
        if (plannedViews.isEmpty()) {
          return ExecutionResult.terminalFailure(
              0,
              0,
              0,
              0,
              1,
              0,
              0,
              "No views matched scope: " + scope.destinationViewId(),
              new IllegalArgumentException("No views matched scope: " + scope.destinationViewId()));
        }
        var task = plannedViews.getFirst();
        if (context.shouldStop().getAsBoolean()) {
          return cancelled(tablesPlanned, viewsPlanned);
        }
        viewJobs.add(
            new PlannedViewJob(ReconcileScope.ofView(List.of(), scope.destinationViewId()), task));
        planned++;
        viewsPlanned++;
        context
            .progressListener()
            .onProgress(
                tablesPlanned,
                0,
                viewsPlanned,
                0,
                0,
                0,
                0,
                "Planned view " + task.sourceNamespace() + "." + task.sourceView());
      } else {
        List<ReconcileTableTask> tableTasks =
            !includesMetadata(payload.captureMode()) && scope.hasCaptureRequestFilter()
                ? planCaptureOnlyScopedTableTasks(principal, connectorId, scope)
                : reconcilerService.planTableTasks(principal, connectorId, scope, null);
        List<ReconcileViewTask> plannedViews =
            includesMetadata(payload.captureMode())
                ? reconcilerService.planViewTasks(principal, connectorId, scope, null)
                : List.of();
        validateScopedCaptureRequestsMatched(payload.captureMode(), scope, tableTasks);
        if (includesMetadata(payload.captureMode())) {
          for (ReconcileViewTask task : plannedViews) {
            if (context.shouldStop().getAsBoolean()) {
              return cancelled(tablesPlanned, viewsPlanned);
            }
            viewJobs.add(new PlannedViewJob(scope, task));
            planned++;
            viewsPlanned++;
            context
                .progressListener()
                .onProgress(
                    tablesPlanned,
                    0,
                    viewsPlanned,
                    0,
                    0,
                    0,
                    0,
                    "Planned view " + task.sourceNamespace() + "." + task.sourceView());
          }
        }
        for (ReconcileTableTask task : tableTasks) {
          if (context.shouldStop().getAsBoolean()) {
            return cancelled(tablesPlanned, viewsPlanned);
          }
          tableJobs.add(
              new PlannedTableJob(
                  scopedTableExecutionScope(scope, payload.captureMode(), task), task));
          planned++;
          tablesPlanned++;
          context
              .progressListener()
              .onProgress(
                  tablesPlanned,
                  0,
                  viewsPlanned,
                  0,
                  0,
                  0,
                  0,
                  "Planned table " + task.sourceNamespace() + "." + task.sourceTable());
        }
      }
      if (context.shouldStop().getAsBoolean()) {
        return cancelled(tablesPlanned, viewsPlanned);
      }
      if (!workerClient.submitPlanConnectorSuccess(remoteLease, tableJobs, viewJobs)) {
        return ExecutionResult.failure(
            tablesPlanned,
            0,
            viewsPlanned,
            0,
            1,
            0,
            0,
            "standalone planner result submission was rejected",
            new IllegalStateException("planner result submission rejected"));
      }
      return ExecutionResult.success(
          tablesPlanned, 0, viewsPlanned, 0, 0, 0, 0, "Planned " + planned + " reconcile jobs");
    } catch (Exception e) {
      Exception classified =
          e instanceof ReconcileFailureException failure ? failure : classifyPlannerFailure(e);
      workerClient.submitPlanConnectorFailure(
          remoteLease,
          failureKindOf(classified),
          retryDispositionOf(classified),
          retryClassOf(classified),
          classified.getMessage());
      String message = e.getMessage();
      if (message == null || message.isBlank()) {
        message = e.getClass().getSimpleName();
      }
      String failureMessage =
          planned > 0
              ? "Planning failed after preparing " + planned + " reconcile jobs: " + message
              : "Planning failed: " + message;
      if (classified instanceof ReconcileFailureException failure
          && failure.retryDisposition() == ExecutionResult.RetryDisposition.TERMINAL) {
        return ExecutionResult.terminalFailure(
            tablesPlanned, 0, viewsPlanned, 0, 1, 0, 0, failureMessage, classified);
      }
      return ExecutionResult.failure(
          tablesPlanned,
          0,
          viewsPlanned,
          0,
          1,
          0,
          0,
          failureKindOf(classified),
          retryDispositionOf(classified),
          retryClassOf(classified),
          failureMessage,
          classified);
    }
  }

  private static Exception classifyPlannerFailure(Exception error) {
    if (error instanceof ReconcileFailureException) {
      return error;
    }
    if (error instanceof IllegalArgumentException || error instanceof IllegalStateException) {
      return new ReconcileFailureException(
          ExecutionResult.FailureKind.INTERNAL,
          ExecutionResult.RetryDisposition.TERMINAL,
          error.getMessage(),
          error);
    }
    return error;
  }

  private static ExecutionResult.FailureKind failureKindOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.failureKind()
        : ExecutionResult.FailureKind.INTERNAL;
  }

  private static ExecutionResult.RetryDisposition retryDispositionOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.retryDisposition()
        : ExecutionResult.RetryDisposition.RETRYABLE;
  }

  private static ExecutionResult.RetryClass retryClassOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.retryClass()
        : ExecutionResult.RetryClass.TRANSIENT_ERROR;
  }

  private static boolean includesMetadata(ReconcilerService.CaptureMode captureMode) {
    return captureMode != ReconcilerService.CaptureMode.CAPTURE_ONLY;
  }

  private List<ReconcileTableTask> planCaptureOnlyScopedTableTasks(
      PrincipalContext principal, ResourceId connectorId, ReconcileScope scope) {
    Map<String, List<ReconcileScope.ScopedCaptureRequest>> requestsByTableId =
        scope.destinationCaptureRequests().stream()
            .filter(request -> request != null && !request.tableId().isBlank())
            .collect(
                Collectors.groupingBy(
                    ReconcileScope.ScopedCaptureRequest::tableId,
                    LinkedHashMap::new,
                    Collectors.toList()));
    List<ReconcileTableTask> plannedTasks = new java.util.ArrayList<>();
    for (Map.Entry<String, List<ReconcileScope.ScopedCaptureRequest>> entry :
        requestsByTableId.entrySet()) {
      ReconcileScope strictScope =
          ReconcileScope.of(List.of(), entry.getKey(), entry.getValue(), scope.capturePolicy());
      plannedTasks.addAll(
          reconcilerService.planTableTasks(principal, connectorId, strictScope, null));
    }
    return plannedTasks;
  }

  private static ReconcileScope scopedTableExecutionScope(
      ReconcileScope scope, ReconcilerService.CaptureMode captureMode, ReconcileTableTask task) {
    if (includesMetadata(captureMode)
        || scope == null
        || !scope.hasCaptureRequestFilter()
        || task == null
        || task.destinationTableId() == null
        || task.destinationTableId().isBlank()) {
      return scope;
    }
    List<ReconcileScope.ScopedCaptureRequest> requests =
        scope.destinationCaptureRequests().stream()
            .filter(
                request -> request != null && task.destinationTableId().equals(request.tableId()))
            .toList();
    return ReconcileScope.of(List.of(), task.destinationTableId(), requests, scope.capturePolicy());
  }

  private static void validateScopedCaptureRequestsMatched(
      ReconcilerService.CaptureMode captureMode,
      ReconcileScope scope,
      List<ReconcileTableTask> tableTasks) {
    if (includesMetadata(captureMode)
        || scope == null
        || scope.destinationCaptureRequests().isEmpty()) {
      return;
    }
    LinkedHashSet<String> unmatchedTableIds =
        scope.destinationCaptureRequests().stream()
            .map(ReconcileScope.ScopedCaptureRequest::tableId)
            .filter(tableId -> tableId != null && !tableId.isBlank())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    for (ReconcileTableTask task :
        tableTasks == null ? List.<ReconcileTableTask>of() : tableTasks) {
      if (task == null
          || task.destinationTableId() == null
          || task.destinationTableId().isBlank()) {
        continue;
      }
      unmatchedTableIds.remove(task.destinationTableId());
    }
    if (!unmatchedTableIds.isEmpty()) {
      throw new IllegalArgumentException(
          "No tables matched scoped capture requests: "
              + unmatchedTableIds.stream().sorted().collect(Collectors.joining(", ")));
    }
  }

  private static ExecutionResult cancelled(long tablesPlanned, long viewsPlanned) {
    return ExecutionResult.cancelled(tablesPlanned, 0, viewsPlanned, 0, 0, 0, 0, "Cancelled");
  }
}
