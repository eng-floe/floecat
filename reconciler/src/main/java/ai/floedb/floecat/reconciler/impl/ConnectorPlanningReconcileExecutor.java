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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ConnectorPlanningReconcileExecutor implements ReconcileExecutor {
  private final ReconcilerService reconcilerService;
  private final ReconcileJobStore jobs;
  private final ReconcileExecutorRegistry executorRegistry;
  private final boolean enabled;
  private final String executionPinnedExecutorId;

  @Inject
  public ConnectorPlanningReconcileExecutor(
      ReconcilerService reconcilerService,
      ReconcileJobStore jobs,
      ReconcileExecutorRegistry executorRegistry,
      @ConfigProperty(name = "floecat.reconciler.executor.planner.enabled", defaultValue = "true")
          boolean enabled) {
    this.reconcilerService = reconcilerService;
    this.jobs = jobs;
    this.executorRegistry = executorRegistry;
    this.enabled = enabled;
    this.executionPinnedExecutorId =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.reconciler.auto.pinned-executor-id", String.class)
            .map(String::trim)
            .orElse("");
  }

  @Override
  public String id() {
    return "planner_reconciler";
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
    // Planner jobs always run wherever the planner executor is present; the child table jobs
    // carry the actual execution lane policy that remote/local workers enforce later.
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
    var connectorId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(lease.connectorId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.planner")
            .setCorrelationId("reconcile-plan-" + lease.jobId)
            .build();
    long planned = 0L;
    long tablesPlanned = 0L;
    long changed = 0L;
    long viewsPlanned = 0L;
    long viewsChanged = 0L;
    long errors = 0L;
    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;

    try {
      ReconcileScope scope = lease.scope == null ? ReconcileScope.empty() : lease.scope;
      if (!includesMetadata(lease.captureMode) && scope.hasViewFilter()) {
        throw new IllegalArgumentException(
            "capture-only reconcile is not valid for view reconcile");
      }
      if (!includesMetadata(lease.captureMode)
          && scope.hasCaptureRequestFilter()
          && scope.hasNamespaceFilter()) {
        throw new IllegalArgumentException(
            "capture-only scoped capture requests cannot be combined with namespace scope");
      }
      if (scope.hasTableFilter()) {
        var tableTasks = reconcilerService.planTableTasks(principal, connectorId, scope, null);
        ensureTableExecutorAvailable();
        if (tableTasks.isEmpty()) {
          return ExecutionResult.failure(
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
          return cancelled(
              tablesPlanned,
              changed,
              viewsPlanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
        }
        jobs.enqueueTablePlan(
            lease.accountId,
            lease.connectorId,
            lease.fullRescan,
            lease.captureMode,
            ReconcileScope.of(
                List.of(),
                scope.destinationTableId(),
                scope.destinationCaptureRequests(),
                scope.capturePolicy()),
            task,
            lease.executionPolicy,
            lease.jobId,
            executionPinnedExecutorId);
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
                snapshotsProcessed,
                0,
                "Planned table " + task.sourceNamespace() + "." + task.sourceTable());
      } else if (scope.hasViewFilter()) {
        List<ReconcileViewTask> viewTasks =
            reconcilerService.planViewTasks(principal, connectorId, scope, null);
        ensureViewExecutorAvailable();
        if (viewTasks.isEmpty()) {
          return ExecutionResult.failure(
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
        var task = viewTasks.getFirst();
        if (context.shouldStop().getAsBoolean()) {
          return cancelled(
              tablesPlanned,
              changed,
              viewsPlanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
        }
        jobs.enqueueViewPlan(
            lease.accountId,
            lease.connectorId,
            lease.fullRescan,
            lease.captureMode,
            ReconcileScope.ofView(List.of(), scope.destinationViewId()),
            task,
            lease.executionPolicy,
            lease.jobId,
            executionPinnedExecutorId);
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
            !includesMetadata(lease.captureMode) && scope.hasCaptureRequestFilter()
                ? planCaptureOnlyScopedTableTasks(principal, connectorId, scope)
                : reconcilerService.planTableTasks(principal, connectorId, scope, null);
        List<ReconcileViewTask> viewTasks =
            includesMetadata(lease.captureMode)
                ? reconcilerService.planViewTasks(principal, connectorId, scope, null)
                : List.of();
        validateScopedCaptureRequestsMatched(lease.captureMode, scope, tableTasks);
        ensureExecutionExecutorAvailable(tableTasks, viewTasks);
        if (includesMetadata(lease.captureMode)) {
          for (ReconcileViewTask task : viewTasks) {
            if (context.shouldStop().getAsBoolean()) {
              return cancelled(
                  tablesPlanned,
                  changed,
                  viewsPlanned,
                  viewsChanged,
                  errors,
                  snapshotsProcessed,
                  statsProcessed);
            }
            jobs.enqueueViewPlan(
                lease.accountId,
                lease.connectorId,
                lease.fullRescan,
                lease.captureMode,
                scope,
                task,
                lease.executionPolicy,
                lease.jobId,
                executionPinnedExecutorId);
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
            return cancelled(
                tablesPlanned,
                changed,
                viewsPlanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed);
          }
          jobs.enqueueTablePlan(
              lease.accountId,
              lease.connectorId,
              lease.fullRescan,
              lease.captureMode,
              scopedTableExecutionScope(scope, lease.captureMode, task),
              task,
              lease.executionPolicy,
              lease.jobId,
              executionPinnedExecutorId);
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
                  snapshotsProcessed,
                  0,
                  "Planned table " + task.sourceNamespace() + "." + task.sourceTable());
        }
      }
      if (context.shouldStop().getAsBoolean()) {
        return cancelled(
            tablesPlanned,
            changed,
            viewsPlanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed);
      }
      return ExecutionResult.success(
          tablesPlanned,
          changed,
          viewsPlanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          "Planned " + planned + " reconcile jobs");
    } catch (Exception e) {
      String message = e.getMessage();
      if (message == null || message.isBlank()) {
        message = e.getClass().getSimpleName();
      }
      return ExecutionResult.failure(
          tablesPlanned,
          0,
          viewsPlanned,
          0,
          1,
          0,
          0,
          planned > 0
              ? "Planning failed after enqueuing " + planned + " reconcile jobs: " + message
              : "Planning failed: " + message,
          e);
    }
  }

  private void ensureExecutionExecutorAvailable(
      List<ai.floedb.floecat.reconciler.jobs.ReconcileTableTask> tableTasks,
      List<ReconcileViewTask> viewTasks) {
    if (!tableTasks.isEmpty()
        && (executorRegistry == null
            || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE))) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for PLAN_TABLE jobs");
    }
    if (!viewTasks.isEmpty()
        && (executorRegistry == null
            || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW))) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for PLAN_VIEW jobs");
    }
  }

  private void ensureTableExecutorAvailable() {
    if (executorRegistry == null
        || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for PLAN_TABLE jobs");
    }
  }

  private void ensureViewExecutorAvailable() {
    if (executorRegistry == null
        || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW)) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for PLAN_VIEW jobs");
    }
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

  private static ExecutionResult cancelled(
      long tablesPlanned,
      long changed,
      long viewsPlanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    return ExecutionResult.cancelled(
        tablesPlanned,
        changed,
        viewsPlanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        "Cancelled");
  }
}
