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
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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
      List<ai.floedb.floecat.reconciler.jobs.ReconcileTableTask> tableTasks =
          reconcilerService.planTableTasks(principal, connectorId, lease.scope, null);
      List<ReconcileViewTask> viewTasks =
          lease.captureMode == ReconcilerService.CaptureMode.STATS_ONLY
              ? List.of()
              : reconcilerService.planViewTasks(principal, connectorId, lease.scope, null);
      ensureExecutionExecutorAvailable(tableTasks, viewTasks);
      if (tableTasks.isEmpty()
          && viewTasks.isEmpty()
          && lease.scope != null
          && lease.scope.hasTableFilter()) {
        return ExecutionResult.failure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            "No tables matched scope: " + lease.scope.destinationTableDisplayName(),
            new IllegalArgumentException(
                "No tables matched scope: " + lease.scope.destinationTableDisplayName()));
      }
      for (var task : tableTasks) {
        if (context.shouldStop().getAsBoolean()) {
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
        jobs.enqueueTableExecution(
            lease.accountId,
            lease.connectorId,
            lease.fullRescan,
            lease.captureMode,
            lease.scope,
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
                0,
                0,
                "Planned table " + task.sourceNamespace() + "." + task.sourceTable());
      }
      for (var task : viewTasks) {
        if (context.shouldStop().getAsBoolean()) {
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
        jobs.enqueueViewExecution(
            lease.accountId,
            lease.connectorId,
            lease.fullRescan,
            lease.captureMode,
            lease.scope,
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
      if (context.shouldStop().getAsBoolean()) {
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
            || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_TABLE))) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for EXEC_TABLE jobs");
    }
    if (!viewTasks.isEmpty()
        && (executorRegistry == null
            || !executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_VIEW))) {
      throw new IllegalStateException(
          "No enabled reconcile executor is available for EXEC_VIEW jobs");
    }
  }
}
