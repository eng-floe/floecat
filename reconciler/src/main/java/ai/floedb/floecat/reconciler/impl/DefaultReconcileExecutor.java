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
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Default executor that preserves the existing in-process ReconcilerService planning path. */
@ApplicationScoped
public class DefaultReconcileExecutor implements ReconcileExecutor {
  private final ReconcilerService reconcilerService;
  private final ReconcileJobStore jobs;
  private final ReconcileExecutorRegistry executorRegistry;
  private final boolean enabled;

  @Inject
  public DefaultReconcileExecutor(
      ReconcilerService reconcilerService,
      ReconcileJobStore jobs,
      ReconcileExecutorRegistry executorRegistry,
      @ConfigProperty(name = "floecat.reconciler.executor.default.enabled", defaultValue = "true")
          boolean enabled) {
    this.reconcilerService = reconcilerService;
    this.jobs = jobs;
    this.executorRegistry = executorRegistry;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "default_reconciler";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.PLAN_TABLE, ReconcileJobKind.PLAN_VIEW);
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
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();

    ReconcilerService.Result result =
        lease.jobKind == ReconcileJobKind.PLAN_VIEW
            ? reconcilerService.reconcileView(
                principal,
                connectorId,
                lease.scope,
                lease.viewTask,
                null,
                context.shouldStop(),
                context.progressListener()::onProgress)
            : reconcilerService.reconcile(
                principal,
                connectorId,
                lease.fullRescan,
                lease.scope,
                lease.tableTask,
                lease.captureMode,
                null,
                context.shouldStop(),
                context.progressListener()::onProgress);

    if (result.cancelled()) {
      return ExecutionResult.cancelled(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message());
    }
    if (!result.ok()) {
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          failureKindOf(result.error),
          result.message(),
          result.error);
    }
    if (lease.jobKind == ReconcileJobKind.PLAN_TABLE
        && lease.captureMode == ReconcilerService.CaptureMode.METADATA_AND_STATS) {
      List<ReconcileSnapshotTask> snapshotTasks =
          snapshotTasksForSuccessfulPlan(principal, connectorId, lease, result);
      ensureSnapshotPlanningExecutorAvailable(snapshotTasks);
      enqueueSnapshotPlans(lease, snapshotTasks, context);
      return ExecutionResult.success(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message());
    }
    return ExecutionResult.success(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        result.viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message());
  }

  private List<ReconcileSnapshotTask> snapshotTasksForSuccessfulPlan(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileJobStore.LeasedJob lease,
      ReconcilerService.Result result) {
    if (lease.captureMode != ReconcilerService.CaptureMode.METADATA_AND_STATS) {
      return List.of();
    }
    ReconcileTableTask task = resolvedTableTaskForSnapshotPlanning(lease.tableTask, result);
    if (task.isEmpty()
        || task.destinationTableId() == null
        || task.destinationTableId().isBlank()) {
      return List.of();
    }
    ReconcileScope scope = lease.scope == null ? ReconcileScope.empty() : lease.scope;
    return reconcilerService.planSnapshotTasks(
        principal, connectorId, lease.fullRescan, scope, task, null);
  }

  private static ReconcileTableTask resolvedTableTaskForSnapshotPlanning(
      ReconcileTableTask original, ReconcilerService.Result result) {
    ReconcileTableTask task = original == null ? ReconcileTableTask.empty() : original;
    if (task.isEmpty()) {
      return task;
    }
    if (task.destinationTableId() != null && !task.destinationTableId().isBlank()) {
      return task;
    }
    if (result == null || result.matchedTableIds().isEmpty()) {
      return task;
    }
    String resolvedTableId = result.matchedTableIds().getFirst();
    return task.discoveryMode()
        ? ReconcileTableTask.discovery(
            task.sourceNamespace(),
            task.sourceTable(),
            task.destinationNamespaceId(),
            resolvedTableId,
            task.destinationTableDisplayName())
        : ReconcileTableTask.of(
            task.sourceNamespace(),
            task.sourceTable(),
            task.destinationNamespaceId(),
            resolvedTableId,
            task.destinationTableDisplayName());
  }

  private void ensureSnapshotPlanningExecutorAvailable(List<ReconcileSnapshotTask> snapshotTasks) {
    if ((snapshotTasks == null || snapshotTasks.isEmpty())
        || (executorRegistry != null
            && executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_SNAPSHOT))) {
      return;
    }
    throw new IllegalStateException(
        "No enabled reconcile executor is available for PLAN_SNAPSHOT jobs");
  }

  private void enqueueSnapshotPlans(
      ReconcileJobStore.LeasedJob lease,
      List<ReconcileSnapshotTask> snapshotTasks,
      ExecutionContext context) {
    if (snapshotTasks == null || snapshotTasks.isEmpty()) {
      return;
    }
    ReconcileScope scope = lease.scope == null ? ReconcileScope.empty() : lease.scope;
    for (ReconcileSnapshotTask snapshotTask : snapshotTasks) {
      if (context.shouldStop().getAsBoolean()) {
        break;
      }
      jobs.enqueueSnapshotPlan(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          scope,
          snapshotTask,
          lease.executionPolicy,
          lease.jobId,
          lease.pinnedExecutorId);
    }
  }

  private static ExecutionResult.FailureKind failureKindOf(Exception error) {
    if (error instanceof ReconcileFailureException failure) {
      return failure.failureKind();
    }
    return ExecutionResult.FailureKind.INTERNAL;
  }
}
