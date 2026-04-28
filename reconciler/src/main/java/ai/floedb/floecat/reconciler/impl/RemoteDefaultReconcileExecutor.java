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
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RemoteDefaultReconcileExecutor implements ReconcileExecutor {
  private final ReconcilerService reconcilerService;
  private final RemotePlannerWorkerClient workerClient;
  private final boolean enabled;

  @Inject
  public RemoteDefaultReconcileExecutor(
      ReconcilerService reconcilerService,
      RemotePlannerWorkerClient workerClient,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-default.enabled",
              defaultValue = "false")
          boolean enabled) {
    this.reconcilerService = reconcilerService;
    this.workerClient = workerClient;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "remote_default_worker";
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
  public boolean supports(ReconcileJobStore.LeasedJob lease) {
    return lease != null
        && (lease.jobKind == ReconcileJobKind.PLAN_TABLE
            || lease.jobKind == ReconcileJobKind.PLAN_VIEW);
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
    if (lease == null) {
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    return lease.jobKind == ReconcileJobKind.PLAN_VIEW
        ? executeView(context, new RemoteLeasedJob(lease))
        : executeTable(context, new RemoteLeasedJob(lease));
  }

  private ExecutionResult executeTable(ExecutionContext context, RemoteLeasedJob remoteLease) {
    var lease = remoteLease.lease();
    StandalonePlanTablePayload payload = workerClient.getPlanTableInput(remoteLease);
    ResourceId connectorId = payload.connectorId();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();

    ReconcilerService.Result result =
        reconcilerService.reconcilePlannedTableExecution(
            principal,
            connectorId,
            payload.fullRescan(),
            payload.scope(),
            payload.tableTask(),
            payload.captureMode(),
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
      workerClient.submitPlanTableFailure(remoteLease, result.message());
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
    if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY) {
      if (!workerClient.submitPlanTableSuccess(remoteLease, List.of())) {
        return ExecutionResult.failure(
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            1,
            result.snapshotsProcessed,
            result.statsProcessed,
            "standalone planner result submission was rejected",
            new IllegalStateException("planner result submission rejected"));
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

    List<ReconcileSnapshotTask> snapshotTasks =
        snapshotTasksForSuccessfulPlan(principal, connectorId, payload, result);
    List<PlannedSnapshotJob> snapshotJobs =
        snapshotTasks.stream().map(task -> new PlannedSnapshotJob(payload.scope(), task)).toList();
    if (!workerClient.submitPlanTableSuccess(remoteLease, snapshotJobs)) {
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          1,
          result.snapshotsProcessed,
          result.statsProcessed,
          "standalone planner result submission was rejected",
          new IllegalStateException("planner result submission rejected"));
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

  private ExecutionResult executeView(ExecutionContext context, RemoteLeasedJob remoteLease) {
    StandalonePlanViewPayload payload = workerClient.getPlanViewInput(remoteLease);
    ResourceId connectorId = payload.connectorId();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(remoteLease.lease().accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + remoteLease.lease().jobId)
            .build();
    ReconcilerService.PlannedViewMutationResult planned =
        reconcilerService.planPlannedViewExecution(
            principal,
            connectorId,
            payload.scope(),
            payload.viewTask(),
            null,
            context.shouldStop(),
            context.progressListener()::onProgress);
    ReconcilerService.Result result = planned.result();
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
      workerClient.submitPlanViewFailure(remoteLease, result.message());
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
    var submit =
        workerClient.submitPlanViewSuccess(
            remoteLease,
            planned.mutation() == null
                ? null
                : new PlannedViewMutation(
                    planned.mutation().destinationViewId(),
                    planned.mutation().viewSpec(),
                    planned.mutation().idempotencyKey()));
    if (!submit.accepted()) {
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          1,
          result.snapshotsProcessed,
          result.statsProcessed,
          "standalone planner result submission was rejected",
          new IllegalStateException("planner result submission rejected"));
    }
    long viewsChanged = submit.viewsChanged();
    if (planned.mutation() != null) {
      context
          .progressListener()
          .onProgress(
              0,
              0,
              1,
              viewsChanged,
              0,
              0,
              0,
              "Finished view "
                  + payload.viewTask().sourceNamespace()
                  + "."
                  + payload.viewTask().sourceView());
    }
    return ExecutionResult.success(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message());
  }

  private List<ReconcileSnapshotTask> snapshotTasksForSuccessfulPlan(
      PrincipalContext principal,
      ResourceId connectorId,
      StandalonePlanTablePayload payload,
      ReconcilerService.Result result) {
    if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY) {
      return List.of();
    }
    ReconcileTableTask task = resolvedTableTaskForSnapshotPlanning(payload.tableTask(), result);
    if (task.isEmpty()
        || task.destinationTableId() == null
        || task.destinationTableId().isBlank()) {
      return List.of();
    }
    ReconcileScope scope = payload.scope() == null ? ReconcileScope.empty() : payload.scope();
    return reconcilerService.planSnapshotTasks(
        principal, connectorId, payload.fullRescan(), scope, task, payload.captureMode(), null);
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

  private static ExecutionResult.FailureKind failureKindOf(Exception error) {
    if (error instanceof ReconcileFailureException failure) {
      return failure.failureKind();
    }
    return ExecutionResult.FailureKind.INTERNAL;
  }
}
