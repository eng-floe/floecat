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
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
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
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteDefaultReconcileExecutor implements ReconcileExecutor {
  // Surfaces table-planning execution failures, which were previously not logged
  // anywhere (a failing job would silently requeue and retry).
  private static final Logger LOG = Logger.getLogger(RemoteDefaultReconcileExecutor.class);

  private final ReconcilerService reconcilerService;
  private final QueuedReconcileWorkerSupport queuedWorkerSupport;
  private final RemotePlannerWorkerClient workerClient;
  private final ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  private final boolean enabled;
  private final boolean workerAuthRequired;

  @Inject
  public RemoteDefaultReconcileExecutor(
      ReconcilerService reconcilerService,
      QueuedReconcileWorkerSupport queuedWorkerSupport,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-default.enabled",
              defaultValue = "false")
          boolean enabled,
      @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
          boolean workerAuthRequired) {
    this.reconcilerService = reconcilerService;
    this.queuedWorkerSupport = queuedWorkerSupport;
    this.workerClient = workerClient;
    this.reconcileWorkerAuthProvider = reconcileWorkerAuthProvider;
    this.enabled = enabled;
    this.workerAuthRequired = workerAuthRequired;
  }

  RemoteDefaultReconcileExecutor(
      ReconcilerService reconcilerService,
      QueuedReconcileWorkerSupport queuedWorkerSupport,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      boolean enabled) {
    this(
        reconcilerService,
        queuedWorkerSupport,
        workerClient,
        reconcileWorkerAuthProvider,
        enabled,
        true);
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
  public Set<String> supportedLanes() {
    return Set.of();
  }

  @Override
  public boolean supportsLane(String lane) {
    return true;
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
      return ExecutionResult.terminalFailure(
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
    ReconcileExecutor.ProgressListener progressListener = context.progressListener();
    StandalonePlanTablePayload payload = workerClient.getPlanTableInput(remoteLease);
    ResourceId connectorId = payload.connectorId();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();

    QueuedReconcileWorkerSupport.TableExecutionResult tableExecution =
        queuedWorkerSupport.executePlannedTable(
            principal,
            connectorId,
            payload.fullRescan(),
            payload.scope(),
            payload.tableTask(),
            payload.captureMode(),
            workerAuthorizationHeader(lease.accountId),
            context.shouldStop(),
            progressListener);
    ExecutionResult result = tableExecution.result();

    if (result.cancelled) {
      return result;
    }
    if (result.error != null) {
      LOG.warnf(
          result.error,
          "PLAN_TABLE execution failed jobId=%s connectorId=%s failureKind=%s retryDisposition=%s message=%s",
          lease.jobId,
          connectorId,
          result.failureKind,
          result.retryDisposition,
          result.message);
      try {
        workerClient.submitPlanTableFailure(
            remoteLease,
            result.failureKind,
            result.retryDisposition,
            result.retryClass,
            result.message);
      } catch (RemoteLeasePreconditionFailedException leaseRejected) {
        return leaseNoLongerValid(context, lease, connectorId, result);
      }
      return result;
    }
    if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY) {
      context.beforeHandledCompletion().run();
      boolean accepted;
      try {
        accepted =
            workerClient.submitPlanTableSuccess(
                remoteLease,
                List.of(),
                result.tablesScanned,
                result.tablesChanged,
                result.errors,
                result.snapshotsProcessed,
                result.statsProcessed);
      } catch (RemoteLeasePreconditionFailedException leaseRejected) {
        return leaseNoLongerValid(context, lease, connectorId, result);
      }
      if (!accepted) {
        throw plannerSubmissionRejected();
      }
      return ExecutionResult.successHandled(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message);
    }

    List<ReconcileSnapshotTask> snapshotTasks;
    try {
      snapshotTasks =
          snapshotTasksForSuccessfulPlan(principal, connectorId, payload, tableExecution);
    } catch (Exception e) {
      Exception classified =
          e instanceof ReconcileFailureException failure
              ? failure
              : ReconcileFailureClassifier.normalize(e);
      ExecutionResult.FailureKind failureKind = failureKindOf(classified);
      ExecutionResult.RetryDisposition retryDisposition = retryDispositionOf(classified);
      ExecutionResult.RetryClass retryClass = retryClassOf(classified);
      ReconcileTableTask task =
          payload.tableTask() == null ? ReconcileTableTask.empty() : payload.tableTask();
      LOG.warnf(
          classified,
          "PLAN_TABLE snapshot planning failed jobId=%s connectorId=%s tableId=%s source=%s.%s"
              + " captureMode=%s fullRescan=%s failureKind=%s retryDisposition=%s retryClass=%s"
              + " message=%s rootCause=%s",
          lease.jobId,
          connectorId,
          task.destinationTableId(),
          task.sourceNamespace(),
          task.sourceTable(),
          payload.captureMode(),
          payload.fullRescan(),
          failureKind,
          retryDisposition,
          retryClass,
          blankToEmpty(classified.getMessage()),
          rootCauseMessage(classified));
      try {
        workerClient.submitPlanTableFailure(
            remoteLease,
            failureKind,
            retryDisposition,
            retryClass,
            blankToEmpty(classified.getMessage()));
      } catch (RemoteLeasePreconditionFailedException leaseRejected) {
        return leaseNoLongerValid(context, lease, connectorId, result);
      }
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          1,
          result.snapshotsProcessed,
          result.statsProcessed,
          failureKind,
          retryDisposition,
          retryClass,
          blankToEmpty(classified.getMessage()),
          classified);
    }
    List<PlannedSnapshotJob> snapshotJobs =
        snapshotTasks.stream().map(task -> new PlannedSnapshotJob(payload.scope(), task)).toList();
    context.beforeHandledCompletion().run();
    boolean accepted;
    try {
      accepted =
          workerClient.submitPlanTableSuccess(
              remoteLease,
              snapshotJobs,
              result.tablesScanned,
              result.tablesChanged,
              result.errors,
              result.snapshotsProcessed,
              result.statsProcessed);
    } catch (RemoteLeasePreconditionFailedException leaseRejected) {
      return leaseNoLongerValid(context, lease, connectorId, result);
    }
    if (!accepted) {
      throw plannerSubmissionRejected();
    }
    return ExecutionResult.successHandled(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        result.viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message);
  }

  private ExecutionResult executeView(ExecutionContext context, RemoteLeasedJob remoteLease) {
    ReconcileExecutor.ProgressListener progressListener = context.progressListener();
    StandalonePlanViewPayload payload = workerClient.getPlanViewInput(remoteLease);
    ResourceId connectorId = payload.connectorId();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(remoteLease.lease().accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + remoteLease.lease().jobId)
            .build();
    QueuedReconcileWorkerSupport.PlannedViewMutationResult planned =
        queuedWorkerSupport.prepareViewMutation(
            principal,
            connectorId,
            payload.scope(),
            payload.viewTask(),
            workerAuthorizationHeader(remoteLease.lease().accountId),
            context.shouldStop(),
            progressListener);
    ExecutionResult result = planned.result();
    if (result.cancelled) {
      return result;
    }
    if (result.error != null) {
      workerClient.submitPlanViewFailure(
          remoteLease,
          result.failureKind,
          result.retryDisposition,
          result.retryClass,
          result.message);
      return result;
    }
    context.beforeHandledCompletion().run();
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
      throw plannerSubmissionRejected();
    }
    long viewsChanged = submit.viewsChanged();
    return ExecutionResult.successHandled(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message);
  }

  private ExecutionResult leaseNoLongerValid(
      ExecutionContext context,
      ReconcileJobStore.LeasedJob lease,
      ResourceId connectorId,
      ExecutionResult result) {
    LOG.infof(
        "PLAN_TABLE result submission ignored because reconcile lease is no longer valid jobId=%s connectorId=%s",
        lease.jobId, connectorId);
    context.beforeHandledCompletion().run();
    return ExecutionResult.cancelled(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        result.viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        "Lease no longer valid");
  }

  private static ReconcileFailureException plannerSubmissionRejected() {
    return new ReconcileFailureException(
        ExecutionResult.FailureKind.INTERNAL,
        ExecutionResult.RetryDisposition.RETRYABLE,
        ExecutionResult.RetryClass.STATE_UNCERTAIN,
        "standalone planner result submission was rejected",
        new IllegalStateException("planner result submission rejected"));
  }

  private List<ReconcileSnapshotTask> snapshotTasksForSuccessfulPlan(
      PrincipalContext principal,
      ResourceId connectorId,
      StandalonePlanTablePayload payload,
      QueuedReconcileWorkerSupport.TableExecutionResult tableExecution) {
    if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY) {
      return List.of();
    }
    ReconcileTableTask task =
        resolvedTableTaskForSnapshotPlanning(payload.tableTask(), tableExecution.matchedTableIds());
    if (task.isEmpty()
        || task.destinationTableId() == null
        || task.destinationTableId().isBlank()) {
      return List.of();
    }
    ReconcileScope scope = payload.scope() == null ? ReconcileScope.empty() : payload.scope();
    return reconcilerService.planSnapshotTasks(
        principal,
        connectorId,
        payload.fullRescan(),
        scope,
        task,
        payload.captureMode(),
        workerAuthorizationHeader(principal.getAccountId()));
  }

  private String workerAuthorizationHeader(String accountId) {
    if (!workerAuthRequired) {
      return null;
    }
    return reconcileWorkerAuthProvider.authorizationHeader(accountId).orElse(null);
  }

  private static String rootCauseMessage(Throwable error) {
    Throwable root = rootCause(error);
    if (root == null) {
      return "";
    }
    String message = root.getMessage();
    return message == null || message.isBlank() ? root.getClass().getSimpleName() : message;
  }

  private static Throwable rootCause(Throwable error) {
    var seen = new java.util.HashSet<Throwable>();
    Throwable cur = error;
    Throwable last = null;
    while (cur != null && !seen.contains(cur)) {
      seen.add(cur);
      last = cur;
      cur = cur.getCause();
    }
    return last;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
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

  private static ReconcileTableTask resolvedTableTaskForSnapshotPlanning(
      ReconcileTableTask original, List<String> matchedTableIds) {
    ReconcileTableTask task = original == null ? ReconcileTableTask.empty() : original;
    if (task.isEmpty()) {
      return task;
    }
    if (task.destinationTableId() != null && !task.destinationTableId().isBlank()) {
      return task;
    }
    if (matchedTableIds == null || matchedTableIds.isEmpty()) {
      return task;
    }
    String resolvedTableId = matchedTableIds.getFirst();
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
}
