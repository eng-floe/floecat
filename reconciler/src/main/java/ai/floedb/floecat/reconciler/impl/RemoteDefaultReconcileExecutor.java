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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteDefaultReconcileExecutor implements ReconcileExecutor {
  // Surfaces table-planning execution failures, which were previously not logged
  // anywhere (a failing job would silently requeue and retry).
  private static final Logger LOG = Logger.getLogger(RemoteDefaultReconcileExecutor.class);

  private final QueuedReconcileWorkerSupport queuedWorkerSupport;
  private final RemotePlannerWorkerClient workerClient;
  private final ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  private final boolean enabled;
  private final boolean workerAuthRequired;

  @Inject
  public RemoteDefaultReconcileExecutor(
      QueuedReconcileWorkerSupport queuedWorkerSupport,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-default.enabled",
              defaultValue = "false")
          boolean enabled,
      @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
          boolean workerAuthRequired) {
    this.queuedWorkerSupport = queuedWorkerSupport;
    this.workerClient = workerClient;
    this.reconcileWorkerAuthProvider = reconcileWorkerAuthProvider;
    this.enabled = enabled;
    this.workerAuthRequired = workerAuthRequired;
  }

  RemoteDefaultReconcileExecutor(
      QueuedReconcileWorkerSupport queuedWorkerSupport,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      boolean enabled) {
    this(queuedWorkerSupport, workerClient, reconcileWorkerAuthProvider, enabled, true);
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

    int snapshotChunkMaxCount = Math.max(1, workerClient.planTableChunkMaxCount());
    int[] submittedSnapshotChunks = {0};
    long[] lastSnapshotId = {Long.MIN_VALUE};
    List<PlannedSnapshotJob> pendingSnapshotJobs =
        new java.util.ArrayList<>(snapshotChunkMaxCount);
    QueuedReconcileWorkerSupport.TableExecutionResult tableExecution =
        queuedWorkerSupport.executePlannedTable(
            principal,
            connectorId,
            payload.fullRescan(),
            payload.scope(),
            payload.tableTask(),
            payload.captureMode(),
            workerAuthorizationHeader(lease.accountId),
            lease.jobId,
            lease.leaseEpoch,
            context.shouldStop(),
            progressListener,
            emission -> {
              if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY) {
                return;
              }
              FloecatConnector.SnapshotBundle bundle = emission.bundle();
              if (bundle == null
                  || bundle.snapshotId() < 0L
                  || bundle.snapshotId() == lastSnapshotId[0]) {
                return;
              }
              lastSnapshotId[0] = bundle.snapshotId();
              ReconcileSnapshotTask snapshotTask =
                  ReconcileSnapshotTask.of(
                          emission.tableId().getId(),
                          bundle.snapshotId(),
                          emission.sourceNamespace(),
                          emission.sourceTable())
                      .withContentState(
                          sourceRevision(bundle, bundle.snapshotId()),
                          metadataFingerprint(bundle, bundle.snapshotId()),
                          ReconcileSnapshotContentState.coverage(
                              payload.captureMode(), payload.scope()));
              pendingSnapshotJobs.add(new PlannedSnapshotJob(payload.scope(), snapshotTask));
              if (pendingSnapshotJobs.size() == snapshotChunkMaxCount) {
                int chunkIndex = submittedSnapshotChunks[0]++;
                if (!workerClient.submitPlanTableChunk(
                    remoteLease, chunkIndex, List.copyOf(pendingSnapshotJobs), 0)) {
                  throw plannerSubmissionRejected();
                }
                pendingSnapshotJobs.clear();
              }
            });
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
                0,
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

    if (!pendingSnapshotJobs.isEmpty()) {
      int chunkIndex = submittedSnapshotChunks[0]++;
      if (!workerClient.submitPlanTableChunk(
          remoteLease, chunkIndex, List.copyOf(pendingSnapshotJobs), 0)) {
        throw plannerSubmissionRejected();
      }
      pendingSnapshotJobs.clear();
    }
    int chunkCount = submittedSnapshotChunks[0];
    context.beforeHandledCompletion().run();
    boolean accepted;
    try {
      accepted =
          workerClient.submitPlanTableSuccess(
              remoteLease,
              chunkCount,
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
            remoteLease.lease().jobId,
            remoteLease.lease().leaseEpoch,
            context.shouldStop(),
            progressListener);
    ExecutionResult result = planned.result();
    if (result.cancelled) {
      return result;
    }
    if (result.error != null) {
      try {
        workerClient.submitPlanViewFailure(
            remoteLease,
            result.failureKind,
            result.retryDisposition,
            result.retryClass,
            result.message);
      } catch (RemoteLeasePreconditionFailedException leaseRejected) {
        return leaseNoLongerValid(context, remoteLease.lease(), connectorId, result);
      }
      return result;
    }
    context.beforeHandledCompletion().run();
    RemotePlannerWorkerClient.PlanViewSubmitResult submit;
    try {
      submit =
          workerClient.submitPlanViewSuccess(
              remoteLease,
              planned.mutation() == null
                  ? null
                  : new PlannedViewMutation(
                      planned.mutation().destinationViewId(),
                      planned.mutation().viewSpec(),
                      planned.mutation().idempotencyKey()));
    } catch (RemoteLeasePreconditionFailedException leaseRejected) {
      return leaseNoLongerValid(context, remoteLease.lease(), connectorId, result);
    }
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
        "%s result submission ignored because reconcile lease is no longer valid jobId=%s connectorId=%s",
        lease.jobKind, lease.jobId, connectorId);
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

  static String sourceRevision(FloecatConnector.SnapshotBundle bundle, long snapshotId) {
    if (bundle == null) {
      return "";
    }
    return Long.toString(snapshotId);
  }

  static String metadataFingerprint(FloecatConnector.SnapshotBundle bundle, long snapshotId) {
    if (bundle == null) {
      return "";
    }
    return ReconcileSnapshotContentState.fingerprint(
        Map.ofEntries(
            Map.entry("snapshotId", snapshotId),
            Map.entry("parentId", bundle.parentId()),
            Map.entry("schemaJson", blankToEmpty(bundle.schemaJson())),
            Map.entry(
                "partitionSpec",
                bundle.partitionSpec() == null
                    ? ""
                    : java.util.Base64.getEncoder()
                        .encodeToString(bundle.partitionSpec().toByteArray())),
            Map.entry("sequenceNumber", bundle.sequenceNumber()),
            Map.entry("manifestList", blankToEmpty(bundle.manifestList())),
            Map.entry("summary", bundle.summary() == null ? Map.of() : bundle.summary()),
            Map.entry("schemaId", bundle.schemaId())));
  }

  private String workerAuthorizationHeader(String accountId) {
    if (!workerAuthRequired) {
      return null;
    }
    return reconcileWorkerAuthProvider.authorizationHeader(accountId).orElse(null);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

}
