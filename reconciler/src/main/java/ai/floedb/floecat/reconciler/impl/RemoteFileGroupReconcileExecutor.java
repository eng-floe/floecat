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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteFileGroupReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(RemoteFileGroupReconcileExecutor.class);

  private final boolean enabled;
  private final RemoteFileGroupWorkerClient workerClient;
  private final StandaloneJavaFileGroupExecutionRunner runner;

  @Inject
  public RemoteFileGroupReconcileExecutor(
      RemoteFileGroupWorkerClient workerClient,
      StandaloneJavaFileGroupExecutionRunner runner,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-file-group.enabled",
              defaultValue = "false")
          boolean enabled) {
    this.workerClient = Objects.requireNonNull(workerClient, "workerClient");
    this.runner = Objects.requireNonNull(runner, "runner");
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "remote_file_group_worker";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 20;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP);
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
    return lease != null && lease.jobKind == ReconcileJobKind.EXEC_FILE_GROUP;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
    if (lease == null || lease.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    if (context.shouldStop().getAsBoolean()) {
      return stopRequestedResult(null);
    }
    StandaloneFileGroupExecutionPayload payload = null;
    try {
      payload = workerClient.getExecution(remoteLease);
      if (payload.plannedFilePaths().isEmpty()) {
        return ExecutionResult.terminalFailure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            "planned file group does not contain any file handles",
            new IllegalStateException("planned file group does not contain any file handles"));
      }
      if (!payload.requestsStats() && !payload.capturePageIndex()) {
        return submitTerminalSuccess(
            context,
            lease,
            remoteLease,
            payload,
            StandaloneFileGroupExecutionResult.empty(successResultId(lease, payload)),
            0,
            "Skipped file group " + payload.groupId() + " (no capture outputs requested)");
      }
      var captured =
          StandaloneJavaFileGroupExecutionRunner.PersistableResult.of(runner.execute(payload));
      String successResultId = successResultId(lease, payload);
      var result =
          new StandaloneFileGroupExecutionResult(
              successResultId, captured.statsRecords(), captured.stagedIndexArtifacts());
      if (payload.capturePageIndex() && captured.stagedIndexArtifacts().isEmpty()) {
        throw new IllegalStateException(
            "page-index capture produced no staged artifacts for file group " + payload.groupId());
      }
      if (payload.capturePageIndex()) {
        List<String> missingArtifactFiles =
            FileGroupExecutionSupport.missingIndexArtifactFiles(
                ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.of(
                    payload.planId(),
                    payload.groupId(),
                    payload.tableId() == null ? "" : payload.tableId().getId(),
                    payload.snapshotId(),
                    payload.plannedFilePaths()),
                captured.stagedIndexArtifacts());
        if (!missingArtifactFiles.isEmpty()) {
          throw new IllegalStateException(
              "page-index capture did not produce artifacts for planned files in file group "
                  + payload.groupId()
                  + ": "
                  + String.join(", ", missingArtifactFiles));
        }
      }
      long statsProcessed = captured.statsRecords().size();
      return submitTerminalSuccess(
          context,
          lease,
          remoteLease,
          payload,
          result,
          statsProcessed,
          "Executed file group " + payload.groupId());
    } catch (ReconcileFailureException e) {
      throw e;
    } catch (RuntimeException e) {
      if (context.shouldStop().getAsBoolean()) {
        LOG.warnf(
            "Skipping file-group failure submission for job %s leaseEpoch=%s because outer stop was requested during execution",
            lease.jobId, lease.leaseEpoch);
        return stopRequestedResult(payload);
      }
      String failureDetail = failureDetail(e);
      LOG.errorf(
          e,
          "File-group capture failed planId=%s groupId=%s tableId=%s snapshotId=%d",
          payload == null ? "" : payload.planId(),
          payload == null ? "" : payload.groupId(),
          payload == null || payload.tableId() == null ? "" : payload.tableId().getId(),
          payload == null ? 0L : payload.snapshotId());
      workerClient.submitFailure(remoteLease, failureResultId(lease, payload), failureDetail);
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "File-group capture failed: " + failureDetail, e);
    }
  }

  private ExecutionResult submitTerminalSuccess(
      ExecutionContext context,
      ReconcileJobStore.LeasedJob lease,
      RemoteLeasedJob remoteLease,
      StandaloneFileGroupExecutionPayload payload,
      StandaloneFileGroupExecutionResult result,
      long statsProcessed,
      String successMessage) {
    if (shouldSkipTerminalSubmission(context.shouldStop(), lease, payload, "success")) {
      return stopRequestedResult(payload);
    }
    try {
      context.beforeHandledCompletion().run();
      if (!workerClient.submitSuccess(remoteLease, payload, result)) {
        throw terminalSubmissionUncertain(
            "standalone worker success result submission was rejected", null);
      }
    } catch (RuntimeException e) {
      throw terminalSubmissionUncertain(
          "standalone worker success result submission did not complete cleanly", e);
    }
    if (payload != null && !payload.requestsStats() && !payload.capturePageIndex()) {
      return ExecutionResult.successHandled(0, 0, 0, 0, 0, 0, 0, successMessage);
    }
    return ExecutionResult.successHandled(0, 0, 0, 0, 0, 0, statsProcessed, successMessage);
  }

  private boolean shouldSkipTerminalSubmission(
      BooleanSupplier shouldStop,
      ReconcileJobStore.LeasedJob lease,
      StandaloneFileGroupExecutionPayload payload,
      String outcome) {
    if (!shouldStop.getAsBoolean()) {
      return false;
    }
    LOG.warnf(
        "Skipping file-group terminal %s submission for job %s leaseEpoch=%s groupId=%s because outer stop was requested",
        outcome, lease.jobId, lease.leaseEpoch, payload == null ? "" : payload.groupId());
    return true;
  }

  private static ExecutionResult stopRequestedResult(StandaloneFileGroupExecutionPayload payload) {
    String groupId = payload == null ? "" : payload.groupId();
    return ExecutionResult.cancelled(
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        groupId == null || groupId.isBlank()
            ? "Stopped during file-group execution"
            : "Stopped during file-group execution for " + groupId);
  }

  private static String failureDetail(Throwable error) {
    if (error == null) {
      return "unknown error";
    }
    var seen = new HashSet<Throwable>();
    var parts = new ArrayList<String>();
    Throwable current = error;
    while (current != null && seen.add(current)) {
      parts.add(renderThrowable(current));
      current = current.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private static ReconcileFailureException terminalSubmissionUncertain(
      String message, RuntimeException cause) {
    return new ReconcileFailureException(
        ExecutionResult.FailureKind.INTERNAL,
        ExecutionResult.RetryDisposition.RETRYABLE,
        ExecutionResult.RetryClass.STATE_UNCERTAIN,
        message,
        cause);
  }

  private static String renderThrowable(Throwable error) {
    String type = error.getClass().getSimpleName();
    String message = error.getMessage();
    if (message == null || message.isBlank()) {
      return type;
    }
    return type + ": " + message;
  }

  private static String successResultId(
      ReconcileJobStore.LeasedJob lease, StandaloneFileGroupExecutionPayload payload) {
    return resultId(lease, payload, "success");
  }

  private static String failureResultId(
      ReconcileJobStore.LeasedJob lease, StandaloneFileGroupExecutionPayload payload) {
    return resultId(lease, payload, "failure");
  }

  private static String resultId(
      ReconcileJobStore.LeasedJob lease,
      StandaloneFileGroupExecutionPayload payload,
      String outcome) {
    String jobId = lease == null || lease.jobId == null ? "" : lease.jobId.trim();
    String planId =
        payload != null && payload.planId() != null && !payload.planId().isBlank()
            ? payload.planId().trim()
            : (lease == null || lease.fileGroupTask == null ? "" : lease.fileGroupTask.planId());
    String groupId =
        payload != null && payload.groupId() != null && !payload.groupId().isBlank()
            ? payload.groupId().trim()
            : (lease == null || lease.fileGroupTask == null ? "" : lease.fileGroupTask.groupId());
    String leaseEpoch = lease == null || lease.leaseEpoch == null ? "" : lease.leaseEpoch.trim();
    String suffix = outcome == null ? "" : outcome.trim();
    // lease_epoch scopes the result_id to a single execution attempt: a re-leased retry
    // re-executes and may stage byte-different (but logically equal) chunks, so without the
    // epoch two attempts collide on the floecat idempotency key (jobId:resultId:chunk:idx)
    // and the second submission is rejected with "Conflict detected". See
    // docs/rust-remote-capture-executor.md for the result_id contract.
    return jobId + ":" + planId + ":" + groupId + ":" + leaseEpoch + ":" + suffix;
  }
}
