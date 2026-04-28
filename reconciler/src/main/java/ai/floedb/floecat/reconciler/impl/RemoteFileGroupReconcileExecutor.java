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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RemoteFileGroupReconcileExecutor implements ReconcileExecutor {
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
    this.workerClient = workerClient;
    this.runner = runner;
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
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    StandaloneFileGroupExecutionPayload payload = workerClient.getExecution(remoteLease);
    if (payload.plannedFilePaths().isEmpty()) {
      return ExecutionResult.failure(
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
      if (!workerClient.submitSuccess(
          remoteLease, StandaloneFileGroupExecutionResult.empty(successResultId(lease, payload)))) {
        return ExecutionResult.failure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            "standalone worker no-op result submission was rejected",
            new IllegalStateException("worker no-op result submission rejected"));
      }
      return ExecutionResult.success(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          "Skipped file group " + payload.groupId() + " (no capture outputs requested)");
    }
    try {
      var captured =
          StandaloneJavaFileGroupExecutionRunner.PersistableResult.of(runner.execute(payload));
      var result =
          new StandaloneFileGroupExecutionResult(
              successResultId(lease, payload),
              captured.statsRecords(),
              captured.stagedIndexArtifacts());
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
      if (!workerClient.submitSuccess(remoteLease, result)) {
        return ExecutionResult.failure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            "standalone worker result submission was rejected",
            new IllegalStateException("worker result submission rejected"));
      }
      long statsProcessed = captured.statsRecords().size();
      context
          .progressListener()
          .onProgress(
              0,
              0,
              0,
              0,
              0,
              0,
              statsProcessed,
              "Executed file group "
                  + payload.groupId()
                  + " with "
                  + payload.plannedFilePaths().size()
                  + " planned handles"
                  + (statsProcessed > 0 ? " and " + statsProcessed + " captured stats" : ""));
      return ExecutionResult.success(
          0, 0, 0, 0, 0, 0, statsProcessed, "Executed file group " + payload.groupId());
    } catch (RuntimeException e) {
      workerClient.submitFailure(remoteLease, failureResultId(lease, payload), e.getMessage());
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "File-group capture failed: " + e.getMessage(), e);
    }
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
    String suffix = outcome == null ? "" : outcome.trim();
    if (!planId.isBlank() && !groupId.isBlank()) {
      return jobId + ":" + planId + ":" + groupId + ":" + suffix;
    }
    String leaseEpoch = lease == null || lease.leaseEpoch == null ? "" : lease.leaseEpoch.trim();
    return jobId + ":" + leaseEpoch + ":" + suffix;
  }
}
