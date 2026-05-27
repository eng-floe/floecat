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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteFileGroupReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(RemoteFileGroupReconcileExecutor.class);
  private static final long IN_BAND_PROGRESS_HEARTBEAT_MS = 5_000L;

  private final BlobStore blobStore;
  private final SnapshotPlanBlobStore snapshotPlanBlobStore;
  private final boolean enabled;
  private final RemoteFileGroupWorkerClient workerClient;
  private final StandaloneJavaFileGroupExecutionRunner runner;

  @Inject
  public RemoteFileGroupReconcileExecutor(
      BlobStore blobStore,
      SnapshotPlanBlobStore snapshotPlanBlobStore,
      RemoteFileGroupWorkerClient workerClient,
      StandaloneJavaFileGroupExecutionRunner runner,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-file-group.enabled",
              defaultValue = "false")
          boolean enabled) {
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
    this.snapshotPlanBlobStore =
        Objects.requireNonNull(snapshotPlanBlobStore, "snapshotPlanBlobStore");
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
          StandaloneJavaFileGroupExecutionRunner.PersistableResult.of(
              runner.execute(
                  payload,
                  context.shouldStop(),
                  new InBandProgressHeartbeat(context, payload).asRunnable()));
      String successResultId = successResultId(lease, payload);
      List<TargetStatsRecord> fileStats =
          captured.statsRecords().stream()
              .filter(
                  record ->
                      record != null
                          && record.hasTarget()
                          && StatsTargetType.from(record.getTarget()) == StatsTargetType.FILE)
              .toList();
      var uploadedStats = uploadFileStatsDirect(lease, successResultId, fileStats);
      var uploadedArtifacts = uploadIndexArtifactsDirect(lease, captured.stagedIndexArtifacts());
      var result =
          new StandaloneFileGroupExecutionResult(
              successResultId,
              uploadedStats.isEmpty() ? captured.statsRecords() : List.of(),
              uploadedStats,
              uploadedArtifacts == null ? captured.stagedIndexArtifacts() : List.of(),
              uploadedArtifacts == null ? List.of() : uploadedArtifacts);
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
      if (!workerClient.submitSuccess(remoteLease, result)) {
        throw terminalSubmissionUncertain(
            "standalone worker success result submission was rejected", null);
      }
    } catch (RuntimeException e) {
      throw terminalSubmissionUncertain(
          "standalone worker success result submission did not complete cleanly", e);
    }
    if (payload != null && !payload.requestsStats() && !payload.capturePageIndex()) {
      return ExecutionResult.success(0, 0, 0, 0, 0, 0, 0, successMessage);
    }
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
    return ExecutionResult.success(0, 0, 0, 0, 0, 0, statsProcessed, successMessage);
  }

  private List<StandaloneFileGroupExecutionResult.PreUploadedIndexArtifact>
      uploadIndexArtifactsDirect(
          ReconcileJobStore.LeasedJob lease,
          List<ai.floedb.floecat.reconciler.spi.ReconcilerBackend.StagedIndexArtifact>
              stagedArtifacts) {
    if (stagedArtifacts == null || stagedArtifacts.isEmpty()) {
      return List.of();
    }
    try {
      List<StandaloneFileGroupExecutionResult.PreUploadedIndexArtifact> uploaded =
          new ArrayList<>(stagedArtifacts.size());
      for (var artifact : stagedArtifacts) {
        if (artifact == null || artifact.record() == null) {
          continue;
        }
        byte[] content = artifact.content();
        if (content == null || content.length == 0) {
          throw new IllegalArgumentException("staged artifact missing content");
        }
        String artifactUri = artifact.record().getArtifactUri();
        if (artifactUri == null || artifactUri.isBlank()) {
          throw new IllegalArgumentException("staged artifact missing artifact uri");
        }
        String contentType =
            artifact.contentType() == null || artifact.contentType().isBlank()
                ? "application/x-parquet"
                : artifact.contentType();
        blobStore.put(artifactUri, content, contentType);
        uploaded.add(
            new StandaloneFileGroupExecutionResult.PreUploadedIndexArtifact(
                artifact.record(), contentType, artifactUri));
      }
      return List.copyOf(uploaded);
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Direct worker artifact upload failed for job %s; falling back to inline service upload",
          lease.jobId);
      return null;
    }
  }

  private StandaloneFileGroupExecutionResult.FileStatsBlobManifest uploadFileStatsDirect(
      ReconcileJobStore.LeasedJob lease, String resultId, List<TargetStatsRecord> statsRecords) {
    if (statsRecords == null || statsRecords.isEmpty()) {
      return StandaloneFileGroupExecutionResult.FileStatsBlobManifest.empty();
    }
    try {
      return snapshotPlanBlobStore.persistFileGroupStats(
          lease.accountId, lease.jobId, resultId, statsRecords);
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Direct worker file-stats upload failed for job %s; falling back to inline service submit",
          lease.jobId);
      return StandaloneFileGroupExecutionResult.FileStatsBlobManifest.empty();
    }
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

  private static final class InBandProgressHeartbeat {
    private final ReconcileExecutor.ExecutionContext context;
    private final StandaloneFileGroupExecutionPayload payload;
    private final AtomicLong lastReportedAtMs = new AtomicLong(0L);

    private InBandProgressHeartbeat(
        ReconcileExecutor.ExecutionContext context, StandaloneFileGroupExecutionPayload payload) {
      this.context = context;
      this.payload = payload;
    }

    private Runnable asRunnable() {
      return () -> {
        long now = System.currentTimeMillis();
        long last = lastReportedAtMs.get();
        if (last > 0L && now - last < IN_BAND_PROGRESS_HEARTBEAT_MS) {
          return;
        }
        if (!lastReportedAtMs.compareAndSet(last, now)) {
          return;
        }
        if (context.shouldStop().getAsBoolean()) {
          return;
        }
        context
            .progressListener()
            .onProgress(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "Executing file group "
                    + (payload == null ? "" : payload.groupId())
                    + " with "
                    + (payload == null ? 0 : payload.plannedFilePaths().size())
                    + " planned handles");
      };
    }
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
    String suffix = outcome == null ? "" : outcome.trim();
    if (!planId.isBlank() && !groupId.isBlank()) {
      return jobId + ":" + planId + ":" + groupId + ":" + suffix;
    }
    String leaseEpoch = lease == null || lease.leaseEpoch == null ? "" : lease.leaseEpoch.trim();
    return jobId + ":" + leaseEpoch + ":" + suffix;
  }
}
