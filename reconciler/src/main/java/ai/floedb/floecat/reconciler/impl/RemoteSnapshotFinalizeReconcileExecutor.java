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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Finalizes non-empty snapshots by reading immutable file-group payload manifests from storage. */
@ApplicationScoped
public class RemoteSnapshotFinalizeReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(RemoteSnapshotFinalizeReconcileExecutor.class);

  private final RemoteSnapshotFinalizeWorkerClient workerClient;
  private final BlobStore blobStore;
  private final SnapshotPlanBlobStore snapshotPlanBlobStore;
  private final boolean enabled;

  @Inject
  public RemoteSnapshotFinalizeReconcileExecutor(
      RemoteSnapshotFinalizeWorkerClient workerClient,
      BlobStore blobStore,
      SnapshotPlanBlobStore snapshotPlanBlobStore,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-snapshot-finalize.enabled",
              defaultValue = "true")
          boolean enabled) {
    this.workerClient = Objects.requireNonNull(workerClient, "workerClient");
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
    this.snapshotPlanBlobStore =
        Objects.requireNonNull(snapshotPlanBlobStore, "snapshotPlanBlobStore");
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "remote_snapshot_finalize_worker";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 25;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE);
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
    if (lease == null || lease.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
      return false;
    }
    ReconcileSnapshotTask task =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    return task.completionMode() == ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        && task.fileGroupCount() > 0;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    ReconcileJobStore.LeasedJob lease = context.lease();
    if (!supports(lease)) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "Unsupported snapshot finalize job",
          new IllegalArgumentException("non-empty file-group snapshot task is required"));
    }
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    StandaloneSnapshotFinalizeExecutionPayload input = null;
    boolean terminalSubmissionStarted = false;
    try {
      input = workerClient.getSnapshotFinalizeInput(remoteLease);
      Set<GroupKey> remainingPlannedGroups = loadPlannedGroupKeys(input);
      List<ReconcileFileGroupResultDescriptor> descriptors =
          workerClient.listSnapshotFileGroupResults(remoteLease);
      if (descriptors.size() != input.fileGroupCount()) {
        throw new IllegalStateException(
            "snapshot finalizer descriptor count mismatch expected="
                + input.fileGroupCount()
                + " actual="
                + descriptors.size());
      }
      Set<GroupKey> descriptorGroupKeys = new HashSet<>();
      for (ReconcileFileGroupResultDescriptor descriptor : descriptors) {
        if (context.shouldStop().getAsBoolean()) {
          return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
        }
        if (descriptor == null) {
          throw new IllegalStateException("null snapshot file-group descriptor");
        }
        GroupKey groupKey = new GroupKey(descriptor.planId(), descriptor.groupId());
        if (!descriptorGroupKeys.add(groupKey)) {
          throw new IllegalStateException(
              "duplicate snapshot file-group descriptor "
                  + descriptor.planId()
                  + "/"
                  + descriptor.groupId());
        }
        if (!remainingPlannedGroups.remove(groupKey)) {
          throw new IllegalStateException(
              "unexpected snapshot file-group descriptor "
                  + descriptor.planId()
                  + "/"
                  + descriptor.groupId());
        }
      }
      if (!remainingPlannedGroups.isEmpty()) {
        throw new IllegalStateException(
            "missing snapshot file-group descriptors " + remainingPlannedGroups);
      }
      List<TargetStatsRecord> partials = new ArrayList<>();
      for (ReconcileFileGroupResultDescriptor descriptor : descriptors) {
        if (context.shouldStop().getAsBoolean()) {
          return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
        }
        partials.addAll(loadValidatedPartials(lease, input, descriptor));
      }
      Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
      List<TargetStatsRecord> finalStats =
          FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
              input.tableId(), input.snapshotId(), aggregateKinds, partials);
      if (context.shouldStop().getAsBoolean()) {
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
      }
      terminalSubmissionStarted = true;
      context.beforeHandledCompletion().run();
      String resultId = resultId(lease, "success");
      if (!workerClient.submitSnapshotFinalizeSuccess(
          remoteLease, resultId, input.statsPayloadUri(), finalStats)) {
        throw terminalSubmissionUncertain(
            "snapshot finalizer result submission was rejected", null);
      }
      return ExecutionResult.successHandled(
          0, 0, 0, 0, 0, 1, finalStats.size(), "Finalized snapshot " + input.snapshotId());
    } catch (ReconcileFailureException error) {
      throw error;
    } catch (RuntimeException error) {
      if (terminalSubmissionStarted) {
        throw terminalSubmissionUncertain(
            "snapshot finalizer result submission did not complete cleanly", error);
      }
      if (context.shouldStop().getAsBoolean()) {
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
      }
      String message =
          "Snapshot finalize failed: "
              + (error.getMessage() == null
                  ? error.getClass().getSimpleName()
                  : error.getMessage());
      LOG.errorf(error, "%s jobId=%s", message, lease.jobId);
      workerClient.submitSnapshotFinalizeFailure(remoteLease, resultId(lease, "failure"), message);
      return ExecutionResult.failure(0, 0, 0, 0, 1, 0, 0, message, error);
    }
  }

  private Set<GroupKey> loadPlannedGroupKeys(StandaloneSnapshotFinalizeExecutionPayload input) {
    List<ReconcileFileGroupTask> plannedGroups =
        snapshotPlanBlobStore.loadFileGroupsByUri(input.snapshotPlanUri());
    if (plannedGroups.size() != input.fileGroupCount()) {
      throw new IllegalStateException(
          "snapshot plan file-group count mismatch expected="
              + input.fileGroupCount()
              + " actual="
              + plannedGroups.size());
    }
    Set<GroupKey> groupKeys = new HashSet<>();
    for (ReconcileFileGroupTask plannedGroup : plannedGroups) {
      if (plannedGroup == null
          || plannedGroup.isEmpty()
          || !input.tableId().getId().equals(plannedGroup.tableId())
          || input.snapshotId() != plannedGroup.snapshotId()) {
        throw new IllegalStateException("snapshot plan file-group identity mismatch");
      }
      GroupKey groupKey = new GroupKey(plannedGroup.planId(), plannedGroup.groupId());
      if (!groupKeys.add(groupKey)) {
        throw new IllegalStateException(
            "duplicate snapshot plan file-group "
                + plannedGroup.planId()
                + "/"
                + plannedGroup.groupId());
      }
    }
    return groupKeys;
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

  private List<TargetStatsRecord> loadValidatedPartials(
      ReconcileJobStore.LeasedJob lease,
      StandaloneSnapshotFinalizeExecutionPayload input,
      ReconcileFileGroupResultDescriptor descriptor) {
    if (descriptor == null
        || descriptor.formatVersion() != 1
        || !lease.accountId.equals(descriptor.accountId())
        || !lease.connectorId.equals(descriptor.connectorId())
        || !input.parentJobId().equals(descriptor.parentJobId())
        || !input.tableId().getId().equals(descriptor.tableId())
        || input.snapshotId() != descriptor.snapshotId()
        || descriptor.payloadUri().isBlank()
        || descriptor.payloadBytes() <= 0L) {
      throw new IllegalArgumentException("snapshot file-group descriptor identity mismatch");
    }
    byte[] bytes = blobStore.get(descriptor.payloadUri());
    if (bytes.length != descriptor.payloadBytes()) {
      throw new IllegalArgumentException("snapshot file-group result payload size mismatch");
    }
    String actualSha256 = Base64.getEncoder().encodeToString(sha256(bytes));
    if (!MessageDigest.isEqual(
        actualSha256.getBytes(StandardCharsets.US_ASCII),
        descriptor.payloadSha256().getBytes(StandardCharsets.US_ASCII))) {
      throw new IllegalArgumentException("snapshot file-group result payload sha256 mismatch");
    }
    final FileGroupResultPayload payload;
    try {
      payload = FileGroupResultPayload.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("snapshot file-group result payload is invalid", e);
    }
    if (payload.getFormatVersion() != 1
        || !descriptor.accountId().equals(payload.getAccountId())
        || !descriptor.connectorId().equals(payload.getConnectorId())
        || !descriptor.parentJobId().equals(payload.getParentJobId())
        || !descriptor.fileGroupJobId().equals(payload.getFileGroupJobId())
        || !descriptor.planId().equals(payload.getPlanId())
        || !descriptor.groupId().equals(payload.getGroupId())
        || !descriptor.tableId().equals(payload.getTableId())
        || descriptor.snapshotId() != payload.getSnapshotId()
        || !descriptor.leaseEpoch().equals(payload.getLeaseEpoch())
        || !descriptor.resultId().equals(payload.getResultId())
        || descriptor.succeededFileCount() != payload.getFileResultsCount()
        || descriptor.partialAggregateRecordCount() != payload.getPartialAggregateRecordsCount()) {
      throw new IllegalArgumentException("snapshot file-group result payload identity mismatch");
    }
    return payload.getPartialAggregateRecordsList();
  }

  private static Set<FloecatConnector.StatsTargetKind> requestedAggregateKinds(
      ReconcileJobStore.LeasedJob lease) {
    ReconcileCapturePolicy policy =
        lease == null || lease.scope == null
            ? ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    EnumSet<FloecatConnector.StatsTargetKind> kinds =
        EnumSet.noneOf(FloecatConnector.StatsTargetKind.class);
    if (policy.outputs().contains(ReconcileCapturePolicy.Output.TABLE_STATS)) {
      kinds.add(FloecatConnector.StatsTargetKind.TABLE);
    }
    if (policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      kinds.add(FloecatConnector.StatsTargetKind.COLUMN);
    }
    return kinds;
  }

  private static String resultId(ReconcileJobStore.LeasedJob lease, String outcome) {
    return lease.jobId + ":" + lease.leaseEpoch + ":" + outcome;
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private record GroupKey(String planId, String groupId) {
    private GroupKey {
      planId = planId == null ? "" : planId.trim();
      groupId = groupId == null ? "" : groupId.trim();
    }
  }
}
