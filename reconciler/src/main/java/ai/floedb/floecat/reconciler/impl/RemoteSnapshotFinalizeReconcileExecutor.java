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

import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Finalizes file-group snapshots by reading immutable result manifests from storage. */
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
        && task.fileGroupPlanRecorded();
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
          new IllegalArgumentException("file-group snapshot task is required"));
    }
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    StandaloneSnapshotFinalizeExecutionPayload input = null;
    boolean terminalSubmissionStarted = false;
    try {
      input = workerClient.getSnapshotFinalizeInput(remoteLease);
      List<ReconcileFileGroupResultDescriptor> descriptors;
      Map<GroupKey, ReconcileFileGroupTask> plannedGroups = Map.of();
      if (input.fileGroupCount() == 0) {
        if (input.sourceFileCount() != 0) {
          throw new IllegalStateException(
              "explicit-empty snapshot finalizer has non-zero source file count "
                  + input.sourceFileCount());
        }
        descriptors = List.of();
      } else {
        plannedGroups = loadPlannedGroups(input);
        Set<GroupKey> remainingPlannedGroups = new HashSet<>(plannedGroups.keySet());
        descriptors = workerClient.listSnapshotFileGroupResults(remoteLease);
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
      }
      List<TargetStatsRecord> partials = new ArrayList<>();
      List<StatsObjectDescriptor> fileStats = new ArrayList<>();
      List<StatsObjectDescriptor> indexArtifacts = new ArrayList<>();
      for (ReconcileFileGroupResultDescriptor descriptor : descriptors) {
        if (context.shouldStop().getAsBoolean()) {
          return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
        }
        ReconcileFileGroupTask plannedGroup =
            plannedGroups.get(new GroupKey(descriptor.planId(), descriptor.groupId()));
        ValidatedFileGroupArtifacts artifacts =
            loadValidatedArtifacts(lease, input, descriptor, plannedGroup);
        partials.addAll(artifacts.partialAggregates());
        fileStats.addAll(artifacts.fileStats());
        indexArtifacts.addAll(artifacts.indexArtifacts());
      }
      Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
      List<TargetStatsRecord> finalStats =
          input.fileGroupCount() == 0
              ? emptySnapshotStats(lease, input)
              : FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
                  input.tableId(), input.snapshotId(), aggregateKinds, partials);
      if (context.shouldStop().getAsBoolean()) {
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
      }
      String resultId = resultId(lease, "success");
      RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess prepared =
          workerClient.prepareSnapshotFinalizeSuccess(
              remoteLease,
              resultId,
              input.statsObjectPrefix(),
              input.captureManifestUri(),
              input.sourceFileCount(),
              descriptors,
              fileStats,
              finalStats,
              indexArtifacts,
              input.indexPredecessor());
      if (context.shouldStop().getAsBoolean()) {
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
      }
      terminalSubmissionStarted = true;
      context.beforeHandledCompletion().run();
      if (!workerClient.submitSnapshotFinalizeSuccess(remoteLease, prepared)) {
        IllegalStateException error =
            new IllegalStateException("snapshot finalizer result submission was rejected");
        return ExecutionResult.terminalFailure(0, 0, 0, 0, 1, 0, 0, error.getMessage(), error);
      }
      return ExecutionResult.successHandled(
          0, 0, 0, 0, 0, 1, finalStats.size(), "Finalized snapshot " + input.snapshotId());
    } catch (ReconcileFailureException error) {
      throw error;
    } catch (IllegalArgumentException | IllegalStateException error) {
      String message =
          "Snapshot finalize failed: "
              + (error.getMessage() == null
                  ? error.getClass().getSimpleName()
                  : error.getMessage());
      LOG.errorf(error, "%s jobId=%s", message, lease.jobId);
      if (!terminalSubmissionStarted) {
        workerClient.submitSnapshotFinalizeFailure(
            remoteLease, resultId(lease, "failure"), message);
      }
      return ExecutionResult.terminalFailure(0, 0, 0, 0, 1, 0, 0, message, error);
    } catch (RuntimeException error) {
      if (terminalSubmissionStarted) {
        String message =
            "Snapshot finalize submission failed: "
                + (error.getMessage() == null
                    ? error.getClass().getSimpleName()
                    : error.getMessage());
        return ExecutionResult.terminalFailure(0, 0, 0, 0, 1, 0, 0, message, error);
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

  private Map<GroupKey, ReconcileFileGroupTask> loadPlannedGroups(
      StandaloneSnapshotFinalizeExecutionPayload input) {
    List<ReconcileFileGroupTask> plannedGroups =
        snapshotPlanBlobStore.loadFileGroupsByUri(input.snapshotPlanUri());
    if (plannedGroups.size() != input.fileGroupCount()) {
      throw new IllegalStateException(
          "snapshot plan file-group count mismatch expected="
              + input.fileGroupCount()
              + " actual="
              + plannedGroups.size());
    }
    Map<GroupKey, ReconcileFileGroupTask> groupsByKey = new HashMap<>();
    for (ReconcileFileGroupTask plannedGroup : plannedGroups) {
      if (plannedGroup == null
          || plannedGroup.isEmpty()
          || !input.tableId().getId().equals(plannedGroup.tableId())
          || input.snapshotId() != plannedGroup.snapshotId()) {
        throw new IllegalStateException("snapshot plan file-group identity mismatch");
      }
      GroupKey groupKey = new GroupKey(plannedGroup.planId(), plannedGroup.groupId());
      if (groupsByKey.putIfAbsent(groupKey, plannedGroup) != null) {
        throw new IllegalStateException(
            "duplicate snapshot plan file-group "
                + plannedGroup.planId()
                + "/"
                + plannedGroup.groupId());
      }
    }
    return groupsByKey;
  }

  private ValidatedFileGroupArtifacts loadValidatedArtifacts(
      ReconcileJobStore.LeasedJob lease,
      StandaloneSnapshotFinalizeExecutionPayload input,
      ReconcileFileGroupResultDescriptor descriptor,
      ReconcileFileGroupTask plannedGroup) {
    ReconcileCapturePolicy capturePolicy =
        lease == null || lease.scope == null
            ? ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    boolean statsRequested = capturePolicy.requestsStats();
    Set<FloecatConnector.StatsTargetKind> requestedAggregates = requestedAggregateKinds(lease);
    if (descriptor == null
        || plannedGroup == null
        || descriptor.formatVersion() != 1
        || !lease.accountId.equals(descriptor.accountId())
        || !lease.connectorId.equals(descriptor.connectorId())
        || !input.parentJobId().equals(descriptor.parentJobId())
        || !input.tableId().getId().equals(descriptor.tableId())
        || input.snapshotId() != descriptor.snapshotId()
        || descriptor.plannedFileCount() != plannedGroup.filePaths().size()
        || descriptor.succeededFileCount() != descriptor.plannedFileCount()
        || descriptor.failedFileCount() != 0
        || descriptor.skippedFileCount() != 0
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
    if (descriptor.fileStatsRecordCount() != payload.getFileStatsCount()
        || descriptor.indexArtifactCount() != payload.getIndexArtifactsCount()) {
      throw new IllegalArgumentException("snapshot file-group result payload count mismatch");
    }
    if (capturePolicy.requestsIndexes()) {
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor =
          descriptor.indexPredecessor();
      if (predecessor == null
          || !payload.hasIndexPredecessor()
          || !predecessor.generationId().equals(payload.getIndexPredecessor().getGenerationId())
          || predecessor.activePointerVersion()
              != payload.getIndexPredecessor().getActivePointerVersion()
          || !predecessor
              .captureManifestUri()
              .equals(payload.getIndexPredecessor().getCaptureManifestUri())
          || predecessor.captureManifestPointerVersion()
              != payload.getIndexPredecessor().getCaptureManifestPointerVersion()) {
        throw new IllegalArgumentException(
            "snapshot file-group index predecessor does not match its payload");
      }
    }
    String artifactReferencesSha256 =
        ArtifactReferenceDigest.sha256(payload.getFileStatsList(), payload.getIndexArtifactsList());
    if (!artifactReferencesSha256.equals(descriptor.artifactReferencesSha256())) {
      throw new IllegalArgumentException(
          "snapshot file-group artifact references do not match the durable result descriptor");
    }
    Set<String> successfulFiles = new HashSet<>();
    for (var fileResult : payload.getFileResultsList()) {
      if (fileResult.getState()
              != ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SUCCEEDED
          || fileResult.getFilePath().isBlank()
          || !successfulFiles.add(fileResult.getFilePath())) {
        throw new IllegalArgumentException("snapshot file-group success results are invalid");
      }
    }
    Set<String> plannedFiles = new HashSet<>(plannedGroup.filePaths());
    if (plannedFiles.size() != plannedGroup.filePaths().size()
        || !successfulFiles.equals(plannedFiles)) {
      throw new IllegalArgumentException(
          "snapshot file-group success results do not match the immutable plan");
    }
    Set<String> expectedStatsTargets =
        statsRequested ? expectedFileStatsTargets(plannedGroup, successfulFiles) : Set.of();
    if (payload.getFileStatsCount() != expectedStatsTargets.size()) {
      throw new IllegalArgumentException(
          "snapshot file-group stats count mismatch expected="
              + expectedStatsTargets.size()
              + " actual="
              + payload.getFileStatsCount());
    }
    Set<String> indexFiles = new HashSet<>();
    for (StatsObjectDescriptor artifact : payload.getIndexArtifactsList()) {
      String filePath = filePathFromIndexTarget(artifact.getTargetStorageId());
      if (!validObjectDescriptor(artifact, descriptor.statsObjectPrefix())
          || !successfulFiles.contains(filePath)
          || !indexFiles.add(filePath)) {
        throw new IllegalArgumentException("snapshot file-group index artifact metadata mismatch");
      }
    }
    validateIndexArtifactCoverage(capturePolicy.requestsIndexes(), successfulFiles, indexFiles);
    Set<String> statsTargets = new HashSet<>();
    List<StatsObjectDescriptor> fileStatsObjects = new ArrayList<>(payload.getFileStatsCount());
    for (StatsObjectDescriptor statsObject : payload.getFileStatsList()) {
      if (!validObjectDescriptor(statsObject, descriptor.statsObjectPrefix())
          || !statsTargets.add(statsObject.getTargetStorageId())) {
        throw new IllegalArgumentException("snapshot file-group stats coverage mismatch");
      }
      fileStatsObjects.add(statsObject);
    }
    if (!statsTargets.equals(expectedStatsTargets)) {
      throw new IllegalArgumentException("snapshot file-group stats do not cover successful files");
    }
    validatePartialAggregates(
        input,
        statsRequested,
        descriptor.succeededFileCount(),
        requestedAggregates,
        payload.getPartialAggregateRecordsList());
    return new ValidatedFileGroupArtifacts(
        payload.getPartialAggregateRecordsList(),
        fileStatsObjects,
        payload.getIndexArtifactsList());
  }

  static Set<String> expectedFileStatsTargets(
      ReconcileFileGroupTask plannedGroup, Set<String> successfulFiles) {
    Set<String> successful = successfulFiles == null ? Set.of() : Set.copyOf(successfulFiles);
    Set<String> expected = new HashSet<>();
    for (String filePath : successful) {
      expected.add(StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(filePath)));
    }
    if (plannedGroup != null) {
      for (var executionPlan : plannedGroup.fileExecutionPlans()) {
        if (!successful.contains(executionPlan.filePath())) {
          continue;
        }
        for (var deleteFile : executionPlan.icebergDeleteFiles()) {
          if (deleteFile != null && !deleteFile.filePath().isBlank()) {
            expected.add(
                StatsTargetIdentity.storageId(
                    StatsTargetIdentity.fileTarget(deleteFile.filePath())));
          }
        }
      }
    }
    return Set.copyOf(expected);
  }

  static void validateIndexArtifactCoverage(
      boolean indexesRequested, Set<String> successfulFiles, Set<String> indexFiles) {
    Set<String> successful = successfulFiles == null ? Set.of() : Set.copyOf(successfulFiles);
    Set<String> indexed = indexFiles == null ? Set.of() : Set.copyOf(indexFiles);
    if (indexesRequested && !indexed.equals(successful)) {
      throw new IllegalArgumentException(
          "snapshot file-group index artifacts do not cover successful files");
    }
    if (!indexesRequested && !indexed.isEmpty()) {
      throw new IllegalArgumentException(
          "snapshot file-group contains unrequested index artifacts");
    }
  }

  static void validatePartialAggregates(
      StandaloneSnapshotFinalizeExecutionPayload input,
      boolean statsRequested,
      int succeededFileCount,
      Set<FloecatConnector.StatsTargetKind> requested,
      List<TargetStatsRecord> partials) {
    if (!statsRequested || succeededFileCount == 0) {
      if (partials != null && !partials.isEmpty()) {
        throw new IllegalArgumentException(
            "empty or stats-disabled file group contains aggregate partials");
      }
      return;
    }

    boolean sawTable = false;
    Set<Long> columnIds = new HashSet<>();
    for (TargetStatsRecord record : partials == null ? List.<TargetStatsRecord>of() : partials) {
      if (record == null
          || !input.tableId().equals(record.getTableId())
          || input.snapshotId() != record.getSnapshotId()) {
        throw new IllegalArgumentException(
            "snapshot file-group aggregate partial identity mismatch");
      }
      if (record.hasTarget() && record.getTarget().hasTable() && record.hasTable()) {
        if (!requested.contains(FloecatConnector.StatsTargetKind.TABLE) || sawTable) {
          throw new IllegalArgumentException(
              "snapshot file-group aggregate partial target/value mismatch");
        }
        sawTable = true;
      } else if (record.hasTarget()
          && record.getTarget().hasColumn()
          && record.hasScalar()
          && record.getTarget().getColumn().getColumnId() > 0L) {
        long columnId = record.getTarget().getColumn().getColumnId();
        if (!requested.contains(FloecatConnector.StatsTargetKind.COLUMN)
            || !columnIds.add(columnId)) {
          throw new IllegalArgumentException(
              "snapshot file-group aggregate partial target/value mismatch");
        }
      } else {
        throw new IllegalArgumentException(
            "snapshot file-group aggregate partial target/value mismatch");
      }
    }

    if (sawTable != requested.contains(FloecatConnector.StatsTargetKind.TABLE)) {
      throw new IllegalArgumentException(
          "snapshot file-group table aggregate partial coverage mismatch");
    }
    if (!requested.contains(FloecatConnector.StatsTargetKind.COLUMN) && !columnIds.isEmpty()) {
      throw new IllegalArgumentException(
          "snapshot file-group contains unrequested column aggregate partials");
    }
  }

  private static boolean validObjectDescriptor(
      StatsObjectDescriptor descriptor, String requiredPrefix) {
    return descriptor != null
        && !descriptor.getTargetStorageId().isBlank()
        && !descriptor.getPayloadUri().isBlank()
        && descriptor.getPayloadUri().startsWith(requiredPrefix)
        && descriptor.getPayloadBytes() > 0L
        && descriptor.getPayloadSha256().size() == 32;
  }

  private static String filePathFromIndexTarget(String targetStorageId) {
    if (targetStorageId == null || !targetStorageId.startsWith("file:")) {
      return "";
    }
    return targetStorageId.substring("file:".length());
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

  private static List<TargetStatsRecord> emptySnapshotStats(
      ReconcileJobStore.LeasedJob lease, StandaloneSnapshotFinalizeExecutionPayload input) {
    if (!requestsStatsOutputs(lease)) {
      return List.of();
    }
    return List.of(
        TargetStatsRecords.tableRecord(
            input.tableId(),
            input.snapshotId(),
            TableValueStats.newBuilder()
                .setRowCount(0L)
                .setDataFileCount(0L)
                .setTotalSizeBytes(0L)
                .build(),
            null));
  }

  private static boolean requestsStatsOutputs(ReconcileJobStore.LeasedJob lease) {
    ReconcileCapturePolicy policy =
        lease == null || lease.scope == null
            ? ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    return policy.requestsStats();
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

  private static byte[] sha256(byte[] bytes, int offset, int length) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(bytes, offset, length);
      return digest.digest();
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

  private record ValidatedFileGroupArtifacts(
      List<TargetStatsRecord> partialAggregates,
      List<StatsObjectDescriptor> fileStats,
      List<StatsObjectDescriptor> indexArtifacts) {}
}
