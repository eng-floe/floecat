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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
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
        && task.fileGroupPlanRecorded()
        && (task.fileGroupCount() > 0 || task.sourceFileCount() > 0);
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
      SnapshotPlanBlobStore.SnapshotPlanBlob snapshotPlan =
          input.fileGroupCount() > 0 || input.sourceFileCount() > 0
              ? snapshotPlanBlobStore.loadPlan(input.snapshotPlanUri())
              : SnapshotPlanBlobStore.SnapshotPlanBlob.of(List.of());
      AppendOnlySnapshotBaseLoader.Loaded appendOnly =
          loadAppendOnlyBase(lease, input, snapshotPlan);
      List<ReconcileFileGroupResultDescriptor> descriptors;
      Map<GroupKey, ReconcileFileGroupTask> plannedGroups = Map.of();
      if (input.fileGroupCount() == 0) {
        if (input.sourceFileCount() != 0 && appendOnly == null) {
          throw new IllegalStateException(
              "explicit-empty snapshot finalizer has non-zero source file count "
                  + input.sourceFileCount());
        }
        descriptors = List.of();
      } else {
        plannedGroups = loadPlannedGroups(input, snapshotPlan);
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
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> reuseBundles =
          new ArrayList<>();
      Set<String> realizedStatsSelectors = new java.util.TreeSet<>();
      Set<String> realizedIndexSelectors = new java.util.TreeSet<>();
      Set<String> resolvedDefaultStatsSelectors = null;
      Set<String> resolvedDefaultIndexSelectors = null;
      ReconcileCapturePolicy capturePolicy =
          lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
      boolean defaultIndexSelection =
          capturePolicy.requestsIndexes()
              && capturePolicy.selectorsForIndex().isEmpty()
              && capturePolicy.defaultColumnScope()
                  != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
      boolean defaultStatsSelection =
          capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
              && capturePolicy.columns().stream()
                  .noneMatch(ReconcileCapturePolicy.Column::captureStats)
              && capturePolicy.defaultColumnScope()
                  != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
      if (appendOnly != null) {
        partials.addAll(appendOnly.aggregateRecords());
        realizedStatsSelectors.addAll(appendOnly.realizedStatsSelectors());
        realizedIndexSelectors.addAll(appendOnly.realizedIndexSelectors());
        if (defaultStatsSelection) {
          resolvedDefaultStatsSelectors =
              FileArtifactReuse.selectorIdentities(appendOnly.realizedStatsSelectors());
        }
        if (defaultIndexSelection) {
          resolvedDefaultIndexSelectors =
              defaultIndexSelectorIdentities(appendOnly.realizedIndexSelectors());
        }
      }
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
        if (artifacts.reusableArtifactBundle() != null) {
          reuseBundles.add(artifacts.reusableArtifactBundle());
        }
        if (defaultStatsSelection) {
          Set<String> groupSelectors =
              FileArtifactReuse.selectorIdentities(artifacts.realizedStatsSelectors());
          if (resolvedDefaultStatsSelectors == null) {
            resolvedDefaultStatsSelectors = groupSelectors;
          } else if (!resolvedDefaultStatsSelectors.equals(groupSelectors)) {
            throw new IllegalArgumentException(
                "snapshot file groups report inconsistent resolved default stats selectors");
          }
        }
        if (defaultIndexSelection) {
          Set<String> groupSelectors =
              defaultIndexSelectorIdentities(artifacts.realizedIndexSelectors());
          if (resolvedDefaultIndexSelectors == null) {
            resolvedDefaultIndexSelectors = groupSelectors;
          } else if (!resolvedDefaultIndexSelectors.equals(groupSelectors)) {
            throw new IllegalArgumentException(
                "snapshot file groups report inconsistent resolved default index selectors");
          }
        }
        realizedIndexSelectors.addAll(artifacts.realizedIndexSelectors());
        realizedStatsSelectors.addAll(artifacts.realizedStatsSelectors());
      }
      Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
      List<TargetStatsRecord> finalStats =
          input.fileGroupCount() == 0 && appendOnly == null
              ? emptySnapshotStats(lease, input)
              : FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
                  input.tableId(), input.snapshotId(), aggregateKinds, partials);
      if (context.shouldStop().getAsBoolean()) {
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
      }
      DeduplicatedSnapshotArtifacts deduplicated =
          deduplicateSnapshotArtifacts(fileStats, reuseBundles);
      List<StatsObjectDescriptor> uniqueFileStats = deduplicated.fileStats();
      reuseBundles = deduplicated.reuseBundles();
      validateRealizedSelectors(
          capturePolicy, input.sourceFileCount(), realizedStatsSelectors, realizedIndexSelectors);
      ReconcileSnapshotContentState.validateMaterializedStatsCoverage(
          lease.snapshotTask.requestedCoverage(),
          List.copyOf(realizedStatsSelectors),
          input.sourceFileCount());
      String resultId = resultId(lease, "success");
      RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess prepared =
          appendOnly == null
              ? workerClient.prepareSnapshotFinalizeSuccess(
                  remoteLease,
                  resultId,
                  input.statsObjectPrefix(),
                  input.durableCaptureManifestPrefix(),
                  input.reusableArtifactIndexObjectPrefix(),
                  input.statsGenerationManifestUri(),
                  input.indexGenerationCaptureManifestPrefix(),
                  input.sourceFileCount(),
                  descriptors,
                  uniqueFileStats,
                  finalStats,
                  List.of(),
                  reuseBundles,
                  List.copyOf(realizedStatsSelectors),
                  List.copyOf(realizedIndexSelectors),
                  input.indexPredecessor())
              : workerClient.prepareAppendOnlySnapshotFinalizeSuccess(
                  remoteLease,
                  resultId,
                  input.statsObjectPrefix(),
                  input.durableCaptureManifestPrefix(),
                  input.reusableArtifactIndexObjectPrefix(),
                  input.statsGenerationManifestUri(),
                  input.indexGenerationCaptureManifestPrefix(),
                  input.sourceFileCount(),
                  descriptors,
                  uniqueFileStats,
                  finalStats,
                  List.of(),
                  reuseBundles,
                  List.copyOf(realizedStatsSelectors),
                  List.copyOf(realizedIndexSelectors),
                  input.indexPredecessor(),
                  appendOnly.base());
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
            remoteLease, resultId(lease, "failure"), message, failureKind(error));
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
      workerClient.submitSnapshotFinalizeFailure(
          remoteLease, resultId(lease, "failure"), message, failureKind(error));
      return ExecutionResult.failure(0, 0, 0, 0, 1, 0, 0, message, error);
    }
  }

  private AppendOnlySnapshotBaseLoader.Loaded loadAppendOnlyBase(
      ReconcileJobStore.LeasedJob lease,
      StandaloneSnapshotFinalizeExecutionPayload input,
      SnapshotPlanBlobStore.SnapshotPlanBlob snapshotPlan) {
    SnapshotPlanBlobStore.AppendOnlyBase base;
    try {
      base = snapshotPlan.appendOnlyBase().orElse(null);
    } catch (AppendOnlyBaseCompatibilityException error) {
      throw error;
    } catch (IllegalArgumentException error) {
      // A durable plan written against an older contract can never be replayed as-is.
      throw new AppendOnlyBaseCompatibilityException(
          "append-only base is incompatible; full capture required", error);
    }
    // The loader raises AppendOnlyBaseCompatibilityException for contract violations and leaves
    // storage failures as their own retryable exceptions.
    return new AppendOnlySnapshotBaseLoader(blobStore).load(lease, input, base);
  }

  private static ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest
          .FailureKind
      failureKind(RuntimeException error) {
    return error instanceof AppendOnlyBaseCompatibilityException
        ? ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
            .SFFK_APPEND_ONLY_BASE_INCOMPATIBLE
        : ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
            .SFFK_UNSPECIFIED;
  }

  private Map<GroupKey, ReconcileFileGroupTask> loadPlannedGroups(
      StandaloneSnapshotFinalizeExecutionPayload input,
      SnapshotPlanBlobStore.SnapshotPlanBlob snapshotPlan) {
    List<ReconcileFileGroupTask> plannedGroups = snapshotPlan.fileGroups();
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
    int inheritedFileCount =
        snapshotPlan
            .appendOnlyBase()
            .map(SnapshotPlanBlobStore.AppendOnlyBase::sourceFileCount)
            .orElse(0);
    validatePlannedFileCoverage(plannedGroups, inheritedFileCount, input.sourceFileCount());
    return groupsByKey;
  }

  static void validatePlannedFileCoverage(
      List<ReconcileFileGroupTask> plannedGroups, int inheritedFileCount, int sourceFileCount) {
    Set<String> plannedFilePaths = new HashSet<>();
    for (ReconcileFileGroupTask plannedGroup : plannedGroups) {
      for (String filePath : plannedGroup.filePaths()) {
        if (!plannedFilePaths.add(filePath)) {
          throw new IllegalStateException(
              "snapshot plan assigns a file to more than one group: " + filePath);
        }
      }
    }
    if (Math.addExact(plannedFilePaths.size(), inheritedFileCount) != sourceFileCount) {
      throw new IllegalStateException(
          "snapshot plan file identities do not cover the declared source files");
    }
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
    List<String> realizedIndexSelectors =
        payload.getRealizedIndexSelectorsList().stream()
            .filter(selector -> selector != null && !selector.isBlank())
            .map(String::trim)
            .distinct()
            .sorted()
            .toList();
    List<String> realizedStatsSelectors =
        payload.getRealizedStatsSelectorsList().stream()
            .filter(selector -> selector != null && !selector.isBlank())
            .map(String::trim)
            .distinct()
            .sorted()
            .toList();
    validateRealizedStatsSelectors(
        capturePolicy, descriptor.succeededFileCount(), realizedStatsSelectors);
    if (capturePolicy.requestsIndexes() && realizedIndexSelectors.isEmpty()) {
      throw new IllegalArgumentException(
          "snapshot file-group result does not report realized index selectors");
    }
    if (capturePolicy.requestsIndexes()
        && !realizedIndexSelectors.containsAll(capturePolicy.selectorsForIndex())) {
      throw new IllegalArgumentException(
          "snapshot file-group result does not cover explicitly requested index selectors");
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
    ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference reuseBundle =
        payload.hasReusableArtifactBundle() ? payload.getReusableArtifactBundle() : null;
    if (reuseBundle == null
        || !reuseBundle.hasArtifact()
        || reuseBundle.getArtifact().getPayloadUri().isBlank()
        || reuseBundle.getArtifact().getPayloadBytes() <= 0
        || reuseBundle.getArtifact().getPayloadSha256().size() != 32
        || !reuseBundle.getArtifact().getPayloadUri().startsWith(descriptor.statsObjectPrefix())) {
      throw new IllegalArgumentException("snapshot file-group reuse bundle metadata mismatch");
    }
    validateReuseBundleArtifact(
        reuseBundle,
        java.util.stream.Stream.concat(
                fileStatsObjects.stream(), payload.getIndexArtifactsList().stream())
            .toList());
    Set<String> reusableStatsFiles =
        reuseBundle.getFileStatsList().stream()
            .map(ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata::getFilePath)
            .filter(path -> path != null && !path.isBlank())
            .collect(java.util.stream.Collectors.toSet());
    Set<String> expectedReusableStatsFiles =
        statsRequested ? expectedFileStatsPaths(plannedGroup, successfulFiles) : Set.of();
    if (reuseBundle.getFileStatsCount() != fileStatsObjects.size()
        || reusableStatsFiles.size() != reuseBundle.getFileStatsCount()
        || !reusableStatsFiles.equals(expectedReusableStatsFiles)) {
      throw new IllegalArgumentException(
          "snapshot file-group reuse bundle stats do not match published references");
    }
    Set<String> reusableIndexFiles =
        reuseBundle.getIndexArtifactsList().stream()
            .map(ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata::getFilePath)
            .filter(path -> path != null && !path.isBlank())
            .collect(java.util.stream.Collectors.toSet());
    if (reuseBundle.getIndexArtifactsCount() != payload.getIndexArtifactsCount()
        || reusableIndexFiles.size() != reuseBundle.getIndexArtifactsCount()
        || !reusableIndexFiles.equals(indexFiles)) {
      throw new IllegalArgumentException(
          "snapshot file-group reuse bundle indexes do not match published references");
    }
    validateReuseMetadata(
        capturePolicy,
        plannedGroup.fileExecutionPlans(),
        reuseBundle,
        realizedStatsSelectors,
        realizedIndexSelectors);
    validatePartialAggregates(
        input,
        statsRequested,
        descriptor.succeededFileCount(),
        requestedAggregates,
        payload.getPartialAggregateRecordsList());
    return new ValidatedFileGroupArtifacts(
        payload.getPartialAggregateRecordsList(),
        fileStatsObjects,
        payload.getIndexArtifactsList(),
        reuseBundle,
        realizedStatsSelectors,
        realizedIndexSelectors);
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
        var deletionVector = executionPlan.deletionVector();
        if (deletionVector != null && deletionVector.onDisk()) {
          expected.add(
              StatsTargetIdentity.storageId(
                  StatsTargetIdentity.fileTarget(deletionVector.pathOrInlineDv())));
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

  private static Set<String> expectedFileStatsPaths(
      ReconcileFileGroupTask plannedGroup, Set<String> successfulFiles) {
    Set<String> successful = successfulFiles == null ? Set.of() : Set.copyOf(successfulFiles);
    Set<String> expected = new HashSet<>(successful);
    if (plannedGroup != null) {
      for (var executionPlan : plannedGroup.fileExecutionPlans()) {
        if (!successful.contains(executionPlan.filePath())) {
          continue;
        }
        var deletionVector = executionPlan.deletionVector();
        if (deletionVector != null && deletionVector.onDisk()) {
          expected.add(deletionVector.pathOrInlineDv());
        }
        for (var deleteFile : executionPlan.icebergDeleteFiles()) {
          if (deleteFile != null && !deleteFile.filePath().isBlank()) {
            expected.add(deleteFile.filePath());
          }
        }
      }
    }
    return Set.copyOf(expected);
  }

  static List<StatsObjectDescriptor> deduplicateSnapshotFileStats(
      List<StatsObjectDescriptor> descriptors) {
    if (descriptors == null || descriptors.isEmpty()) {
      return List.of();
    }
    Map<String, StatsObjectDescriptor> byTarget = new java.util.TreeMap<>();
    for (StatsObjectDescriptor descriptor : descriptors) {
      if (descriptor == null || descriptor.getTargetStorageId().isBlank()) {
        throw new IllegalArgumentException("invalid snapshot file stats descriptor");
      }
      StatsObjectDescriptor existing =
          byTarget.putIfAbsent(descriptor.getTargetStorageId(), descriptor);
      if (existing == null) {
        continue;
      }
      if (existing.getPayloadBytes() != descriptor.getPayloadBytes()
          || !MessageDigest.isEqual(
              existing.getPayloadSha256().toByteArray(),
              descriptor.getPayloadSha256().toByteArray())) {
        throw new IllegalArgumentException(
            "conflicting snapshot file stats for target " + descriptor.getTargetStorageId());
      }
      if (descriptor.getPayloadUri().compareTo(existing.getPayloadUri()) < 0) {
        byTarget.put(descriptor.getTargetStorageId(), descriptor);
      }
    }
    return List.copyOf(byTarget.values());
  }

  static DeduplicatedSnapshotArtifacts deduplicateSnapshotArtifacts(
      List<StatsObjectDescriptor> descriptors,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> bundles) {
    List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> references =
        bundles == null ? List.of() : List.copyOf(bundles);
    Map<String, Map<String, ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata>>
        metadataByPayload = new HashMap<>();
    Map<String, Map<String, Integer>> ownerByPayload = new HashMap<>();
    for (int bundleIndex = 0; bundleIndex < references.size(); bundleIndex++) {
      var bundle = references.get(bundleIndex);
      if (bundle == null
          || !bundle.hasArtifact()
          || bundle.getArtifact().getPayloadUri().isBlank()) {
        throw new IllegalArgumentException("invalid snapshot reusable artifact bundle");
      }
      Map<String, ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata> byTarget =
          metadataByPayload.computeIfAbsent(
              bundle.getArtifact().getPayloadUri(), ignored -> new HashMap<>());
      Map<String, Integer> owners =
          ownerByPayload.computeIfAbsent(
              bundle.getArtifact().getPayloadUri(), ignored -> new HashMap<>());
      Set<String> bundleTargets = new HashSet<>();
      for (var metadata : bundle.getFileStatsList()) {
        if (metadata.getFilePath().isBlank()) {
          throw new IllegalArgumentException("invalid snapshot reusable stats metadata");
        }
        String target =
            StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(metadata.getFilePath()));
        if (!bundleTargets.add(target)) {
          throw new IllegalArgumentException(
              "duplicate reusable stats metadata for target " + target);
        }
        var existing = byTarget.putIfAbsent(target, metadata);
        if (existing != null && !equivalentReusableStatsMetadata(existing, metadata)) {
          throw new IllegalArgumentException(
              "conflicting reusable stats metadata for target " + target);
        }
        Integer existingOwner = owners.putIfAbsent(target, bundleIndex);
        if (existingOwner != null
            && bundle
                    .getArtifact()
                    .getTargetStorageId()
                    .compareTo(references.get(existingOwner).getArtifact().getTargetStorageId())
                < 0) {
          owners.put(target, bundleIndex);
        }
      }
    }

    Map<String, StatsObjectDescriptor> byTarget = new java.util.TreeMap<>();
    for (StatsObjectDescriptor descriptor :
        descriptors == null ? List.<StatsObjectDescriptor>of() : descriptors) {
      if (descriptor == null || descriptor.getTargetStorageId().isBlank()) {
        throw new IllegalArgumentException("invalid snapshot file stats descriptor");
      }
      StatsObjectDescriptor existing =
          byTarget.putIfAbsent(descriptor.getTargetStorageId(), descriptor);
      if (existing == null) {
        continue;
      }
      boolean samePayload =
          existing.getPayloadBytes() == descriptor.getPayloadBytes()
              && MessageDigest.isEqual(
                  existing.getPayloadSha256().toByteArray(),
                  descriptor.getPayloadSha256().toByteArray());
      if (!samePayload) {
        var existingMetadata =
            metadataByPayload
                .getOrDefault(existing.getPayloadUri(), Map.of())
                .get(descriptor.getTargetStorageId());
        var candidateMetadata =
            metadataByPayload
                .getOrDefault(descriptor.getPayloadUri(), Map.of())
                .get(descriptor.getTargetStorageId());
        if (!equivalentReusableStatsMetadata(existingMetadata, candidateMetadata)) {
          throw new IllegalArgumentException(
              "conflicting snapshot file stats for target " + descriptor.getTargetStorageId());
        }
      }
      if (descriptor.getPayloadUri().compareTo(existing.getPayloadUri()) < 0) {
        byTarget.put(descriptor.getTargetStorageId(), descriptor);
      }
    }

    Map<String, Integer> ownerByTarget = new HashMap<>();
    for (var entry : byTarget.entrySet()) {
      String target = entry.getKey();
      String payloadUri = entry.getValue().getPayloadUri();
      Integer owner = ownerByPayload.getOrDefault(payloadUri, Map.of()).get(target);
      if (owner == null) {
        throw new IllegalArgumentException(
            "snapshot reusable bundle metadata is missing target " + target);
      }
      ownerByTarget.put(target, owner);
    }

    List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> normalized =
        new ArrayList<>(references.size());
    for (int index = 0; index < references.size(); index++) {
      var bundle = references.get(index);
      var builder = bundle.toBuilder().clearFileStats();
      for (var metadata : bundle.getFileStatsList()) {
        String target =
            StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(metadata.getFilePath()));
        Integer owner = ownerByTarget.get(target);
        if (owner != null && owner == index) {
          builder.addFileStats(metadata);
        }
      }
      normalized.add(builder.build());
    }
    return new DeduplicatedSnapshotArtifacts(
        List.copyOf(byTarget.values()), List.copyOf(normalized));
  }

  private static boolean equivalentReusableStatsMetadata(
      ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata first,
      ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata second) {
    return first != null
        && second != null
        && !first.getSourceFingerprint().isBlank()
        && !first.getStatsCaptureSignature().isBlank()
        && first.getFilePath().equals(second.getFilePath())
        && first.getSourceFingerprint().equals(second.getSourceFingerprint())
        && first.getStatsCaptureSignature().equals(second.getStatsCaptureSignature())
        && Set.copyOf(first.getRealizedStatsSelectorsList())
            .equals(Set.copyOf(second.getRealizedStatsSelectorsList()));
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

  static Set<String> defaultIndexSelectorIdentities(java.util.Collection<String> selectors) {
    return FileArtifactReuse.selectorIdentities(selectors);
  }

  private record ExpectedReuseMetadata(String sourceFingerprint, String captureSignature) {}

  static void validateReuseMetadata(
      ReconcileCapturePolicy capturePolicy,
      List<ReconcileFileExecutionPlan> executionPlans,
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference reuseBundle,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors) {
    ReconcileCapturePolicy policy =
        capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    Map<String, ExpectedReuseMetadata> expectedStats = new HashMap<>();
    Map<String, ExpectedReuseMetadata> expectedIndexes = new HashMap<>();
    for (ReconcileFileExecutionPlan plan :
        executionPlans == null ? List.<ReconcileFileExecutionPlan>of() : executionPlans) {
      if (policy.requestsStats()) {
        putExpectedReuseMetadata(
            expectedStats,
            plan.filePath(),
            plan.sourceFingerprint(),
            plan.statsCaptureSignature(),
            "stats");
        for (var auxiliary : plan.auxiliaryStatsFingerprints().entrySet()) {
          putExpectedReuseMetadata(
              expectedStats,
              auxiliary.getKey(),
              auxiliary.getValue(),
              plan.statsCaptureSignature(),
              "stats");
        }
      }
      if (policy.requestsIndexes()) {
        putExpectedReuseMetadata(
            expectedIndexes,
            plan.filePath(),
            plan.indexSourceFingerprint(),
            plan.indexCaptureSignature(),
            "index");
      }
    }
    Set<String> groupStats = Set.copyOf(realizedStatsSelectors);
    Set<String> groupIndexes = Set.copyOf(realizedIndexSelectors);
    Set<String> metadataStats = new HashSet<>();
    Set<String> metadataIndexes = new HashSet<>();
    boolean defaultStatsSelection =
        policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
            && policy.selectorsForStats().isEmpty()
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    boolean defaultIndexSelection =
        policy.requestsIndexes()
            && policy.selectorsForIndex().isEmpty()
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    for (var metadata : reuseBundle.getFileStatsList()) {
      ExpectedReuseMetadata expected = expectedStats.get(metadata.getFilePath());
      if (expected == null
          || expected.sourceFingerprint().isBlank()
          || expected.captureSignature().isBlank()
          || !expected.sourceFingerprint().equals(metadata.getSourceFingerprint())
          || !expected.captureSignature().equals(metadata.getStatsCaptureSignature())) {
        throw new IllegalArgumentException(
            "snapshot file-group reuse stats metadata does not match the immutable plan");
      }
      Set<String> metadataSelectors = Set.copyOf(metadata.getRealizedStatsSelectorsList());
      metadataStats.addAll(metadataSelectors);
      if (!groupStats.containsAll(metadataSelectors)
          || (policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
              && !FileArtifactReuse.coversExplicitSelectors(
                  metadataSelectors, policy.selectorsForStats()))
          || (defaultStatsSelection && metadataSelectors.isEmpty())
          || (defaultStatsSelection
              && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
              && FileArtifactReuse.realizedColumnCount(metadataSelectors)
                  > policy.maxDefaultColumns())) {
        throw new IllegalArgumentException(
            "snapshot file-group reuse stats selectors do not cover the capture policy");
      }
    }
    for (var metadata : reuseBundle.getIndexArtifactsList()) {
      ExpectedReuseMetadata expected = expectedIndexes.get(metadata.getFilePath());
      if (expected == null
          || expected.sourceFingerprint().isBlank()
          || expected.captureSignature().isBlank()
          || !expected.sourceFingerprint().equals(metadata.getSourceFingerprint())
          || !expected.captureSignature().equals(metadata.getIndexCaptureSignature())) {
        throw new IllegalArgumentException(
            "snapshot file-group reuse index metadata does not match the immutable plan");
      }
      Set<String> metadataSelectors = Set.copyOf(metadata.getRealizedIndexSelectorsList());
      metadataIndexes.addAll(metadataSelectors);
      if (!groupIndexes.containsAll(metadataSelectors)
          || !FileArtifactReuse.coversExplicitSelectors(
              metadataSelectors, policy.selectorsForIndex())
          || (defaultIndexSelection && metadataSelectors.isEmpty())
          || (defaultIndexSelection
              && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
              && FileArtifactReuse.realizedColumnCount(metadataSelectors)
                  > policy.maxDefaultColumns())) {
        throw new IllegalArgumentException(
            "snapshot file-group reuse index selectors do not cover the capture policy");
      }
    }
    if (!metadataStats.equals(groupStats) || !metadataIndexes.equals(groupIndexes)) {
      throw new IllegalArgumentException(
          "snapshot file-group reuse selector metadata does not match the file-group result");
    }
  }

  private static void putExpectedReuseMetadata(
      Map<String, ExpectedReuseMetadata> expected,
      String filePath,
      String sourceFingerprint,
      String captureSignature,
      String kind) {
    ExpectedReuseMetadata value = new ExpectedReuseMetadata(sourceFingerprint, captureSignature);
    ExpectedReuseMetadata prior = expected.putIfAbsent(filePath, value);
    if (filePath == null
        || filePath.isBlank()
        || sourceFingerprint == null
        || sourceFingerprint.isBlank()
        || captureSignature == null
        || captureSignature.isBlank()
        || (prior != null && !prior.equals(value))) {
      throw new IllegalArgumentException(
          "snapshot file-group has conflicting immutable " + kind + " reuse metadata");
    }
  }

  static void validateRealizedSelectors(
      ReconcileCapturePolicy policy,
      int sourceFileCount,
      Set<String> realizedStatsSelectors,
      Set<String> realizedIndexSelectors) {
    ReconcileCapturePolicy effective = policy == null ? ReconcileCapturePolicy.empty() : policy;
    Set<String> realizedIndexes =
        realizedIndexSelectors == null ? Set.of() : Set.copyOf(realizedIndexSelectors);
    Set<String> realizedStats =
        realizedStatsSelectors == null ? Set.of() : Set.copyOf(realizedStatsSelectors);
    if (!effective.requestsIndexes()) {
      if (!realizedIndexes.isEmpty()) {
        throw new IllegalArgumentException("snapshot contains unrequested index selectors");
      }
    } else if (sourceFileCount > 0) {
      Set<String> required = effective.selectorsForIndex();
      boolean defaultSelection =
          required.isEmpty()
              && effective.defaultColumnScope()
                  != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
      if (!required.isEmpty() && !realizedIndexes.containsAll(required)) {
        throw new IllegalArgumentException(
            "snapshot does not cover explicitly requested index selectors");
      }
      if (defaultSelection && realizedIndexes.isEmpty()) {
        throw new IllegalArgumentException(
            "snapshot does not report resolved default index selectors");
      }
      if (defaultSelection
          && effective.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
          && realizedColumnCount(realizedIndexes) > effective.maxDefaultColumns()) {
        throw new IllegalArgumentException("snapshot exceeds the requested default index limit");
      }
    }
    if (!effective.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      if (!realizedStats.isEmpty()) {
        throw new IllegalArgumentException("snapshot contains unrequested stats selectors");
      }
      return;
    }
    validateRealizedStatsSelectors(effective, sourceFileCount, realizedStats);
  }

  private static void validateRealizedStatsSelectors(
      ReconcileCapturePolicy policy,
      int sourceFileCount,
      java.util.Collection<String> realizedStatsSelectors) {
    Set<String> realizedStats =
        realizedStatsSelectors == null ? Set.of() : Set.copyOf(realizedStatsSelectors);
    if (!policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      if (!realizedStats.isEmpty()) {
        throw new IllegalArgumentException("snapshot contains unrequested stats selectors");
      }
      return;
    }
    Set<String> requiredStats = policy.selectorsForStats();
    boolean defaultStatsSelection =
        requiredStats.isEmpty()
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    if (sourceFileCount > 0) {
      if (!requiredStats.isEmpty()
          && !FileArtifactReuse.coversExplicitSelectors(realizedStats, requiredStats)) {
        throw new IllegalArgumentException(
            "snapshot does not cover explicitly requested stats selectors");
      }
      if (defaultStatsSelection && realizedStats.isEmpty()) {
        throw new IllegalArgumentException(
            "snapshot does not report resolved default stats selectors");
      }
      if (defaultStatsSelection
          && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
          && realizedColumnCount(realizedStats) > policy.maxDefaultColumns()) {
        throw new IllegalArgumentException("snapshot exceeds the requested default stats limit");
      }
    }
  }

  static int realizedColumnCount(Set<String> selectors) {
    return FileArtifactReuse.realizedColumnCount(selectors);
  }

  static void validateReuseBundleArtifact(
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference reuseBundle,
      List<StatsObjectDescriptor> committedDescriptors) {
    var artifact = reuseBundle.getArtifact();
    for (StatsObjectDescriptor descriptor :
        committedDescriptors == null ? List.<StatsObjectDescriptor>of() : committedDescriptors) {
      if (!artifact.getPayloadUri().equals(descriptor.getPayloadUri())
          || artifact.getPayloadBytes() != descriptor.getPayloadBytes()
          || !MessageDigest.isEqual(
              artifact.getPayloadSha256().toByteArray(),
              descriptor.getPayloadSha256().toByteArray())) {
        throw new IllegalArgumentException(
            "snapshot file-group reuse bundle does not match committed artifact descriptors");
      }
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

  private record GroupKey(String planId, String groupId) {
    private GroupKey {
      planId = planId == null ? "" : planId.trim();
      groupId = groupId == null ? "" : groupId.trim();
    }
  }

  record DeduplicatedSnapshotArtifacts(
      List<StatsObjectDescriptor> fileStats,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> reuseBundles) {}

  private record ValidatedFileGroupArtifacts(
      List<TargetStatsRecord> partialAggregates,
      List<StatsObjectDescriptor> fileStats,
      List<StatsObjectDescriptor> indexArtifacts,
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference reusableArtifactBundle,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors) {}
}
