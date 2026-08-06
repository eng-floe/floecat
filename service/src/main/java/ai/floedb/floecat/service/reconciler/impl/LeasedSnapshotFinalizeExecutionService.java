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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.FileArtifactReuse;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundles;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultResponse;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.types.Hashing;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

/** Validates and publishes fenced snapshot capture artifacts. */
@ApplicationScoped
public class LeasedSnapshotFinalizeExecutionService extends BaseServiceImpl {
  private static final Logger LOG = Logger.getLogger(LeasedSnapshotFinalizeExecutionService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  @Inject SnapshotFinalizeChildStateService childStateService;
  @Inject CurrentSnapshotPointerService currentSnapshotPointerService;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject IndexArtifactRepository indexArtifactRepository;
  @Inject StatsStore statsStore;
  @Inject BlobStore blobStore;
  @Inject SnapshotRepository snapshotRepo;

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      SnapshotCaptureManifestDescriptor descriptor) {
    long totalStartNanos = System.nanoTime();
    long[] leaseNanos = {0L};
    long[] validateNanos = {0L};
    long[] manifestValidationNanos = {0L};
    long[] childScanNanos = {0L};
    long[] commitNanos = {0L};
    long[] publishNanos = {0L};
    long[] leaseOutcomeNanos = {0L};
    long[] rpcRequestBytes = {0L};
    long[] manifestBytes = {descriptor == null ? 0L : descriptor.getManifestBytes()};
    String[] outcome = {"failed"};
    try {
      String requiredResultId = requireResultId(resultId);
      String manifestSha256 =
          descriptor == null
              ? ""
              : HexFormat.of().formatHex(descriptor.getManifestSha256().toByteArray());
      ReconcileJobStore.ReconcileJob existing = jobs.getCompactLeaseView(jobId).orElse(null);
      if (existing != null && "JS_SUCCEEDED".equals(existing.state)) {
        boolean replayed =
            jobs.completeSnapshotFinalizeSuccess(
                jobId,
                leaseEpoch,
                requiredResultId,
                descriptor == null ? "" : descriptor.getManifestUri(),
                descriptor == null ? 0L : descriptor.getManifestBytes(),
                manifestSha256,
                descriptor == null ? 0 : descriptor.getFileGroupCount(),
                descriptor == null ? 0 : descriptor.getSourceFileCount(),
                descriptor == null ? 0L : descriptor.getStatsRecordCount(),
                descriptor == null ? 0L : descriptor.getIndexArtifactCount(),
                null,
                System.currentTimeMillis(),
                "Registered snapshot capture manifest");
        requireAcceptedLeaseOutcome(replayed, jobId);
        outcome[0] = "replayed";
        return true;
      }
      long leaseStartNanos = System.nanoTime();
      ReconcileJobStore.LeasedJob lease;
      try {
        lease =
            requireLeasedSnapshotFinalizeJob(
                principalContext.getCorrelationId(), jobId, leaseEpoch);
      } finally {
        leaseNanos[0] = System.nanoTime() - leaseStartNanos;
      }

      long validateStartNanos = System.nanoTime();
      ReconcileSnapshotTask snapshotTask;
      ResourceId tableId;
      SnapshotCaptureManifestDescriptor validated;
      try {
        snapshotTask = requireSnapshotTask(lease);
        tableId = tableId(lease, snapshotTask);
        validated =
            validateManifestDescriptorIdentity(lease, snapshotTask, requiredResultId, descriptor);
      } finally {
        validateNanos[0] = System.nanoTime() - validateStartNanos;
      }
      long manifestValidationStartNanos = System.nanoTime();
      ValidatedCaptureManifest validatedManifest;
      try {
        validatedManifest = validateManifestObject(lease, snapshotTask, validated);
      } finally {
        manifestValidationNanos[0] = System.nanoTime() - manifestValidationStartNanos;
      }
      SnapshotCaptureManifest manifest = validatedManifest.manifest();
      var successPayload = successPayload(requiredResultId, validated);
      rpcRequestBytes[0] =
          SubmitLeasedSnapshotFinalizeResultRequest.newBuilder()
              .setJobId(jobId)
              .setLeaseEpoch(leaseEpoch)
              .setSuccess(successPayload)
              .build()
              .getSerializedSize();

      long commitStartNanos = System.nanoTime();
      boolean accepted = false;
      try {
        long childStartNanos = System.nanoTime();
        try {
          requireReadyChildState(lease, snapshotTask);
        } finally {
          childScanNanos[0] = System.nanoTime() - childStartNanos;
        }
        requireAcceptedLeaseOutcome(
            jobs.beginSnapshotFinalizeCommit(lease.jobId, lease.leaseEpoch), lease.jobId);
        long publishStartNanos = System.nanoTime();
        try {
          byte[] manifestDigest = validated.getManifestSha256().toByteArray();
          String durableManifestUri =
              Keys.reconcileSnapshotDurableCaptureManifestUri(
                  tableId.getAccountId(),
                  tableId.getId(),
                  snapshotTask.snapshotId(),
                  lease.parentJobId,
                  manifestDigest);
          blobStore.put(
              durableManifestUri, validatedManifest.serializedBytes(), "application/x-protobuf");
          publishCaptureArtifacts(lease, tableId, snapshotTask, manifest);
          snapshotRepo.recordReuseManifest(
              tableId,
              snapshotTask.snapshotId(),
              durableManifestUri,
              validated.getManifestBytes(),
              manifestDigest,
              Keys.snapshotTargetStatsManifestBlobUri(
                  tableId.getAccountId(),
                  tableId.getId(),
                  snapshotTask.snapshotId(),
                  "full-rescan-" + lease.parentJobId));
          currentSnapshotPointerService.maybeAdvance(
              tableId, snapshotTask.snapshotId(), lease.jobId);
        } finally {
          publishNanos[0] = System.nanoTime() - publishStartNanos;
        }
        long leaseOutcomeStartNanos = System.nanoTime();
        try {
          accepted =
              jobs.completeSnapshotFinalizeSuccess(
                  lease.jobId,
                  lease.leaseEpoch,
                  requiredResultId,
                  validated.getManifestUri(),
                  validated.getManifestBytes(),
                  manifestSha256,
                  validated.getFileGroupCount(),
                  validated.getSourceFileCount(),
                  validated.getStatsRecordCount(),
                  validated.getIndexArtifactCount(),
                  ReconcileSnapshotContentState.materializedCoverage(
                      snapshotTask.requestedCoverage(),
                      manifest.getRealizedStatsSelectorsList(),
                      manifest.getRealizedIndexSelectorsList(),
                      manifest.getSourceFileCount()),
                  System.currentTimeMillis(),
                  "Registered snapshot capture manifest " + snapshotTask.snapshotId());
          requireAcceptedLeaseOutcome(accepted, lease.jobId);
        } finally {
          leaseOutcomeNanos[0] = System.nanoTime() - leaseOutcomeStartNanos;
        }
      } finally {
        commitNanos[0] = System.nanoTime() - commitStartNanos;
      }
      outcome[0] = accepted ? "accepted" : "rejected";
      return accepted;
    } finally {
      logFinalizeTiming(
          jobId,
          outcome[0],
          totalStartNanos,
          leaseNanos[0],
          validateNanos[0],
          manifestValidationNanos[0],
          childScanNanos[0],
          commitNanos[0],
          publishNanos[0],
          leaseOutcomeNanos[0],
          rpcRequestBytes[0],
          manifestBytes[0]);
    }
  }

  private static void logFinalizeTiming(
      String jobId,
      String outcome,
      long totalStartNanos,
      long leaseNanos,
      long validateNanos,
      long manifestValidationNanos,
      long childScanNanos,
      long commitNanos,
      long publishNanos,
      long leaseOutcomeNanos,
      long rpcRequestBytes,
      long manifestBytes) {
    long totalNanos = System.nanoTime() - totalStartNanos;
    long accountedNanos = leaseNanos + validateNanos + manifestValidationNanos + commitNanos;
    long otherNanos = Math.max(0L, totalNanos - accountedNanos);
    LOG.infof(
        "snapshot_finalize_submission_timing jobId=%s outcome=%s totalMs=%.3f leaseMs=%.3f"
            + " validateMs=%.3f manifestValidationMs=%.3f commitMs=%.3f childScanMs=%.3f"
            + " publishMs=%.3f leaseOutcomeMs=%.3f otherMs=%.3f rpcRequestBytes=%d"
            + " manifestBytes=%d",
        jobId,
        outcome,
        totalNanos / 1_000_000.0,
        leaseNanos / 1_000_000.0,
        validateNanos / 1_000_000.0,
        manifestValidationNanos / 1_000_000.0,
        commitNanos / 1_000_000.0,
        childScanNanos / 1_000_000.0,
        publishNanos / 1_000_000.0,
        leaseOutcomeNanos / 1_000_000.0,
        otherNanos / 1_000_000.0,
        rpcRequestBytes,
        manifestBytes);
  }

  private SnapshotCaptureManifestDescriptor validateManifestDescriptorIdentity(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      String resultId,
      SnapshotCaptureManifestDescriptor descriptor) {
    if (descriptor == null || descriptor.getFormatVersion() != 1) {
      throw new IllegalArgumentException("snapshot capture manifest format_version must be 1");
    }
    String expectedUri =
        Keys.reconcileSnapshotCaptureManifestUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    if (!lease.accountId.equals(descriptor.getAccountId())
        || !lease.connectorId.equals(descriptor.getConnectorId())
        || !lease.parentJobId.equals(descriptor.getParentJobId())
        || !lease.jobId.equals(descriptor.getFinalizeJobId())
        || !snapshotTask.tableId().equals(descriptor.getTableId())
        || snapshotTask.snapshotId() != descriptor.getSnapshotId()
        || !lease.leaseEpoch.equals(descriptor.getLeaseEpoch())
        || !resultId.equals(descriptor.getResultId())) {
      throw new IllegalArgumentException("snapshot capture manifest descriptor identity mismatch");
    }
    if (!expectedUri.equals(descriptor.getManifestUri())) {
      throw new IllegalArgumentException(
          "snapshot capture manifest URI is outside the leased result location");
    }
    if (descriptor.getManifestBytes() <= 0L || descriptor.getManifestSha256().size() != 32) {
      throw new IllegalArgumentException("snapshot capture manifest size and sha256 are required");
    }
    if (descriptor.getFileGroupCount() != snapshotTask.fileGroupCount()
        || descriptor.getSourceFileCount() != snapshotTask.sourceFileCount()) {
      throw new IllegalArgumentException("snapshot capture manifest coverage mismatch");
    }
    return descriptor;
  }

  private ValidatedCaptureManifest validateManifestObject(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      SnapshotCaptureManifestDescriptor descriptor) {
    String expectedUri = descriptor.getManifestUri();
    byte[] bytes =
        loadRequiredPublishedObject(expectedUri, "snapshot capture manifest jobId=" + lease.jobId);
    if (bytes.length != descriptor.getManifestBytes()) {
      throw new IllegalArgumentException("snapshot capture manifest object size mismatch");
    }
    if (!MessageDigest.isEqual(sha256(bytes), descriptor.getManifestSha256().toByteArray())) {
      throw new IllegalArgumentException("snapshot capture manifest object sha256 mismatch");
    }
    SnapshotCaptureManifest manifest;
    try {
      manifest = SnapshotCaptureManifest.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("snapshot capture manifest object is invalid", e);
    }
    if (manifest.getFormatVersion() != 1
        || !descriptor.getAccountId().equals(manifest.getAccountId())
        || !descriptor.getConnectorId().equals(manifest.getConnectorId())
        || !descriptor.getParentJobId().equals(manifest.getParentJobId())
        || !descriptor.getFinalizeJobId().equals(manifest.getFinalizeJobId())
        || !descriptor.getTableId().equals(manifest.getTableId())
        || descriptor.getSnapshotId() != manifest.getSnapshotId()
        || !descriptor.getLeaseEpoch().equals(manifest.getLeaseEpoch())
        || !descriptor.getResultId().equals(manifest.getResultId())
        || descriptor.getFileGroupCount() != manifest.getFileGroupsCount()
        || descriptor.getSourceFileCount() != manifest.getSourceFileCount()
        || descriptor.getStatsRecordCount()
            != manifest.getFileStatsRecordCount() + manifest.getFinalStatsRecordCount()
        || manifest.getFinalStatsRecordCount() != manifest.getFinalStatsCount()
        || descriptor.getIndexArtifactCount() != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException("snapshot capture manifest object identity mismatch");
    }
    validateReusableArtifactCoverage(manifest);
    validateCapturePolicy(lease, manifest.getCapturePolicy());
    validateIndexPredecessor(lease, snapshotTask, manifest);
    validateRealizedStatsSelectors(lease, manifest);
    validateRealizedIndexSelectors(lease, manifest);
    ReconcileSnapshotContentState.validateMaterializedStatsCoverage(
        snapshotTask.requestedCoverage(),
        manifest.getRealizedStatsSelectorsList(),
        manifest.getSourceFileCount());
    if (manifest
            .getCapturePolicy()
            .getOutputsList()
            .contains(ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_PARQUET_PAGE_INDEX)
        && manifest.getIndexArtifactCount() != manifest.getSourceFileCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest index artifacts do not cover planned files");
    }
    return new ValidatedCaptureManifest(manifest, bytes);
  }

  private record ValidatedCaptureManifest(
      SnapshotCaptureManifest manifest, byte[] serializedBytes) {}

  static void validateReusableArtifactCoverage(SnapshotCaptureManifest manifest) {
    Map<String, List<String>> groupStatsPrefixes = new LinkedHashMap<>();
    Set<String> expectedStatsPrefixes = new HashSet<>();
    for (var group : manifest.getFileGroupsList()) {
      if (group.getGroupId().isBlank()
          || group.getStatsObjectPrefix().isBlank()
          || !expectedStatsPrefixes.add(group.getStatsObjectPrefix())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest has duplicate file-group identity");
      }
      groupStatsPrefixes
          .computeIfAbsent("reuse-bundle:" + group.getGroupId(), ignored -> new ArrayList<>())
          .add(group.getStatsObjectPrefix());
    }

    if (!manifest.getReusableArtifactBundlesComplete()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reuse bundle index is not complete");
    }
    if (manifest.getReusableArtifactBundlesCount() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException("snapshot capture manifest reuse bundle count mismatch");
    }

    Set<String> bundleUris = new HashSet<>();
    Set<String> bundledStatsPrefixes = new HashSet<>();
    Set<String> reusableFileStats = new HashSet<>();
    Set<String> reusableIndexArtifacts = new HashSet<>();
    for (var bundle : manifest.getReusableArtifactBundlesList()) {
      if (!bundle.hasArtifact()) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reuse bundle has no artifact");
      }
      StatsObjectDescriptor artifact = bundle.getArtifact();
      String statsPrefix = null;
      for (String candidate :
          groupStatsPrefixes.getOrDefault(artifact.getTargetStorageId(), List.of())) {
        if (artifact.getPayloadUri().startsWith(candidate + "reuse-bundles/")) {
          if (statsPrefix != null) {
            throw new IllegalArgumentException(
                "snapshot capture manifest reuse bundle identity is ambiguous");
          }
          statsPrefix = candidate;
        }
      }
      if (statsPrefix == null
          || !bundledStatsPrefixes.add(statsPrefix)
          || artifact.getPayloadBytes() <= 0L
          || artifact.getPayloadSha256().size() != 32
          || !bundleUris.add(artifact.getPayloadUri())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reuse bundle identity mismatch");
      }
      for (var metadata : bundle.getFileStatsList()) {
        if (metadata.getFilePath().isBlank()
            || metadata.getSourceFingerprint().isBlank()
            || metadata.getStatsCaptureSignature().isBlank()
            || !reusableFileStats.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot capture manifest reuse stats metadata mismatch");
        }
      }
      for (var metadata : bundle.getIndexArtifactsList()) {
        if (metadata.getFilePath().isBlank()
            || metadata.getSourceFingerprint().isBlank()
            || metadata.getIndexCaptureSignature().isBlank()
            || !reusableIndexArtifacts.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot capture manifest reuse index metadata mismatch");
        }
      }
    }
    if (reusableFileStats.size() != manifest.getFileStatsRecordCount()
        || reusableIndexArtifacts.size() != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reuse bundle coverage mismatch");
    }
  }

  static void validateRealizedIndexSelectors(
      ReconcileJobStore.LeasedJob lease, SnapshotCaptureManifest manifest) {
    ReconcileCapturePolicy policy =
        lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
    List<String> submitted = manifest.getRealizedIndexSelectorsList();
    Set<String> realized = new HashSet<>();
    for (String selector : submitted) {
      if (selector == null || selector.isBlank() || !realized.add(selector.trim())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest contains invalid realized index selectors");
      }
    }
    if (!policy.requestsIndexes()) {
      if (!realized.isEmpty()) {
        throw new IllegalArgumentException(
            "non-index snapshot capture manifest contains realized index selectors");
      }
      return;
    }
    Set<String> required = policy.selectorsForIndex();
    boolean defaultSelection =
        required.isEmpty()
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    if (manifest.getSourceFileCount() == 0) {
      return;
    }
    if (!required.isEmpty() && !realized.containsAll(required)) {
      throw new IllegalArgumentException(
          "snapshot capture manifest does not cover explicitly requested index selectors");
    }
    if (defaultSelection && realized.isEmpty()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest does not report resolved default index selectors");
    }
    if (defaultSelection
        && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
        && realizedColumnCount(realized) > policy.maxDefaultColumns()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest exceeds the requested default index limit");
    }
  }

  private static void validateRealizedStatsSelectors(
      ReconcileJobStore.LeasedJob lease, SnapshotCaptureManifest manifest) {
    ReconcileCapturePolicy policy =
        lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
    List<String> submitted = manifest.getRealizedStatsSelectorsList();
    Set<String> realized = new HashSet<>();
    for (String selector : submitted) {
      if (selector == null || selector.isBlank() || !realized.add(selector.trim())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest contains invalid realized stats selectors");
      }
    }
    if (!policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      if (!realized.isEmpty()) {
        throw new IllegalArgumentException(
            "non-column-stats snapshot capture manifest contains realized stats selectors");
      }
      return;
    }
    boolean defaultSelection =
        policy.columns().stream().noneMatch(ReconcileCapturePolicy.Column::captureStats)
            && policy.defaultColumnScope()
                != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
    if (manifest.getSourceFileCount() == 0) {
      return;
    }
    if (defaultSelection
        && policy.defaultColumnScope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
        && realizedColumnCount(realized) > policy.maxDefaultColumns()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest exceeds the requested default stats limit");
    }
  }

  static int realizedColumnCount(Set<String> selectors) {
    int fieldIdCount =
        (int) selectors.stream().filter(selector -> selector.startsWith("#")).count();
    return fieldIdCount > 0 ? fieldIdCount : selectors.size();
  }

  private static void validateIndexPredecessor(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      SnapshotCaptureManifest manifest) {
    boolean indexesRequested = lease.scope != null && lease.scope.capturePolicy().requestsIndexes();
    if (!indexesRequested) {
      if (manifest.hasIndexPredecessor()) {
        throw new IllegalArgumentException(
            "non-index snapshot capture manifest contains an index predecessor");
      }
      return;
    }
    var pinned = snapshotTask.indexPredecessor();
    if (pinned == null) {
      throw new IllegalArgumentException(
          "snapshot index generation predecessor was not pinned before fan-out");
    }
    if (!manifest.hasIndexPredecessor()) {
      throw new IllegalArgumentException("index capture manifest is missing its predecessor");
    }
    var submitted = manifest.getIndexPredecessor();
    if (!pinned.generationId().equals(submitted.getGenerationId())
        || pinned.activePointerVersion() != submitted.getActivePointerVersion()
        || !pinned.captureManifestUri().equals(submitted.getCaptureManifestUri())
        || pinned.captureManifestPointerVersion() != submitted.getCaptureManifestPointerVersion()) {
      throw new IllegalArgumentException(
          "index capture manifest predecessor does not match the pinned snapshot predecessor");
    }
  }

  private static void validateCapturePolicy(
      ReconcileJobStore.LeasedJob lease, ai.floedb.floecat.reconciler.rpc.CapturePolicy submitted) {
    ReconcileCapturePolicy expected =
        lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
    Set<ai.floedb.floecat.reconciler.rpc.CaptureOutput> expectedOutputs = new HashSet<>();
    for (ReconcileCapturePolicy.Output output : expected.outputs()) {
      expectedOutputs.add(
          switch (output) {
            case TABLE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS;
            case FILE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_FILE_STATS;
            case COLUMN_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_COLUMN_STATS;
            case PARQUET_PAGE_INDEX ->
                ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_PARQUET_PAGE_INDEX;
          });
    }
    Set<ai.floedb.floecat.reconciler.rpc.CaptureOutput> submittedOutputs =
        new HashSet<>(submitted.getOutputsList());
    List<CapturePolicyColumn> expectedColumns =
        expected.columns().stream()
            .map(
                column ->
                    new CapturePolicyColumn(
                        column.selector(), column.captureStats(), column.captureIndex()))
            .toList();
    List<CapturePolicyColumn> submittedColumns =
        submitted.getColumnsList().stream()
            .map(
                column ->
                    new CapturePolicyColumn(
                        column.getSelector().trim(),
                        column.getCaptureStats(),
                        column.getCaptureIndex()))
            .toList();
    ReconcileCapturePolicy.DefaultColumnScope submittedDefaultScope =
        switch (submitted.getDefaultColumnScope()) {
          case DCS_ALL -> ReconcileCapturePolicy.DefaultColumnScope.ALL;
          case DCS_EXPLICIT_ONLY -> ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY;
          case DCS_FIRST_N, DCS_UNSPECIFIED, UNRECOGNIZED ->
              ReconcileCapturePolicy.DefaultColumnScope.FIRST_N;
        };
    int submittedMaxDefaultColumns =
        submitted.getMaxDefaultColumns() <= 0
            ? ReconcileCapturePolicy.DEFAULT_MAX_COLUMNS
            : submitted.getMaxDefaultColumns();
    if (!expectedOutputs.equals(submittedOutputs)
        || !expectedColumns.equals(submittedColumns)
        || expected.defaultColumnScope() != submittedDefaultScope
        || expected.maxDefaultColumns() != submittedMaxDefaultColumns
        || !expected.properties().equals(submitted.getPropertiesMap())) {
      throw new IllegalArgumentException(
          "snapshot capture manifest policy does not match the leased reconcile policy");
    }
  }

  private record CapturePolicyColumn(String selector, boolean captureStats, boolean captureIndex) {}

  private void publishCaptureArtifacts(
      ReconcileJobStore.LeasedJob lease,
      ResourceId tableId,
      ReconcileSnapshotTask snapshotTask,
      SnapshotCaptureManifest manifest) {
    String generationId = "full-rescan-" + lease.parentJobId;
    boolean inheritPriorStats = mayInheritPriorStats(lease, snapshotTask);
    Set<String> fileGroups = new HashSet<>();
    Map<String, ReconcileFileGroupResultDescriptor> storedFileGroups =
        succeededFileGroupDescriptors(lease);
    if (storedFileGroups.size() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest does not match the succeeded file-group children");
    }
    int declaredFileStats = 0;
    int declaredIndexArtifacts = 0;
    Map<String, String> stagedArtifactDigests = new LinkedHashMap<>();
    for (var fileGroup : manifest.getFileGroupsList()) {
      ReconcileFileGroupResultDescriptor stored =
          storedFileGroups.remove(fileGroup.getFileGroupJobId());
      String expectedStatsPrefix =
          Keys.reconcileFileGroupStatsObjectPrefix(
              tableId.getAccountId(),
              tableId.getId(),
              snapshotTask.snapshotId(),
              lease.parentJobId,
              fileGroup.getFileGroupJobId(),
              fileGroup.getLeaseEpoch());
      if (stored == null
          || !storedDescriptorMatches(fileGroup, stored)
          || fileGroup.getFormatVersion() != 1
          || !manifest.getAccountId().equals(fileGroup.getAccountId())
          || !manifest.getConnectorId().equals(fileGroup.getConnectorId())
          || !lease.parentJobId.equals(fileGroup.getParentJobId())
          || !manifest.getTableId().equals(fileGroup.getTableId())
          || manifest.getSnapshotId() != fileGroup.getSnapshotId()
          || !fileGroups.add(fileGroup.getPlanId() + ":" + fileGroup.getGroupId())
          || !expectedStatsPrefix.equals(fileGroup.getStatsObjectPrefix())
          || (manifest.hasIndexPredecessor()
              && !manifest.getIndexPredecessor().equals(fileGroup.getIndexPredecessor()))
          || fileGroup.getArtifactReferencesSha256().size() != 32) {
        throw new IllegalArgumentException(
            "snapshot file-group descriptor is outside the fenced worker location");
      }
      String artifactReferencesSha256 =
          HexFormat.of().formatHex(fileGroup.getArtifactReferencesSha256().toByteArray());
      if (stagedArtifactDigests.putIfAbsent(expectedStatsPrefix, artifactReferencesSha256) != null) {
        throw new IllegalArgumentException("duplicate reusable artifact bundle identity");
      }
      if (!statsStore.isPreparedFileGroup(
          tableId,
          snapshotTask.snapshotId(),
          generationId,
          fileGroup.getFileGroupJobId(),
          fileGroup.getLeaseEpoch(),
          artifactReferencesSha256)) {
        throw new StorageAbortRetryableException(
            "accepted file-group pointer staging is incomplete: " + fileGroup.getFileGroupJobId());
      }
      declaredFileStats += fileGroup.getFileStatsRecordCount();
      declaredIndexArtifacts += fileGroup.getIndexArtifactCount();
    }
    Set<String> reusableStatsFiles = new HashSet<>();
    int reusableStatsMetadataCount = 0;
    for (var bundle : manifest.getReusableArtifactBundlesList()) {
      String matchedPrefix = null;
      for (String statsPrefix : stagedArtifactDigests.keySet()) {
        if (bundle.getArtifact().getPayloadUri().startsWith(statsPrefix + "reuse-bundles/")) {
          if (matchedPrefix != null) {
            throw new IllegalArgumentException(
                "snapshot reusable bundle matches multiple staged file groups");
          }
          matchedPrefix = statsPrefix;
        }
      }
      if (matchedPrefix == null) {
        throw new IllegalArgumentException(
            "snapshot reusable bundle does not match a staged file group");
      }
      String expectedArtifactDigest = stagedArtifactDigests.remove(matchedPrefix);
      validateReusableArtifactBundle(bundle, expectedArtifactDigest);
      for (var metadata : bundle.getFileStatsList()) {
        reusableStatsMetadataCount++;
        if (metadata.getFilePath().isBlank() || !reusableStatsFiles.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot reusable bundle contains duplicate or invalid stats metadata");
        }
      }
    }
    if (!storedFileGroups.isEmpty()
        || !stagedArtifactDigests.isEmpty()
        || declaredFileStats < manifest.getFileStatsRecordCount()
        || reusableStatsMetadataCount != manifest.getFileStatsRecordCount()
        || reusableStatsFiles.size() != manifest.getFileStatsRecordCount()
        || declaredIndexArtifacts != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException("snapshot file-group artifact count mismatch");
    }
    List<StatsStore.PrewrittenTargetStatsReference> finalStats = new ArrayList<>();
    Set<String> finalTargets = new HashSet<>();
    String finalStatsPrefix =
        Keys.reconcileSnapshotFinalizeStatsObjectPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotTask.snapshotId(), lease.parentJobId);
    for (StatsObjectDescriptor object : manifest.getFinalStatsList()) {
      finalStats.add(prewrittenStatsReference(finalStatsPrefix, object));
      if (!finalTargets.add(object.getTargetStorageId())) {
        throw new IllegalArgumentException(
            "duplicate target in snapshot stats publication: " + object.getTargetStorageId());
      }
    }
    boolean capturedIndexes =
        manifest.getCapturePolicy().getOutputsList().contains(CaptureOutput.CO_PARQUET_PAGE_INDEX);
    IndexArtifactRepository.GenerationPredecessor indexPredecessor = null;
    IndexArtifactRepository.PreparedActivation preparedIndexActivation = null;
    if (capturedIndexes) {
      if (!manifest.hasIndexPredecessor()) {
        throw new IllegalArgumentException("index capture manifest is missing its predecessor");
      }
      var predecessor = manifest.getIndexPredecessor();
      indexPredecessor =
          new IndexArtifactRepository.GenerationPredecessor(
              predecessor.getGenerationId(),
              predecessor.getActivePointerVersion(),
              predecessor.getCaptureManifestUri(),
              predecessor.getCaptureManifestPointerVersion());
    }
    boolean published = false;
    for (int attempt = 0; attempt < 4 && !published; attempt++) {
      StatsStore.PublicationFence publicationFence = null;
      if (capturedIndexes) {
        preparedIndexActivation =
            indexArtifactRepository.prepareGenerationActivation(
                tableId,
                snapshotTask.snapshotId(),
                generationId,
                manifest.toByteArray(),
                indexPredecessor,
                false);
        publicationFence = preparedIndexActivation.publicationFence();
      }
      StatsStore.StatsGenerationPredecessor statsPredecessor =
          persistence.prepareStatsGenerationForPublication(
              tableId, snapshotTask.snapshotId(), generationId, inheritPriorStats);
      published =
          persistence.publishPreparedStatsGeneration(
              tableId,
              snapshotTask.snapshotId(),
              generationId,
              finalStats,
              statsPredecessor,
              publicationFence);
    }
    if (!published) {
      throw new StorageAbortRetryableException(
          "snapshot stats publication conflicted repeatedly for snapshot "
              + snapshotTask.snapshotId());
    }
    if (preparedIndexActivation != null) {
      indexArtifactRepository.completePreparedGenerationActivation(
          tableId, snapshotTask.snapshotId(), preparedIndexActivation);
    }
    persistence.clearPrewrittenArtifactProtections(
        tableId, snapshotTask.snapshotId(), generationId);
  }

  private Map<String, ReconcileFileGroupResultDescriptor> succeededFileGroupDescriptors(
      ReconcileJobStore.LeasedJob lease) {
    Map<String, ReconcileFileGroupResultDescriptor> descriptors = new LinkedHashMap<>();
    String pageToken = "";
    do {
      ReconcileJobStore.FileGroupResultDescriptorPage page =
          jobs.childFileGroupResultDescriptorsPage(
              lease.accountId, lease.parentJobId, 500, pageToken);
      for (ReconcileFileGroupResultDescriptor descriptor : page.descriptors) {
        if (descriptors.putIfAbsent(descriptor.fileGroupJobId(), descriptor) != null) {
          throw new IllegalArgumentException("duplicate succeeded file-group child descriptor");
        }
      }
      pageToken = page.nextPageToken;
    } while (pageToken != null && !pageToken.isBlank());
    return descriptors;
  }

  private static boolean storedDescriptorMatches(
      ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor submitted,
      ReconcileFileGroupResultDescriptor stored) {
    return submitted.getFormatVersion() == stored.formatVersion()
        && submitted.getAccountId().equals(stored.accountId())
        && submitted.getConnectorId().equals(stored.connectorId())
        && submitted.getParentJobId().equals(stored.parentJobId())
        && submitted.getFileGroupJobId().equals(stored.fileGroupJobId())
        && submitted.getPlanId().equals(stored.planId())
        && submitted.getGroupId().equals(stored.groupId())
        && submitted.getTableId().equals(stored.tableId())
        && submitted.getSnapshotId() == stored.snapshotId()
        && submitted.getLeaseEpoch().equals(stored.leaseEpoch())
        && submitted.getResultId().equals(stored.resultId())
        && submitted.getPayloadUri().equals(stored.payloadUri())
        && submitted.getPayloadBytes() == stored.payloadBytes()
        && Base64.getEncoder()
            .encodeToString(submitted.getPayloadSha256().toByteArray())
            .equals(stored.payloadSha256())
        && submitted.getPlannedFileCount() == stored.plannedFileCount()
        && submitted.getSucceededFileCount() == stored.succeededFileCount()
        && submitted.getFailedFileCount() == stored.failedFileCount()
        && submitted.getSkippedFileCount() == stored.skippedFileCount()
        && submitted.getPartialAggregateRecordCount() == stored.partialAggregateRecordCount()
        && submitted.getStatsObjectPrefix().equals(stored.statsObjectPrefix())
        && submitted.getFileStatsRecordCount() == stored.fileStatsRecordCount()
        && submitted.getIndexArtifactCount() == stored.indexArtifactCount()
        && HexFormat.of()
            .formatHex(submitted.getArtifactReferencesSha256().toByteArray())
            .equalsIgnoreCase(stored.artifactReferencesSha256())
        && submittedCreatedAtMatches(submitted, stored)
        && storedIndexPredecessorMatches(submitted, stored);
  }

  private static boolean submittedCreatedAtMatches(
      ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor submitted,
      ReconcileFileGroupResultDescriptor stored) {
    return submitted.hasCreatedAt()
        ? com.google.protobuf.util.Timestamps.toMillis(submitted.getCreatedAt())
            == stored.createdAtMs()
        : stored.createdAtMs() == 0L;
  }

  private static boolean storedIndexPredecessorMatches(
      ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor submitted,
      ReconcileFileGroupResultDescriptor stored) {
    ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor predecessor =
        stored.indexPredecessor();
    if (!submitted.hasIndexPredecessor()) {
      return predecessor == null;
    }
    return predecessor != null
        && submitted.getIndexPredecessor().getGenerationId().equals(predecessor.generationId())
        && submitted.getIndexPredecessor().getActivePointerVersion()
            == predecessor.activePointerVersion()
        && submitted
            .getIndexPredecessor()
            .getCaptureManifestUri()
            .equals(predecessor.captureManifestUri())
        && submitted.getIndexPredecessor().getCaptureManifestPointerVersion()
            == predecessor.captureManifestPointerVersion();
  }

  private boolean mayInheritPriorStats(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    if (lease.fullRescan || snapshotTask.sourceRevision().isBlank()) {
      return false;
    }
    ReconcileJobStore.FinalizedSnapshotEvent prior =
        jobs.getFinalizedSnapshot(
                lease.accountId, snapshotTask.tableId(), snapshotTask.snapshotId())
            .orElse(null);
    return prior != null
        && prior.formatVersion >= ReconcileSnapshotContentState.FORMAT_VERSION
        && lease.connectorId.equals(prior.connectorId)
        && snapshotTask.sourceNamespace().equals(prior.sourceNamespace)
        && snapshotTask.sourceTable().equals(prior.sourceTable)
        && snapshotTask.sourceRevision().equals(prior.sourceRevision);
  }

  private StatsStore.PrewrittenTargetStatsReference prewrittenStatsReference(
      String requiredPrefix, StatsObjectDescriptor descriptor) {
    String targetStorageId = descriptor.getTargetStorageId();
    byte[] payloadSha256 = descriptor.getPayloadSha256().toByteArray();
    String expectedUri =
        requiredPrefix
            + Hashing.sha256Hex(targetStorageId)
            + "/"
            + HexFormat.of().formatHex(payloadSha256)
            + ".pb";
    if (descriptor.getTargetStorageId().isBlank()
        || descriptor.getPayloadUri().isBlank()
        || !descriptor.getPayloadUri().equals(expectedUri)
        || descriptor.getPayloadBytes() <= 0L
        || descriptor.getPayloadSha256().size() != 32) {
      throw new IllegalArgumentException("invalid target stats object descriptor");
    }
    return new StatsStore.PrewrittenTargetStatsReference(
        targetStorageId, descriptor.getPayloadUri(), descriptor.getPayloadBytes(), payloadSha256);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  void validateReusableArtifactBundle(
      ReusableArtifactBundleReference submitted, String expectedArtifactDigest) {
    StatsObjectDescriptor artifact = submitted.getArtifact();
    byte[] bytes =
        loadRequiredPublishedObject(
            artifact.getPayloadUri(), "reusable artifact bundle " + artifact.getTargetStorageId());
    byte[] digest = sha256(bytes);
    if (artifact.getPayloadBytes() != bytes.length
        || artifact.getPayloadSha256().size() != 32
        || !MessageDigest.isEqual(digest, artifact.getPayloadSha256().toByteArray())
        || !ReusableArtifactBundleUris.matchesDigest(artifact.getPayloadUri(), digest)) {
      throw new IllegalArgumentException("reusable artifact bundle descriptor mismatch");
    }
    ReusableArtifactBundlePayload payload;
    try {
      payload = ReusableArtifactBundles.parse(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException error) {
      throw new IllegalArgumentException("reusable artifact bundle payload is invalid", error);
    }

    ReusableArtifactBundleReference.Builder expected =
        ReusableArtifactBundleReference.newBuilder().setArtifact(artifact);
    List<StatsObjectDescriptor> statsDescriptors = new ArrayList<>();
    for (var record : payload.getFileStatsList()) {
      String filePath =
          record.hasTarget() && record.getTarget().hasFile()
              ? record.getTarget().getFile().getFilePath()
              : "";
      expected.addFileStats(
          ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
              .setFilePath(filePath)
              .setSourceFingerprint(
                  record.getPropertiesOrDefault(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, ""))
              .setStatsCaptureSignature(
                  record.getPropertiesOrDefault(FileArtifactReuse.STATS_SIGNATURE_PROPERTY, ""))
              .addAllRealizedStatsSelectors(
                  FileArtifactReuse.decodeSelectors(
                      record.getPropertiesOrDefault(
                          FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, ""))));
      statsDescriptors.add(
          artifact.toBuilder()
              .setTargetStorageId(StatsTargetIdentity.storageId(record.getTarget()))
              .build());
    }
    List<StatsObjectDescriptor> indexDescriptors = new ArrayList<>();
    for (var record : payload.getIndexArtifactsList()) {
      String filePath =
          record.hasTarget() && record.getTarget().hasFile()
              ? record.getTarget().getFile().getFilePath()
              : "";
      expected.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
              .setFilePath(filePath)
              .setSourceFingerprint(
                  record.getPropertiesOrDefault(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, ""))
              .setIndexCaptureSignature(
                  record.getPropertiesOrDefault(FileArtifactReuse.INDEX_SIGNATURE_PROPERTY, ""))
              .addAllRealizedIndexSelectors(
                  FileArtifactReuse.decodeSelectors(
                          record.getPropertiesOrDefault(
                              FileArtifactReuse.INDEXED_COLUMNS_PROPERTY, ""))
                      .stream()
                      .sorted()
                      .toList()));
      indexDescriptors.add(
          artifact.toBuilder().setTargetStorageId("file:" + filePath).build());
    }
    if (!expected.build().equals(submitted)
        || !ArtifactReferenceDigest.sha256(statsDescriptors, indexDescriptors)
            .equalsIgnoreCase(expectedArtifactDigest)) {
      throw new IllegalArgumentException(
          "reusable artifact bundle metadata does not match staged artifacts");
    }
  }

  private byte[] loadRequiredPublishedObject(String uri, String description) {
    try {
      byte[] bytes = blobStore.get(uri);
      if (bytes == null) {
        throw new StorageAbortRetryableException(
            description + " is not committed or not yet visible uri=" + uri);
      }
      return bytes;
    } catch (StorageNotFoundException error) {
      throw new StorageAbortRetryableException(
          description + " is not committed or not yet visible uri=" + uri, error);
    }
  }

  private void requireReadyChildState(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    if (snapshotTask.fileGroupCount() == 0) {
      return;
    }
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.compactChildState(
            lease.accountId, lease.parentJobId, lease.jobId, snapshotTask.fileGroupCount());
    if (!childState.duplicateGroups().isEmpty()
        || !childState.invalidSucceededGroups().isEmpty()
        || !childState.failedGroups().isEmpty()
        || !childState.cancelledGroups().isEmpty()
        || !childState.pendingGroups().isEmpty()
        || !childState.missingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot finalization child results are not ready")
          .asRuntimeException();
    }
  }

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    byte[] requestBytes = failurePayload(requiredResultId, effectiveMessage).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                                .setAccepted(true)
                                .build(),
                            tableId),
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedSnapshotFinalizeResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
  }

  private ReconcileJobStore.LeasedJob requireLeasedSnapshotFinalizeJob(
      String corr, String jobId, String leaseEpoch) {
    if (!jobs.renewLease(jobId, leaseEpoch)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getCompactLeaseView(jobId)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("reconcile job not found " + jobId)
                        .asRuntimeException());
    if (job.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a FINALIZE_SNAPSHOT_CAPTURE job")
          .asRuntimeException();
    }
    if (!isActiveLeasedState(job.state)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is no longer active for lease " + jobId)
          .asRuntimeException();
    }
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode,
        job.scope,
        job.executionPolicy,
        leaseEpoch,
        "",
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
  }

  private static ReconcileSnapshotTask requireSnapshotTask(ReconcileJobStore.LeasedJob lease) {
    ReconcileSnapshotTask snapshotTask =
        lease == null || lease.snapshotTask == null
            ? ReconcileSnapshotTask.empty()
            : lease.snapshotTask;
    if (snapshotTask.isEmpty()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot task is required for FINALIZE_SNAPSHOT_CAPTURE result submission")
          .asRuntimeException();
    }
    return snapshotTask;
  }

  private static ResourceId tableId(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(snapshotTask.tableId())
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Success successPayload(
      String resultId, SnapshotCaptureManifestDescriptor descriptor) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
        .setResultId(resultId)
        .setManifestDescriptor(descriptor)
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Failure failurePayload(
      String resultId, String message) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
  }

  private static String resultIdempotencyKey(String jobId, String resultId) {
    return (jobId == null ? "" : jobId.trim()) + ":" + resultId;
  }

  private static String requireResultId(String resultId) {
    if (resultId == null || resultId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("result_id is required for snapshot finalize result submission")
          .asRuntimeException();
    }
    return resultId.trim();
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }
}
