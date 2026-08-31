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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundles;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactManifest;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
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
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/** Validates and publishes fenced snapshot capture artifacts. */
@ApplicationScoped
public class LeasedSnapshotFinalizeExecutionService extends BaseServiceImpl {
  private static final Logger LOG = Logger.getLogger(LeasedSnapshotFinalizeExecutionService.class);

  @Inject ReconcileJobStore jobs;
  @Inject ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  @Inject SnapshotFinalizeChildStateService childStateService;
  @Inject SnapshotFinalizeCoverageService coverageService;
  @Inject CurrentSnapshotPointerService currentSnapshotPointerService;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject IndexArtifactRepository indexArtifactRepository;
  @Inject StatsStore statsStore;
  @Inject BlobStore blobStore;
  @Inject SnapshotRepository snapshotRepo;
  @Inject TableBlobReachabilityGuard reachabilityGuard;

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
      SnapshotCaptureManifestDescriptor validated;
      try {
        snapshotTask = requireSnapshotTask(lease);
        validated =
            validateManifestDescriptorIdentity(lease, snapshotTask, requiredResultId, descriptor);
      } finally {
        validateNanos[0] = System.nanoTime() - validateStartNanos;
      }
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
        ReconcileJobStore.SnapshotFinalizeCommitIntent intent =
            new ReconcileJobStore.SnapshotFinalizeCommitIntent(
                lease.jobId,
                lease.leaseEpoch,
                requiredResultId,
                validated.getManifestUri(),
                validated.getManifestBytes(),
                manifestSha256,
                validated.getFileGroupCount(),
                validated.getSourceFileCount(),
                validated.getStatsRecordCount(),
                validated.getIndexArtifactCount());
        requireAcceptedLeaseOutcome(
            jobs.beginSnapshotFinalizeCommit(lease.jobId, lease.leaseEpoch, intent), lease.jobId);
        accepted = true;
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

  public boolean publishAcceptedSnapshotFinalize(String jobId) {
    ReconcileJobStore.SnapshotFinalizeCommitIntent intent =
        jobs.snapshotFinalizeCommitIntent(jobId).orElse(null);
    if (intent == null) {
      return false;
    }
    long startedNanos = System.nanoTime();
    ReconcileJobStore.LeasedJob lease =
        jobs.getCompletionLeaseView(jobId, intent.leaseEpoch(), true)
            .filter(value -> value.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription("accepted snapshot finalizer is no longer publishable")
                        .asRuntimeException());
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    byte[] manifestDigest;
    try {
      manifestDigest = HexFormat.of().parseHex(intent.manifestSha256());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("snapshot capture manifest sha256 is invalid", e);
    }
    SnapshotCaptureManifestDescriptor descriptor =
        SnapshotCaptureManifestDescriptor.newBuilder()
            .setFormatVersion(1)
            .setAccountId(lease.accountId)
            .setConnectorId(lease.connectorId)
            .setParentJobId(lease.parentJobId)
            .setFinalizeJobId(lease.jobId)
            .setTableId(snapshotTask.tableId())
            .setSnapshotId(snapshotTask.snapshotId())
            .setLeaseEpoch(lease.leaseEpoch)
            .setResultId(intent.resultId())
            .setManifestUri(intent.manifestUri())
            .setManifestBytes(intent.manifestBytes())
            .setManifestSha256(com.google.protobuf.ByteString.copyFrom(manifestDigest))
            .setFileGroupCount(intent.fileGroupCount())
            .setSourceFileCount(intent.sourceFileCount())
            .setStatsRecordCount(Math.toIntExact(intent.statsRecordCount()))
            .setIndexArtifactCount(Math.toIntExact(intent.indexArtifactCount()))
            .build();
    SnapshotCaptureManifestDescriptor validated =
        validateManifestDescriptorIdentity(lease, snapshotTask, intent.resultId(), descriptor);
    ValidatedCaptureManifest validatedManifest =
        validateManifestObject(lease, snapshotTask, validated);
    requireReadyChildState(lease, snapshotTask);
    return reachabilityGuard.publishing(
        tableId,
        () ->
            publishAcceptedSnapshotFinalizeGuarded(
                jobId,
                startedNanos,
                intent,
                lease,
                snapshotTask,
                tableId,
                validated,
                validatedManifest));
  }

  private boolean publishAcceptedSnapshotFinalizeGuarded(
      String jobId,
      long startedNanos,
      ReconcileJobStore.SnapshotFinalizeCommitIntent intent,
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      SnapshotCaptureManifestDescriptor validated,
      ValidatedCaptureManifest validatedManifest) {
    SnapshotCaptureManifest manifest = validatedManifest.manifest();
    publishCaptureArtifacts(
        lease,
        tableId,
        snapshotTask,
        manifest,
        validated.getManifestUri(),
        validated.getManifestBytes());
    Snapshot finalizedSnapshot =
        snapshotRepo.recordReuseManifest(
            tableId,
            snapshotTask.snapshotId(),
            SnapshotReuseManifestRef.newBuilder()
                .setFormatVersion(
                    ai.floedb.floecat.reconciler.jobs.ReusableArtifactManifest.FORMAT_VERSION)
                .setUri(validated.getManifestUri())
                .setPayloadBytes(validated.getManifestBytes())
                .setPayloadSha256(validated.getManifestSha256())
                .setStatsGenerationManifestUri(
                    Keys.snapshotTargetStatsManifestBlobUri(
                        tableId.getAccountId(),
                        tableId.getId(),
                        snapshotTask.snapshotId(),
                        "full-rescan-" + lease.parentJobId))
                .build());
    currentSnapshotPointerService.maybeAdvance(tableId, finalizedSnapshot, lease.jobId);
    boolean accepted =
        jobs.completeSnapshotFinalizeSuccess(
            lease.jobId,
            lease.leaseEpoch,
            intent.resultId(),
            validated.getManifestUri(),
            validated.getManifestBytes(),
            intent.manifestSha256(),
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
    LOG.infof(
        "snapshot_finalize_publication_timing jobId=%s outcome=accepted totalMs=%.3f"
            + " manifestBytes=%d inheritedStats=%d inheritedIndexes=%d",
        jobId,
        (System.nanoTime() - startedNanos) / 1_000_000.0,
        validated.getManifestBytes(),
        validated.getStatsRecordCount(),
        validated.getIndexArtifactCount());
    return true;
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
        Keys.reconcileSnapshotDurableCaptureManifestUri(
            lease.accountId,
            snapshotTask.tableId(),
            snapshotTask.snapshotId(),
            lease.parentJobId,
            descriptor.getManifestSha256().toByteArray());
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
    validateCapturePolicy(lease, manifest.getCapturePolicy());
    validateRealizedIndexSelectors(lease, manifest);
    ReusableArtifactManifest.validate(manifest);
    if (!manifest.hasReusableArtifactIndex()) {
      throw new IllegalArgumentException("snapshot capture manifest is missing its reusable index");
    }
    ReusableArtifactIndexStore indexStore = new ReusableArtifactIndexStore(blobStore);
    indexStore.validateReadableReference(
        Keys.tableTargetStatsBlobPrefix(descriptor.getAccountId(), descriptor.getTableId()),
        manifest.getReusableArtifactIndex());
    if (manifest.getReusableArtifactIndex().getFileStatsRecordCount()
            != manifest.getFileStatsRecordCount()
        || manifest.getReusableArtifactIndex().getIndexArtifactCount()
            != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException(
          "snapshot reusable artifact index count does not match the manifest");
    }
    ReconcileSnapshotContentState.validateMaterializedStatsCoverage(
        snapshotTask.requestedCoverage(),
        manifest.getRealizedStatsSelectorsList(),
        manifest.getSourceFileCount());
    boolean capturedIndexes =
        manifest.getCapturePolicy().getOutputsList().contains(CaptureOutput.CO_PARQUET_PAGE_INDEX);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(descriptor.getAccountId())
            .setId(descriptor.getTableId())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    validateInheritedIndexArtifactBundles(
        tableId, capturedIndexes, coverageService.plannedFileGroups(snapshotTask), manifest);
    if (capturedIndexes && manifest.getIndexArtifactCount() != manifest.getSourceFileCount()) {
      throw new IllegalArgumentException("snapshot index artifacts do not cover every source file");
    }
    validateIndexPredecessor(snapshotTask, capturedIndexes, manifest);
    return new ValidatedCaptureManifest(manifest);
  }

  private void validateInheritedIndexArtifactBundles(
      ResourceId tableId,
      boolean capturedIndexes,
      List<ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask> plannedGroups,
      SnapshotCaptureManifest manifest) {
    if (!capturedIndexes && manifest.getInheritedIndexArtifactBundlesCount() != 0) {
      throw new IllegalArgumentException(
          "non-index snapshot contains inherited index artifact bundles");
    }
    List<StatsObjectDescriptor> expected =
        ReusableArtifactBundles.inheritedIndexArtifactBundles(plannedGroups);
    Map<String, StatsObjectDescriptor> expectedByUri = new LinkedHashMap<>();
    for (StatsObjectDescriptor descriptor : expected) {
      expectedByUri.put(descriptor.getPayloadUri(), descriptor);
    }
    Map<String, StatsObjectDescriptor> submittedByUri = new LinkedHashMap<>();
    for (StatsObjectDescriptor descriptor : manifest.getInheritedIndexArtifactBundlesList()) {
      if (submittedByUri.putIfAbsent(descriptor.getPayloadUri(), descriptor) != null) {
        throw new IllegalArgumentException("duplicate inherited index artifact bundle descriptor");
      }
    }
    if (!expectedByUri.equals(submittedByUri)) {
      throw new IllegalArgumentException(
          "snapshot inherited index artifact bundles do not match the immutable file plan");
    }
    if (capturedIndexes) {
      indexArtifactRepository.inheritedManagedSidecarGenerations(
          tableId, ReusableArtifactBundles.inheritedIndexArtifactBundleSelections(plannedGroups));
    }
  }

  private static void validateIndexPredecessor(
      ReconcileSnapshotTask snapshotTask,
      boolean capturedIndexes,
      SnapshotCaptureManifest manifest) {
    if (!capturedIndexes) {
      if (manifest.hasIndexPredecessor()) {
        throw new IllegalArgumentException("snapshot contains an unrequested index predecessor");
      }
      return;
    }
    var expected = snapshotTask.indexPredecessor();
    if (expected == null
        || !manifest.hasIndexPredecessor()
        || !expected.generationId().equals(manifest.getIndexPredecessor().getGenerationId())
        || expected.activePointerVersion()
            != manifest.getIndexPredecessor().getActivePointerVersion()
        || !expected
            .captureManifestUri()
            .equals(manifest.getIndexPredecessor().getCaptureManifestUri())
        || expected.captureManifestPointerVersion()
            != manifest.getIndexPredecessor().getCaptureManifestPointerVersion()) {
      throw new IllegalArgumentException(
          "snapshot index predecessor does not match the immutable snapshot plan");
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

  private static int realizedColumnCount(Set<String> selectors) {
    int fieldIdCount =
        (int) selectors.stream().filter(selector -> selector.startsWith("#")).count();
    return fieldIdCount > 0 ? fieldIdCount : selectors.size();
  }

  private static void validateCapturePolicy(
      ReconcileJobStore.LeasedJob lease, ai.floedb.floecat.reconciler.rpc.CapturePolicy submitted) {
    ReconcileCapturePolicy expected =
        lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
    Set<CaptureOutput> expectedOutputs = new HashSet<>();
    for (ReconcileCapturePolicy.Output output : expected.outputs()) {
      expectedOutputs.add(
          switch (output) {
            case TABLE_STATS -> CaptureOutput.CO_TABLE_STATS;
            case FILE_STATS -> CaptureOutput.CO_FILE_STATS;
            case COLUMN_STATS -> CaptureOutput.CO_COLUMN_STATS;
            case PARQUET_PAGE_INDEX -> CaptureOutput.CO_PARQUET_PAGE_INDEX;
          });
    }
    Set<CaptureOutput> submittedOutputs = new HashSet<>(submitted.getOutputsList());
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

  private record ValidatedCaptureManifest(SnapshotCaptureManifest manifest) {}

  private static String currentBundleStatsPrefix(
      Map<String, List<String>> groupStatsPrefixes, StatsObjectDescriptor artifact) {
    String statsPrefix = null;
    for (String candidate :
        groupStatsPrefixes.getOrDefault(artifact.getTargetStorageId(), List.of())) {
      if (!artifact
          .getPayloadUri()
          .startsWith(candidate + ReusableArtifactBundleUris.BUNDLE_DIRECTORY)) {
        continue;
      }
      if (statsPrefix != null) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reuse bundle identity is ambiguous");
      }
      statsPrefix = candidate;
    }
    return statsPrefix;
  }

  private void publishCaptureArtifacts(
      ReconcileJobStore.LeasedJob lease,
      ResourceId tableId,
      ReconcileSnapshotTask snapshotTask,
      SnapshotCaptureManifest manifest,
      String captureManifestUri,
      long captureManifestBytes) {
    String generationId = "full-rescan-" + lease.parentJobId;
    boolean inheritPriorStats = mayInheritPriorStats(lease, snapshotTask);
    Set<String> fileGroups = new HashSet<>();
    Map<String, List<String>> groupStatsPrefixes = new LinkedHashMap<>();
    Map<String, ReconcileFileGroupResultDescriptor> storedFileGroups =
        succeededFileGroupDescriptors(lease);
    if (storedFileGroups.size() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest does not match the succeeded file-group children");
    }
    int declaredFileStats = 0;
    int declaredIndexArtifacts = 0;
    Map<String, String> stagedArtifactDigests = new HashMap<>();
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
      if (stagedArtifactDigests.putIfAbsent(expectedStatsPrefix, artifactReferencesSha256)
          != null) {
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
      groupStatsPrefixes
          .computeIfAbsent("reuse-bundle:" + fileGroup.getGroupId(), ignored -> new ArrayList<>())
          .add(fileGroup.getStatsObjectPrefix());
    }
    Set<String> reusableStatsFiles = new HashSet<>();
    int reusableStatsMetadataCount = 0;
    int currentStatsMetadataCount = 0;
    int currentIndexMetadataCount = 0;
    Set<String> reusableIndexFiles = new HashSet<>();
    for (var bundle : manifest.getReusableArtifactBundlesList()) {
      StatsObjectDescriptor artifact = bundle.getArtifact();
      String currentStatsPrefix = currentBundleStatsPrefix(groupStatsPrefixes, artifact);
      if (currentStatsPrefix != null) {
        String stagedArtifactDigest = stagedArtifactDigests.remove(currentStatsPrefix);
        if (stagedArtifactDigest == null) {
          throw new IllegalArgumentException(
              "snapshot reusable bundle does not match a staged file group");
        }
        validateStagedArtifactMappings(bundle, stagedArtifactDigest);
      }
      for (var metadata : bundle.getFileStatsList()) {
        reusableStatsMetadataCount++;
        if (metadata.getFilePath().isBlank() || !reusableStatsFiles.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot reusable bundle contains duplicate or invalid stats metadata");
        }
        if (currentStatsPrefix != null) {
          currentStatsMetadataCount++;
        }
      }
      for (var metadata : bundle.getIndexArtifactsList()) {
        if (metadata.getFilePath().isBlank() || !reusableIndexFiles.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot reusable bundle contains duplicate or invalid index metadata");
        }
        if (currentStatsPrefix != null) {
          currentIndexMetadataCount++;
        }
      }
    }
    int inheritedFileStats =
        manifest.hasAppendOnlyBase() ? manifest.getAppendOnlyBase().getFileStatsRecordCount() : 0;
    int inheritedIndexArtifacts =
        manifest.hasAppendOnlyBase() ? manifest.getAppendOnlyBase().getIndexArtifactCount() : 0;
    if (!storedFileGroups.isEmpty()
        || !stagedArtifactDigests.isEmpty()
        || declaredFileStats < currentStatsMetadataCount
        || reusableStatsMetadataCount != manifest.getFileStatsRecordCount() - inheritedFileStats
        || reusableStatsFiles.size() != manifest.getFileStatsRecordCount() - inheritedFileStats
        || reusableIndexFiles.size() != manifest.getIndexArtifactCount() - inheritedIndexArtifacts
        || manifest.getIndexArtifactCount() - inheritedIndexArtifacts != currentIndexMetadataCount
        || declaredIndexArtifacts != currentIndexMetadataCount) {
      throw new IllegalArgumentException("snapshot file-group artifact count mismatch");
    }
    boolean capturedIndexes =
        manifest.getCapturePolicy().getOutputsList().contains(CaptureOutput.CO_PARQUET_PAGE_INDEX);
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
    statsStore.registerGenerationArtifactMap(
        tableId, snapshotTask.snapshotId(), generationId, captureManifestUri, captureManifestBytes);
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

  static void validateStagedArtifactMappings(
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle,
      String stagedArtifactDigest) {
    StatsObjectDescriptor artifact = bundle.getArtifact();
    List<StatsObjectDescriptor> statsDescriptors =
        bundle.getFileStatsList().stream()
            .map(
                metadata ->
                    artifact.toBuilder()
                        .setTargetStorageId(
                            StatsTargetIdentity.storageId(
                                StatsTargetIdentity.fileTarget(metadata.getFilePath())))
                        .build())
            .toList();
    List<StatsObjectDescriptor> indexDescriptors =
        bundle.getIndexArtifactsList().stream()
            .map(
                metadata ->
                    artifact.toBuilder()
                        .setTargetStorageId("file:" + metadata.getFilePath())
                        .build())
            .toList();
    if (!ArtifactReferenceDigest.sha256(statsDescriptors, indexDescriptors)
        .equals(stagedArtifactDigest)) {
      throw new IllegalArgumentException(
          "snapshot reusable bundle target mappings do not match the staged file group");
    }
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
      String message,
      SubmitLeasedSnapshotFinalizeResultRequest.FailureKind failureKind) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    SubmitLeasedSnapshotFinalizeResultRequest.FailureKind effectiveFailureKind =
        failureKind == null
            ? SubmitLeasedSnapshotFinalizeResultRequest.FailureKind.SFFK_UNSPECIFIED
            : failureKind;
    byte[] requestBytes =
        failurePayload(requiredResultId, effectiveMessage, effectiveFailureKind).toByteArray();
    AtomicBoolean recorded = new AtomicBoolean();
    boolean accepted =
        MutationOps.createProtoReceiptOnly(
                principalContext.getAccountId(),
                "SubmitLeasedSnapshotFinalizeResult",
                resultIdempotencyKey(jobId, requiredResultId),
                () -> requestBytes,
                () -> {
                  recorded.set(true);
                  return new IdempotencyGuard.CreateResult<>(
                      SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                          .setAccepted(true)
                          .build(),
                      tableId);
                },
                ignored -> MutationMeta.getDefaultInstance(),
                idempotencyStore,
                nowTs(),
                idempotencyTtlSeconds(),
                principalContext::getCorrelationId,
                SubmitLeasedSnapshotFinalizeResultResponse::parseFrom)
            .body
            .getAccepted();
    if (recorded.get()
        && effectiveFailureKind
            == SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
                .SFFK_APPEND_ONLY_BASE_INCOMPATIBLE) {
      enqueueFullCaptureReplacement(lease, snapshotTask);
    }
    return accepted;
  }

  private void enqueueFullCaptureReplacement(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    String replacementJobId =
        jobs.enqueue(
            lease.accountId,
            lease.connectorId,
            true,
            lease.captureMode,
            lease.scope,
            lease.executionPolicy,
            "");
    LOG.warnf(
        "Append-only snapshot finalize is incompatible; enqueued full-capture reconcile"
            + " failedJobId=%s replacementJobId=%s tableId=%s snapshotId=%d",
        lease.jobId, replacementJobId, snapshotTask.tableId(), snapshotTask.snapshotId());
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
      String resultId,
      String message,
      SubmitLeasedSnapshotFinalizeResultRequest.FailureKind failureKind) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .setKind(
            failureKind == null
                ? SubmitLeasedSnapshotFinalizeResultRequest.FailureKind.SFFK_UNSPECIFIED
                : failureKind)
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
