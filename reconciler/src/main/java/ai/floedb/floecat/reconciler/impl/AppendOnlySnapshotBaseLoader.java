/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Loads and authenticates the immutable predecessor used by append-only finalization. */
final class AppendOnlySnapshotBaseLoader {
  private final BlobStore blobStore;

  AppendOnlySnapshotBaseLoader(BlobStore blobStore) {
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
  }

  Loaded load(
      ReconcileJobStore.LeasedJob lease,
      StandaloneSnapshotFinalizeExecutionPayload input,
      SnapshotPlanBlobStore.AppendOnlyBase base) {
    if (base == null) {
      return null;
    }
    if (base.snapshotId() == input.snapshotId()
        || base.sourceFileCount() > input.sourceFileCount()) {
      throw new AppendOnlyBaseCompatibilityException(
          "append-only snapshot base is not an earlier subset");
    }
    // Blob reads stay untyped: a storage failure is retryable and must not schedule a rescan.
    byte[] bytes = blobStore.get(base.manifestUri());
    if (bytes == null) {
      throw new StorageNotFoundException(
          "append-only snapshot base manifest is missing: " + base.manifestUri());
    }
    if (bytes.length != base.manifestBytes()
        || !MessageDigest.isEqual(sha256(bytes), base.manifestSha256Bytes())) {
      throw new AppendOnlyBaseCompatibilityException(
          "append-only snapshot base manifest metadata mismatch");
    }
    SnapshotCaptureManifest manifest = parseManifest(bytes);
    try {
      ReusableArtifactManifest.validateReuseBaseSummary(manifest);
    } catch (IllegalArgumentException error) {
      throw new AppendOnlyBaseCompatibilityException(
          "append-only snapshot base reuse summary is invalid", error);
    }
    ReconcileCapturePolicy policy =
        lease.scope == null ? ReconcileCapturePolicy.empty() : lease.scope.capturePolicy();
    if (manifest.getFormatVersion() != ReusableArtifactManifest.FORMAT_VERSION
        || !lease.accountId.equals(manifest.getAccountId())
        || !lease.connectorId.equals(manifest.getConnectorId())
        || !input.tableId().getId().equals(manifest.getTableId())
        || base.snapshotId() != manifest.getSnapshotId()
        || base.sourceFileCount() != manifest.getSourceFileCount()
        || base.fileStatsRecordCount() != manifest.getFileStatsRecordCount()
        || base.indexArtifactCount() != manifest.getIndexArtifactCount()
        || base.chainDepth() != ReusableArtifactManifest.chainDepth(manifest)
        || !base.reusableArtifactIndex().equals(manifest.getReusableArtifactIndex())
        || !base.statsGenerationId().equals("full-rescan-" + manifest.getParentJobId())
        || (base.indexArtifactCount() > 0
            && !base.indexGenerationId().equals("full-rescan-" + manifest.getParentJobId()))
        || !RemoteSnapshotPlanningReconcileExecutor.capturePolicyMatches(
            policy, manifest.getCapturePolicy())) {
      throw new AppendOnlyBaseCompatibilityException(
          "append-only snapshot base manifest identity mismatch");
    }
    List<TargetStatsRecord> aggregates = loadAggregates(manifest, input);
    return new Loaded(
        base,
        aggregates,
        manifest.getRealizedStatsSelectorsList(),
        manifest.getRealizedIndexSelectorsList());
  }

  private SnapshotCaptureManifest parseManifest(byte[] bytes) {
    try {
      return SnapshotCaptureManifest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException error) {
      throw new AppendOnlyBaseCompatibilityException(
          "append-only snapshot base manifest is invalid", error);
    }
  }

  private List<TargetStatsRecord> loadAggregates(
      SnapshotCaptureManifest manifest, StandaloneSnapshotFinalizeExecutionPayload input) {
    List<TargetStatsRecord> aggregates = new ArrayList<>();
    for (var descriptor : manifest.getFinalStatsList()) {
      byte[] recordBytes = blobStore.get(descriptor.getPayloadUri());
      if (recordBytes == null) {
        throw new StorageNotFoundException(
            "append-only snapshot base aggregate is missing: " + descriptor.getPayloadUri());
      }
      if (recordBytes.length != descriptor.getPayloadBytes()
          || descriptor.getPayloadSha256().size() != 32
          || !MessageDigest.isEqual(
              sha256(recordBytes), descriptor.getPayloadSha256().toByteArray())) {
        throw new AppendOnlyBaseCompatibilityException(
            "append-only base aggregate metadata mismatch");
      }
      try {
        aggregates.add(
            TargetStatsRecord.parseFrom(recordBytes).toBuilder()
                .setTableId(input.tableId())
                .setSnapshotId(input.snapshotId())
                .build());
      } catch (InvalidProtocolBufferException error) {
        throw new AppendOnlyBaseCompatibilityException(
            "append-only base aggregate is invalid", error);
      }
    }
    if (aggregates.size() != manifest.getFinalStatsRecordCount()) {
      throw new AppendOnlyBaseCompatibilityException("append-only base aggregate count mismatch");
    }
    return List.copyOf(aggregates);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
  }

  record Loaded(
      SnapshotPlanBlobStore.AppendOnlyBase base,
      List<TargetStatsRecord> aggregateRecords,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors) {}
}
