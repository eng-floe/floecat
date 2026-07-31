/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.reconciler.jobs;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.List;
import java.util.Map;

public record ReconcileFileExecutionPlan(
    String filePath,
    long fileSizeInBytes,
    String partitionDataJson,
    DeltaDeletionVector deletionVector,
    String fileFormat,
    int partitionSpecId,
    List<IcebergDeleteFile> icebergDeleteFiles,
    String contentIdentity,
    String sourceFingerprint,
    String indexSourceFingerprint,
    String statsCaptureSignature,
    String indexCaptureSignature,
    Map<String, String> auxiliaryStatsFingerprints,
    TargetStatsRecord reusableFileStats,
    List<TargetStatsRecord> reusableAuxiliaryStats,
    IndexArtifactRecord reusableIndexArtifact,
    ReusableStatsArtifactReference reusableFileStatsReference,
    List<ReusableStatsArtifactReference> reusableAuxiliaryStatsReferences,
    ReusableIndexArtifactReference reusableIndexArtifactReference,
    List<ReusableArtifactBundleSelection> reusableArtifactBundleSelections) {

  public ReconcileFileExecutionPlan {
    filePath = filePath == null ? "" : filePath.trim();
    fileSizeInBytes = Math.max(0L, fileSizeInBytes);
    partitionDataJson = partitionDataJson == null ? "" : partitionDataJson;
    fileFormat = fileFormat == null ? "" : fileFormat.trim();
    icebergDeleteFiles = icebergDeleteFiles == null ? List.of() : List.copyOf(icebergDeleteFiles);
    contentIdentity = contentIdentity == null ? "" : contentIdentity.trim();
    sourceFingerprint = sourceFingerprint == null ? "" : sourceFingerprint.trim();
    indexSourceFingerprint = indexSourceFingerprint == null ? "" : indexSourceFingerprint.trim();
    statsCaptureSignature = statsCaptureSignature == null ? "" : statsCaptureSignature.trim();
    indexCaptureSignature = indexCaptureSignature == null ? "" : indexCaptureSignature.trim();
    auxiliaryStatsFingerprints =
        auxiliaryStatsFingerprints == null ? Map.of() : Map.copyOf(auxiliaryStatsFingerprints);
    reusableFileStats =
        reusableFileStats == null ? TargetStatsRecord.getDefaultInstance() : reusableFileStats;
    reusableAuxiliaryStats =
        reusableAuxiliaryStats == null ? List.of() : List.copyOf(reusableAuxiliaryStats);
    reusableIndexArtifact =
        reusableIndexArtifact == null
            ? IndexArtifactRecord.getDefaultInstance()
            : reusableIndexArtifact;
    reusableAuxiliaryStatsReferences =
        reusableAuxiliaryStatsReferences == null
            ? List.of()
            : List.copyOf(reusableAuxiliaryStatsReferences);
    reusableArtifactBundleSelections =
        reusableArtifactBundleSelections == null
            ? List.of()
            : reusableArtifactBundleSelections.stream()
                .filter(selection -> selection != null && !selection.isEmpty())
                .toList();
  }

  public ReconcileFileExecutionPlan(
      String filePath,
      long fileSizeInBytes,
      String partitionDataJson,
      DeltaDeletionVector deletionVector,
      String fileFormat,
      int partitionSpecId,
      List<IcebergDeleteFile> icebergDeleteFiles) {
    this(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles,
        "",
        "",
        "",
        "",
        "",
        Map.of(),
        TargetStatsRecord.getDefaultInstance(),
        List.of(),
        IndexArtifactRecord.getDefaultInstance(),
        null,
        List.of(),
        null,
        List.of());
  }

  public static ReconcileFileExecutionPlan of(
      String filePath,
      long fileSizeInBytes,
      String partitionDataJson,
      DeltaDeletionVector deletionVector) {
    return new ReconcileFileExecutionPlan(
        filePath, fileSizeInBytes, partitionDataJson, deletionVector, "", 0, List.of());
  }

  public static ReconcileFileExecutionPlan of(
      String filePath,
      long fileSizeInBytes,
      String partitionDataJson,
      DeltaDeletionVector deletionVector,
      String fileFormat,
      int partitionSpecId,
      java.util.List<IcebergDeleteFile> icebergDeleteFiles) {
    return new ReconcileFileExecutionPlan(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles);
  }

  public static ReconcileFileExecutionPlan of(
      String filePath,
      long fileSizeInBytes,
      String partitionDataJson,
      DeltaDeletionVector deletionVector,
      String fileFormat,
      int partitionSpecId,
      java.util.List<IcebergDeleteFile> icebergDeleteFiles,
      String contentIdentity) {
    return new ReconcileFileExecutionPlan(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles,
        contentIdentity,
        "",
        "",
        "",
        "",
        Map.of(),
        TargetStatsRecord.getDefaultInstance(),
        List.of(),
        IndexArtifactRecord.getDefaultInstance(),
        null,
        List.of(),
        null,
        List.of());
  }

  public ReconcileFileExecutionPlan withReuse(
      String sourceFingerprint,
      String indexSourceFingerprint,
      String statsCaptureSignature,
      String indexCaptureSignature,
      Map<String, String> auxiliaryStatsFingerprints,
      TargetStatsRecord reusableFileStats,
      List<TargetStatsRecord> reusableAuxiliaryStats,
      IndexArtifactRecord reusableIndexArtifact) {
    return new ReconcileFileExecutionPlan(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles,
        contentIdentity,
        sourceFingerprint,
        indexSourceFingerprint,
        statsCaptureSignature,
        indexCaptureSignature,
        auxiliaryStatsFingerprints,
        reusableFileStats,
        reusableAuxiliaryStats,
        reusableIndexArtifact,
        null,
        List.of(),
        null,
        List.of());
  }

  public ReconcileFileExecutionPlan withReuseReferences(
      String sourceFingerprint,
      String indexSourceFingerprint,
      String statsCaptureSignature,
      String indexCaptureSignature,
      Map<String, String> auxiliaryStatsFingerprints,
      ReusableStatsArtifactReference reusableFileStatsReference,
      List<ReusableStatsArtifactReference> reusableAuxiliaryStatsReferences,
      ReusableIndexArtifactReference reusableIndexArtifactReference) {
    return new ReconcileFileExecutionPlan(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles,
        contentIdentity,
        sourceFingerprint,
        indexSourceFingerprint,
        statsCaptureSignature,
        indexCaptureSignature,
        auxiliaryStatsFingerprints,
        TargetStatsRecord.getDefaultInstance(),
        List.of(),
        IndexArtifactRecord.getDefaultInstance(),
        reusableFileStatsReference,
        reusableAuxiliaryStatsReferences,
        reusableIndexArtifactReference,
        List.of());
  }

  public ReconcileFileExecutionPlan withReuseBundleSelections(
      String sourceFingerprint,
      String indexSourceFingerprint,
      String statsCaptureSignature,
      String indexCaptureSignature,
      Map<String, String> auxiliaryStatsFingerprints,
      List<ReusableArtifactBundleSelection> selections) {
    return new ReconcileFileExecutionPlan(
        filePath,
        fileSizeInBytes,
        partitionDataJson,
        deletionVector,
        fileFormat,
        partitionSpecId,
        icebergDeleteFiles,
        contentIdentity,
        sourceFingerprint,
        indexSourceFingerprint,
        statsCaptureSignature,
        indexCaptureSignature,
        auxiliaryStatsFingerprints,
        TargetStatsRecord.getDefaultInstance(),
        List.of(),
        IndexArtifactRecord.getDefaultInstance(),
        null,
        List.of(),
        null,
        selections);
  }

  public boolean reusesFileStats() {
    return reusableArtifactBundleSelections.stream()
            .anyMatch(selection -> selection.statsFilePaths().contains(filePath))
        || (reusableFileStatsReference != null && !reusableFileStatsReference.isEmpty())
        || (reusableFileStats != null
            && !reusableFileStats.equals(TargetStatsRecord.getDefaultInstance()));
  }

  public boolean reusesIndexArtifact() {
    return reusableArtifactBundleSelections.stream()
            .anyMatch(selection -> selection.indexFilePaths().contains(filePath))
        || (reusableIndexArtifactReference != null && !reusableIndexArtifactReference.isEmpty())
        || (reusableIndexArtifact != null
            && !reusableIndexArtifact.equals(IndexArtifactRecord.getDefaultInstance()));
  }

  public record DeltaDeletionVector(
      String storageType,
      String pathOrInlineDv,
      Integer offset,
      int sizeInBytes,
      long cardinality) {
    public DeltaDeletionVector {
      storageType = storageType == null ? "" : storageType.trim();
      pathOrInlineDv = pathOrInlineDv == null ? "" : pathOrInlineDv;
      sizeInBytes = Math.max(0, sizeInBytes);
      cardinality = Math.max(0L, cardinality);
    }

    public boolean onDisk() {
      return ("u".equals(storageType) || "p".equals(storageType)) && !pathOrInlineDv.isBlank();
    }
  }

  public record IcebergDeleteFile(
      String filePath,
      long fileSizeInBytes,
      IcebergDeleteContent content,
      int partitionSpecId,
      java.util.List<Integer> equalityFieldIds,
      String contentIdentity) {
    public IcebergDeleteFile(
        String filePath,
        long fileSizeInBytes,
        IcebergDeleteContent content,
        int partitionSpecId,
        java.util.List<Integer> equalityFieldIds) {
      this(filePath, fileSizeInBytes, content, partitionSpecId, equalityFieldIds, "");
    }

    public IcebergDeleteFile {
      filePath = filePath == null ? "" : filePath.trim();
      fileSizeInBytes = Math.max(0L, fileSizeInBytes);
      content = content == null ? IcebergDeleteContent.UNSPECIFIED : content;
      equalityFieldIds =
          equalityFieldIds == null ? java.util.List.of() : java.util.List.copyOf(equalityFieldIds);
      contentIdentity = contentIdentity == null ? "" : contentIdentity.trim();
    }
  }

  public enum IcebergDeleteContent {
    UNSPECIFIED,
    POSITION,
    EQUALITY
  }
}
