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

public record ReconcileFileExecutionPlan(
    String filePath,
    long fileSizeInBytes,
    String partitionDataJson,
    DeltaDeletionVector deletionVector,
    String fileFormat,
    int partitionSpecId,
    java.util.List<IcebergDeleteFile> icebergDeleteFiles) {

  public ReconcileFileExecutionPlan {
    filePath = filePath == null ? "" : filePath.trim();
    fileSizeInBytes = Math.max(0L, fileSizeInBytes);
    partitionDataJson = partitionDataJson == null ? "" : partitionDataJson;
    fileFormat = fileFormat == null ? "" : fileFormat.trim();
    icebergDeleteFiles =
        icebergDeleteFiles == null
            ? java.util.List.of()
            : java.util.List.copyOf(icebergDeleteFiles);
  }

  public static ReconcileFileExecutionPlan of(
      String filePath,
      long fileSizeInBytes,
      String partitionDataJson,
      DeltaDeletionVector deletionVector) {
    return new ReconcileFileExecutionPlan(
        filePath, fileSizeInBytes, partitionDataJson, deletionVector, "", 0, java.util.List.of());
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
      java.util.List<Integer> equalityFieldIds) {
    public IcebergDeleteFile {
      filePath = filePath == null ? "" : filePath.trim();
      fileSizeInBytes = Math.max(0L, fileSizeInBytes);
      content = content == null ? IcebergDeleteContent.UNSPECIFIED : content;
      equalityFieldIds =
          equalityFieldIds == null ? java.util.List.of() : java.util.List.copyOf(equalityFieldIds);
    }
  }

  public enum IcebergDeleteContent {
    UNSPECIFIED,
    POSITION,
    EQUALITY
  }
}
