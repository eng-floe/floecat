/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.reconciler.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A file plan's selected records from one durable file-group reuse bundle. */
public record ReusableArtifactBundleSelection(
    String targetStorageId,
    String payloadUri,
    long payloadBytes,
    byte[] payloadSha256,
    List<String> statsFilePaths,
    List<String> indexFilePaths) {

  public ReusableArtifactBundleSelection {
    targetStorageId = targetStorageId == null ? "" : targetStorageId.trim();
    payloadUri = payloadUri == null ? "" : payloadUri.trim();
    payloadBytes = Math.max(0L, payloadBytes);
    payloadSha256 = payloadSha256 == null ? new byte[0] : payloadSha256.clone();
    statsFilePaths = statsFilePaths == null ? List.of() : List.copyOf(statsFilePaths);
    indexFilePaths = indexFilePaths == null ? List.of() : List.copyOf(indexFilePaths);
  }

  @Override
  public byte[] payloadSha256() {
    return payloadSha256.clone();
  }

  public boolean isEmpty() {
    return payloadUri.isBlank()
        || payloadBytes <= 0
        || payloadSha256.length != 32
        || (statsFilePaths.isEmpty() && indexFilePaths.isEmpty());
  }

  @Override
  public boolean equals(Object value) {
    return value instanceof ReusableArtifactBundleSelection other
        && payloadBytes == other.payloadBytes
        && targetStorageId.equals(other.targetStorageId)
        && payloadUri.equals(other.payloadUri)
        && Arrays.equals(payloadSha256, other.payloadSha256)
        && statsFilePaths.equals(other.statsFilePaths)
        && indexFilePaths.equals(other.indexFilePaths);
  }

  @Override
  public int hashCode() {
    return 31
            * Objects.hash(
                targetStorageId, payloadUri, payloadBytes, statsFilePaths, indexFilePaths)
        + Arrays.hashCode(payloadSha256);
  }
}
