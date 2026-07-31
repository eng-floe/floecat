/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.reconciler.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Compact reference to an immutable reusable target-stats record. */
public record ReusableStatsArtifactReference(
    String filePath,
    String targetStorageId,
    String payloadUri,
    long payloadBytes,
    byte[] payloadSha256,
    String sourceFingerprint,
    String statsCaptureSignature,
    List<String> realizedStatsSelectors) {

  public ReusableStatsArtifactReference {
    filePath = normalize(filePath);
    targetStorageId = normalize(targetStorageId);
    payloadUri = normalize(payloadUri);
    payloadBytes = Math.max(0L, payloadBytes);
    payloadSha256 = payloadSha256 == null ? new byte[0] : payloadSha256.clone();
    sourceFingerprint = normalize(sourceFingerprint);
    statsCaptureSignature = normalize(statsCaptureSignature);
    realizedStatsSelectors =
        realizedStatsSelectors == null ? List.of() : List.copyOf(realizedStatsSelectors);
  }

  @Override
  public byte[] payloadSha256() {
    return payloadSha256.clone();
  }

  public boolean isEmpty() {
    return filePath.isBlank() || payloadUri.isBlank();
  }

  @Override
  public boolean equals(Object value) {
    return value instanceof ReusableStatsArtifactReference other
        && payloadBytes == other.payloadBytes
        && filePath.equals(other.filePath)
        && targetStorageId.equals(other.targetStorageId)
        && payloadUri.equals(other.payloadUri)
        && Arrays.equals(payloadSha256, other.payloadSha256)
        && sourceFingerprint.equals(other.sourceFingerprint)
        && statsCaptureSignature.equals(other.statsCaptureSignature)
        && realizedStatsSelectors.equals(other.realizedStatsSelectors);
  }

  @Override
  public int hashCode() {
    return 31
            * Objects.hash(
                filePath,
                targetStorageId,
                payloadUri,
                payloadBytes,
                sourceFingerprint,
                statsCaptureSignature,
                realizedStatsSelectors)
        + Arrays.hashCode(payloadSha256);
  }

  private static String normalize(String value) {
    return value == null ? "" : value.trim();
  }
}
