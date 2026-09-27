/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain;
import ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef;
import ai.floedb.floecat.reconciler.rpc.ReusableOutputFamily;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** Reader for the Owner v2 sorted, fixed-width reusable coverage manifest. */
public final class ReusableCoverageManifest {
  public static final int FORMAT_VERSION = 1;
  public static final int RECORD_BYTES = 80;
  private static final int STORAGE_MANAGED = 0;
  private static final int STORAGE_EXTERNAL_SIDECAR = 1;

  public record Batch(
      java.util.List<Record> records,
      long recordCount,
      byte[] firstCoverageId,
      byte[] lastCoverageId,
      boolean sawExternalSidecar) {}

  public record Record(
      byte[] coverageId,
      ReusableOutputFamily outputFamily,
      long payloadBytes,
      byte[] payloadSha256,
      int storageSpace) {
    public boolean externalSidecar() {
      return storageSpace == STORAGE_EXTERNAL_SIDECAR;
    }
  }

  private ReusableCoverageManifest() {}

  public static void walkTrusted(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      long firstRecord,
      Consumer<Record> consumer,
      LongConsumer chunkComplete,
      String accountId,
      String tableId,
      long snapshotId) {
    walkCommitted(
        blobs, descriptor, firstRecord, consumer, chunkComplete, accountId, tableId, snapshotId);
  }

  private static void walkCommitted(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      long firstRecord,
      Consumer<Record> consumer,
      LongConsumer chunkComplete,
      String accountId,
      String tableId,
      long snapshotId) {
    ExternalManifestCommitmentIndex index =
        ExternalManifestCommitments.load(
            blobs,
            descriptor.getCommitmentIndex(),
            ExternalManifestDomain.EMD_REUSABLE_COVERAGE,
            accountId,
            tableId,
            snapshotId,
            descriptor.getPayloadBytes(),
            descriptor.getEntryCount(),
            0L,
            0L,
            0L,
            RECORD_BYTES,
            OwnerArtifactRegistrationManifest.HARD_MAX_READ_BYTES,
            OwnerArtifactRegistrationManifest.HARD_MAX_READ_BYTES / RECORD_BYTES,
            0);
    long record = 0L;
    byte[] prior = null;
    boolean foundCursor = firstRecord == 0L;
    for (ExternalManifestChunkCommitment commitment : index.getChunksList()) {
      if (record == firstRecord) {
        foundCursor = true;
      }
      if (record >= firstRecord) {
        Batch batch = readCommittedChunk(blobs, descriptor, commitment);
        if (prior != null && Arrays.compareUnsigned(prior, batch.firstCoverageId()) >= 0) {
          throw new IllegalArgumentException("reusable coverage manifest is not strictly sorted");
        }
        for (Record value : batch.records()) {
          consumer.accept(value);
        }
        prior = batch.lastCoverageId();
        record += commitment.getRecordCount();
        chunkComplete.accept(record);
      } else {
        record += commitment.getRecordCount();
      }
    }
    if (!foundCursor || record != descriptor.getEntryCount()) {
      throw new IllegalArgumentException("reusable coverage manifest cursor is not chunk aligned");
    }
  }

  public static Batch readCommittedChunk(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      ExternalManifestChunkCommitment commitment) {
    validateDescriptor(descriptor);
    byte[] chunk =
        ExternalManifestCommitments.readVerifiedChunk(blobs, descriptor.getUri(), commitment);
    if (chunk.length % RECORD_BYTES != 0
        || chunk.length / RECORD_BYTES != commitment.getRecordCount()) {
      throw new IllegalArgumentException("reusable coverage chunk is not record aligned");
    }
    ByteBuffer records = ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN);
    java.util.List<Record> decoded =
        new java.util.ArrayList<>(Math.toIntExact(commitment.getRecordCount()));
    byte[] firstCoverage = null;
    byte[] prior = null;
    boolean sawExternalSidecar = false;
    while (records.hasRemaining()) {
      Record record = decodeRecord(records, descriptor);
      if (prior != null && Arrays.compareUnsigned(prior, record.coverageId()) >= 0) {
        throw new IllegalArgumentException("reusable coverage manifest is not strictly sorted");
      }
      if (firstCoverage == null) {
        firstCoverage = record.coverageId();
      }
      prior = record.coverageId();
      sawExternalSidecar |= record.externalSidecar();
      decoded.add(record);
    }
    return new Batch(
        java.util.List.copyOf(decoded),
        decoded.size(),
        firstCoverage == null ? new byte[0] : firstCoverage,
        prior == null ? new byte[0] : prior,
        sawExternalSidecar);
  }

  public static boolean hasContentAddressedUri(
      ReusableCoverageManifestRef descriptor, String requiredPrefix) {
    byte[] identity = descriptor.getCommitmentIndex().getPayloadSha256().toByteArray();
    return descriptor
        .getUri()
        .equals(requiredPrefix + "reuse-" + HexFormat.of().formatHex(identity) + ".bin");
  }

  /** Validates descriptor metadata without fetching the manifest or any registered artifact. */
  public static boolean hasValidDescriptor(ReusableCoverageManifestRef descriptor) {
    try {
      validateDescriptor(descriptor);
      return true;
    } catch (IllegalArgumentException | ArithmeticException error) {
      return false;
    }
  }

  public static String managedUri(String prefix, Record record) {
    String directory =
        switch (record.outputFamily()) {
          case ROF_PLANNER_STATISTICS -> "statistics/planner/";
          case ROF_FILE_STATISTICS -> "statistics/files/";
          case ROF_FILE_RANGE_SIDECAR -> "sidecars/file-range/";
          case ROF_PAGE_GROUP_RANGE_SIDECAR -> "sidecars/page-group-range/";
          default -> throw new IllegalArgumentException("output family is not reusable");
        };
    String suffix =
        record.outputFamily() == ReusableOutputFamily.ROF_PLANNER_STATISTICS
                || record.outputFamily() == ReusableOutputFamily.ROF_FILE_STATISTICS
            ? ".pb"
            : ".parquet";
    return Keys.ownerReusableArtifactBlobUri(
        prefix,
        directory.substring(0, directory.length() - 1),
        HexFormat.of().formatHex(record.coverageId()),
        suffix);
  }

  private static void validateDescriptor(ReusableCoverageManifestRef descriptor) {
    long expected = Math.multiplyExact(descriptor.getEntryCount(), (long) RECORD_BYTES);
    if (descriptor.getFormatVersion() != FORMAT_VERSION
        || !descriptor.hasCommitmentIndex()
        || descriptor.getRecordBytes() != RECORD_BYTES
        || descriptor.getPayloadBytes() != expected
        || descriptor.getPayloadSha256().size() != 32
        || (descriptor.getExternalSidecarStorageSha256().size() != 0
            && descriptor.getExternalSidecarStorageSha256().size() != 32)
        || descriptor.getUri().isBlank()) {
      throw new IllegalArgumentException("invalid reusable coverage manifest descriptor");
    }
  }

  private static Record decodeRecord(ByteBuffer records, ReusableCoverageManifestRef descriptor) {
    byte[] coverage = new byte[32];
    byte[] payloadDigest = new byte[32];
    records.get(coverage);
    records.get(payloadDigest);
    long payloadBytes = records.getLong();
    ReusableOutputFamily family = ReusableOutputFamily.forNumber(records.getInt());
    int storageSpace = records.getInt();
    if (payloadBytes <= 0L
        || (storageSpace != STORAGE_MANAGED && storageSpace != STORAGE_EXTERNAL_SIDECAR)
        || (family != ReusableOutputFamily.ROF_PLANNER_STATISTICS
            && family != ReusableOutputFamily.ROF_FILE_STATISTICS
            && family != ReusableOutputFamily.ROF_FILE_RANGE_SIDECAR
            && family != ReusableOutputFamily.ROF_PAGE_GROUP_RANGE_SIDECAR)) {
      throw new IllegalArgumentException("invalid reusable coverage manifest record");
    }
    boolean sidecar =
        family == ReusableOutputFamily.ROF_FILE_RANGE_SIDECAR
            || family == ReusableOutputFamily.ROF_PAGE_GROUP_RANGE_SIDECAR;
    if (storageSpace == STORAGE_EXTERNAL_SIDECAR && !sidecar) {
      throw new IllegalArgumentException("only sidecars may use external reusable storage");
    }
    if (storageSpace == STORAGE_EXTERNAL_SIDECAR
        && descriptor.getExternalSidecarStorageSha256().size() != 32) {
      throw new IllegalArgumentException("external sidecar storage identity is missing");
    }
    return new Record(coverage, family, payloadBytes, payloadDigest, storageSpace);
  }
}
