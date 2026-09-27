/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactObjectReference;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactRegistrationManifestRef;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/** Reader for one independently committed, record-aligned Owner registration chunk. */
final class OwnerArtifactRegistrationManifest {
  static final int FORMAT_VERSION = 1;
  static final int CHUNK_HEADER_BYTES = 64;
  static final int DEFAULT_READ_BYTES = 8 * 1024 * 1024;
  static final int DEFAULT_MAX_OBJECTS = 10_000;
  static final int DEFAULT_MAX_TARGETS = 10_000;
  static final int HARD_MAX_READ_BYTES = 16 * 1024 * 1024;
  static final int HARD_MAX_OBJECTS = 100_000;
  static final int HARD_MAX_TARGETS = 50_000;
  private static final byte[] MAGIC = "FLOREGC1".getBytes(StandardCharsets.US_ASCII);

  record Batch(
      List<OwnerArtifactObjectReference> objects,
      long objectCount,
      long fileStatsTargetCount,
      long indexTargetCount,
      long aggregateStatsTargetCount,
      String firstTarget,
      String lastTarget) {}

  private OwnerArtifactRegistrationManifest() {}

  static boolean hasContentAddressedUri(
      OwnerArtifactRegistrationManifestRef descriptor,
      String accountId,
      String tableId,
      long snapshotId) {
    return descriptor
        .getUri()
        .equals(
            Keys.snapshotOwnerRegistrationManifestBlobUri(
                accountId,
                tableId,
                snapshotId,
                HexFormat.of()
                    .formatHex(descriptor.getCommitmentIndex().getPayloadSha256().toByteArray())));
  }

  static Batch readChunk(
      BlobStore blobs,
      OwnerArtifactRegistrationManifestRef descriptor,
      long chunkIndex,
      ExternalManifestChunkCommitment commitment) {
    validateDescriptor(descriptor);
    byte[] chunk =
        ExternalManifestCommitments.readVerifiedChunk(blobs, descriptor.getUri(), commitment);
    ByteBuffer records = ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN);
    if (records.remaining() < CHUNK_HEADER_BYTES) {
      throw new IllegalArgumentException("Owner registration chunk header is unreadable");
    }
    byte[] magic = new byte[8];
    records.get(magic);
    int version = records.getInt();
    int headerBytes = records.getInt();
    long ordinal = records.getLong();
    long objectCount = records.getLong();
    long fileTargets = records.getLong();
    long indexTargets = records.getLong();
    long aggregateTargets = records.getLong();
    long flags = records.getLong();
    if (!Arrays.equals(magic, MAGIC)
        || version != FORMAT_VERSION
        || headerBytes != CHUNK_HEADER_BYTES
        || ordinal != chunkIndex
        || objectCount != commitment.getRecordCount()
        || fileTargets != commitment.getFileStatsTargetCount()
        || indexTargets != commitment.getIndexTargetCount()
        || aggregateTargets != commitment.getAggregateStatsTargetCount()
        || flags != 0L) {
      throw new IllegalArgumentException("invalid Owner registration chunk header");
    }
    List<OwnerArtifactObjectReference> objects = new ArrayList<>(Math.toIntExact(objectCount));
    long actualFileTargets = 0L;
    long actualIndexTargets = 0L;
    long actualAggregateTargets = 0L;
    String firstTarget = "";
    String lastTarget = "";
    while (records.hasRemaining()) {
      if (records.remaining() < Integer.BYTES) {
        throw new IllegalArgumentException("truncated Owner registration record length");
      }
      long recordBytes = Integer.toUnsignedLong(records.getInt());
      if (recordBytes <= 0L || recordBytes > records.remaining()) {
        throw new IllegalArgumentException("invalid Owner registration record length");
      }
      byte[] encoded = new byte[Math.toIntExact(recordBytes)];
      records.get(encoded);
      OwnerArtifactObjectReference object;
      try {
        object = OwnerArtifactObjectReference.parseFrom(encoded);
      } catch (InvalidProtocolBufferException error) {
        throw new IllegalArgumentException("invalid Owner registration record", error);
      }
      objects.add(object);
      actualFileTargets =
          Math.addExact(actualFileTargets, object.getFileStatsTargetStorageIdsCount());
      actualIndexTargets =
          Math.addExact(actualIndexTargets, object.getIndexTargetStorageIdsCount());
      actualAggregateTargets =
          Math.addExact(actualAggregateTargets, object.getAggregateStatsTargetStorageIdsCount());
      List<String> targets = new ArrayList<>();
      targets.addAll(object.getAggregateStatsTargetStorageIdsList());
      targets.addAll(object.getFileStatsTargetStorageIdsList());
      targets.addAll(object.getIndexTargetStorageIdsList());
      for (String target : targets) {
        if (target.isBlank()
            || (!lastTarget.isEmpty() && compareUtf8Unsigned(lastTarget, target) >= 0)) {
          throw new IllegalArgumentException(
              "Owner registration manifest targets are not strictly sorted");
        }
        if (firstTarget.isEmpty()) {
          firstTarget = target;
        }
        lastTarget = target;
      }
    }
    if (objects.size() != objectCount
        || actualFileTargets != fileTargets
        || actualIndexTargets != indexTargets
        || actualAggregateTargets != aggregateTargets) {
      throw new IllegalArgumentException("Owner registration chunk totals mismatch");
    }
    return new Batch(
        List.copyOf(objects),
        objectCount,
        fileTargets,
        indexTargets,
        aggregateTargets,
        firstTarget,
        lastTarget);
  }

  static int compareUtf8Unsigned(String left, String right) {
    return Arrays.compareUnsigned(
        left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
  }

  static void validateDescriptor(OwnerArtifactRegistrationManifestRef descriptor) {
    if (descriptor.getFormatVersion() != FORMAT_VERSION
        || descriptor.getUri().isBlank()
        || descriptor.getPayloadBytes() < CHUNK_HEADER_BYTES
        || descriptor.getPayloadSha256().size() != 32
        || !descriptor.hasCommitmentIndex()) {
      throw new IllegalArgumentException("invalid Owner registration manifest descriptor");
    }
  }
}
