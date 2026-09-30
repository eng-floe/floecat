/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndexRef;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Loads and validates the small content-addressed index for a chunked external manifest. */
final class ExternalManifestCommitments {
  static final int FORMAT_VERSION = 1;
  static final int MAX_INDEX_BYTES = 16 * 1024 * 1024;
  static final int MAX_CHUNKS = 100_000;

  record CacheKey(
      ExternalManifestCommitmentIndexRef reference,
      ExternalManifestDomain domain,
      String accountId,
      String tableId,
      long snapshotId,
      long payloadBytes,
      long recordCount,
      long fileStatsTargets,
      long indexTargets,
      long aggregateStatsTargets,
      int fixedRecordBytes,
      int maximumChunkBytes,
      int maximumChunkRecords,
      int maximumChunkTargets) {}

  private ExternalManifestCommitments() {}

  static ExternalManifestCommitmentIndex load(
      ExternalManifestCommitmentCache cache,
      BlobStore blobs,
      ExternalManifestCommitmentIndexRef reference,
      ExternalManifestDomain domain,
      String accountId,
      String tableId,
      long snapshotId,
      long payloadBytes,
      long recordCount,
      long fileStatsTargets,
      long indexTargets,
      long aggregateStatsTargets,
      int fixedRecordBytes,
      int maximumChunkBytes,
      int maximumChunkRecords,
      int maximumChunkTargets) {
    validateReference(reference, domain, accountId, tableId, snapshotId);
    CacheKey cacheKey =
        new CacheKey(
            reference,
            domain,
            accountId,
            tableId,
            snapshotId,
            payloadBytes,
            recordCount,
            fileStatsTargets,
            indexTargets,
            aggregateStatsTargets,
            fixedRecordBytes,
            maximumChunkBytes,
            maximumChunkRecords,
            maximumChunkTargets);
    return cache.get(
        cacheKey,
        () -> {
          // BlobStore's default implementation may buffer the full object; S3BlobStore enforces
          // this as a physical HTTP Range bound. The exact-length check remains authoritative for
          // every implementation.
          byte[] bytes =
              blobs.getRangeAtMost(
                  reference.getUri(), 0L, Math.toIntExact(reference.getPayloadBytes() + 1L));
          if (bytes == null
              || bytes.length != reference.getPayloadBytes()
              || !MessageDigest.isEqual(
                  sha256(bytes), reference.getPayloadSha256().toByteArray())) {
            throw new IllegalArgumentException("external manifest commitment index is unreadable");
          }
          ExternalManifestCommitmentIndex index;
          try {
            index = ExternalManifestCommitmentIndex.parseFrom(bytes);
          } catch (InvalidProtocolBufferException error) {
            throw new IllegalArgumentException("invalid external manifest commitment index", error);
          }
          if (index.getFormatVersion() != FORMAT_VERSION
              || index.getDomain() != domain
              || index.getPayloadBytes() != payloadBytes
              || index.getRecordCount() != recordCount
              || index.getFileStatsTargetCount() != fileStatsTargets
              || index.getIndexTargetCount() != indexTargets
              || index.getAggregateStatsTargetCount() != aggregateStatsTargets
              || index.getFixedRecordBytes() != fixedRecordBytes
              || index.getChunksCount() != reference.getChunkCount()
              || index.getChunksCount() > MAX_CHUNKS
              || index.getChunkSizeLimit() <= 0L
              || index.getChunkSizeLimit() > maximumChunkBytes) {
            throw new IllegalArgumentException(
                "external manifest commitment index metadata mismatch");
          }
          long offset = 0L;
          long records = 0L;
          long fileTargets = 0L;
          long indexes = 0L;
          long aggregateTargets = 0L;
          for (ExternalManifestChunkCommitment chunk : index.getChunksList()) {
            long targets;
            long fixedPayloadBytes = -1L;
            try {
              targets =
                  Math.addExact(
                      Math.addExact(chunk.getFileStatsTargetCount(), chunk.getIndexTargetCount()),
                      chunk.getAggregateStatsTargetCount());
              if (fixedRecordBytes > 0) {
                fixedPayloadBytes =
                    Math.multiplyExact(chunk.getRecordCount(), (long) fixedRecordBytes);
              }
            } catch (ArithmeticException error) {
              throw new IllegalArgumentException("external manifest chunk counts overflow", error);
            }
            if (chunk.getPayloadOffset() != offset
                || chunk.getPayloadBytes() <= 0L
                || chunk.getPayloadBytes() > index.getChunkSizeLimit()
                || chunk.getPayloadBytes() > maximumChunkBytes
                || chunk.getPayloadSha256().size() != 32
                || chunk.getRecordCount() <= 0L
                || chunk.getRecordCount() > maximumChunkRecords
                || targets > maximumChunkTargets
                || (fixedRecordBytes > 0 && chunk.getPayloadBytes() != fixedPayloadBytes)) {
              throw new IllegalArgumentException("invalid external manifest chunk commitment");
            }
            try {
              offset = Math.addExact(offset, chunk.getPayloadBytes());
              records = Math.addExact(records, chunk.getRecordCount());
              fileTargets = Math.addExact(fileTargets, chunk.getFileStatsTargetCount());
              indexes = Math.addExact(indexes, chunk.getIndexTargetCount());
              aggregateTargets =
                  Math.addExact(aggregateTargets, chunk.getAggregateStatsTargetCount());
            } catch (ArithmeticException error) {
              throw new IllegalArgumentException(
                  "external manifest commitment totals overflow", error);
            }
          }
          if (offset != payloadBytes
              || records != recordCount
              || fileTargets != fileStatsTargets
              || indexes != indexTargets
              || aggregateTargets != aggregateStatsTargets) {
            throw new IllegalArgumentException("external manifest commitment totals mismatch");
          }
          return index;
        });
  }

  static byte[] readVerifiedChunk(
      BlobStore blobs, String payloadUri, ExternalManifestChunkCommitment commitment) {
    byte[] bytes =
        blobs.getRange(
            payloadUri,
            commitment.getPayloadOffset(),
            Math.toIntExact(commitment.getPayloadBytes()));
    if (bytes == null
        || bytes.length != commitment.getPayloadBytes()
        || !MessageDigest.isEqual(sha256(bytes), commitment.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException("external manifest chunk digest mismatch");
    }
    return bytes;
  }

  private static void validateReference(
      ExternalManifestCommitmentIndexRef reference,
      ExternalManifestDomain domain,
      String accountId,
      String tableId,
      long snapshotId) {
    String digest = HexFormat.of().formatHex(reference.getPayloadSha256().toByteArray());
    String expected =
        Keys.snapshotOwnerManifestCommitmentIndexBlobUri(
            accountId, tableId, snapshotId, domainName(domain), digest);
    if (reference.getFormatVersion() != FORMAT_VERSION
        || reference.getDomain() != domain
        || reference.getPayloadBytes() <= 0L
        || reference.getPayloadBytes() > MAX_INDEX_BYTES
        || reference.getPayloadSha256().size() != 32
        || (reference.getChunkCount() == 0L
            && domain != ExternalManifestDomain.EMD_REUSABLE_COVERAGE)
        || reference.getChunkCount() > MAX_CHUNKS
        || !expected.equals(reference.getUri())) {
      throw new IllegalArgumentException("invalid external manifest commitment index descriptor");
    }
  }

  private static String domainName(ExternalManifestDomain domain) {
    return switch (domain) {
      case EMD_OWNER_ARTIFACT_REGISTRATION -> "registration";
      case EMD_REUSABLE_COVERAGE -> "coverage";
      default -> throw new IllegalArgumentException("unsupported external manifest domain");
    };
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }
}
