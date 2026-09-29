/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.reconciler.rpc.AggregationTreeNode;
import ai.floedb.floecat.reconciler.rpc.AggregationTreeNodeRef;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain;
import ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef;
import ai.floedb.floecat.reconciler.rpc.ReusableOutputFamily;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** Reader for the Owner v2 fixed-width index of independently committed file-group shards. */
public final class ReusableCoverageManifest {
  public static final int FORMAT_VERSION = 1;
  public static final int RECORD_BYTES = 80;
  public static final int SHARD_INDEX_RECORD_BYTES = 192;
  private static final int STORAGE_MANAGED = 0;
  private static final int STORAGE_EXTERNAL_SIDECAR = 1;

  public record Batch(List<ShardReference> shards, long shardCount, long coverageEntryCount) {}

  public record ShardReference(
      byte[] shardKey,
      byte[] payloadSha256,
      long payloadBytes,
      long entryCount,
      byte[] plannerStatisticsPayloadSha256,
      long plannerStatisticsPayloadBytes,
      byte[] sidecarFormatsSha256,
      byte[] groupLayoutPayloadSha256,
      long groupLayoutPayloadBytes) {}

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
      long firstShard,
      Consumer<Record> consumer,
      Consumer<String> shardRoot,
      Set<String> completedAggregationNodes,
      LongConsumer chunkComplete,
      String accountId,
      String tableId,
      long snapshotId,
      String reusablePrefix) {
    ExternalManifestCommitmentIndex index =
        loadIndex(blobs, descriptor, accountId, tableId, snapshotId);
    long shard = 0L;
    boolean foundCursor = firstShard == 0L;
    for (ExternalManifestChunkCommitment commitment : index.getChunksList()) {
      if (shard == firstShard) {
        foundCursor = true;
      }
      if (shard >= firstShard) {
        Batch batch = readCommittedChunk(blobs, descriptor, commitment);
        for (ShardReference reference : batch.shards()) {
          String uri = shardUri(reusablePrefix, reference);
          shardRoot.accept(uri);
          shardRoot.accept(groupLayoutUri(reusablePrefix, reference));
          for (Record record : readShard(blobs, descriptor, reference, uri)) {
            consumer.accept(record);
          }
        }
        shard += batch.shardCount();
        chunkComplete.accept(shard);
      } else {
        shard += commitment.getRecordCount();
      }
    }
    if (shard == firstShard) {
      foundCursor = true;
    }
    if (!foundCursor || shard != descriptor.getShardCount()) {
      throw new IllegalArgumentException("reusable coverage shard-index cursor is not aligned");
    }
    if (descriptor.hasAggregationTreeRoot()) {
      walkAggregationTree(
          blobs,
          descriptor.getAggregationTreeRoot(),
          reusablePrefix,
          completedAggregationNodes,
          shardRoot);
    }
  }

  public static ExternalManifestCommitmentIndex loadIndex(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      String accountId,
      String tableId,
      long snapshotId) {
    validateDescriptor(descriptor);
    return ExternalManifestCommitments.load(
        blobs,
        descriptor.getCommitmentIndex(),
        ExternalManifestDomain.EMD_REUSABLE_COVERAGE,
        accountId,
        tableId,
        snapshotId,
        descriptor.getPayloadBytes(),
        descriptor.getShardCount(),
        0L,
        0L,
        0L,
        SHARD_INDEX_RECORD_BYTES,
        OwnerArtifactRegistrationManifest.HARD_MAX_READ_BYTES,
        OwnerArtifactRegistrationManifest.HARD_MAX_READ_BYTES / SHARD_INDEX_RECORD_BYTES,
        0);
  }

  public static Batch readCommittedChunk(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      ExternalManifestChunkCommitment commitment) {
    validateDescriptor(descriptor);
    byte[] chunk =
        ExternalManifestCommitments.readVerifiedChunk(blobs, descriptor.getUri(), commitment);
    if (chunk.length % SHARD_INDEX_RECORD_BYTES != 0
        || chunk.length / SHARD_INDEX_RECORD_BYTES != commitment.getRecordCount()) {
      throw new IllegalArgumentException("reusable coverage shard-index chunk is not aligned");
    }
    ByteBuffer records = ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN);
    List<ShardReference> decoded =
        new java.util.ArrayList<>(Math.toIntExact(commitment.getRecordCount()));
    HashSet<String> shardKeys = new HashSet<>();
    long coverageEntries = 0L;
    while (records.hasRemaining()) {
      ShardReference reference = decodeShardReference(records);
      if (!shardKeys.add(HexFormat.of().formatHex(reference.shardKey()))) {
        throw new IllegalArgumentException("duplicate reusable coverage shard key");
      }
      coverageEntries = Math.addExact(coverageEntries, reference.entryCount());
      decoded.add(reference);
    }
    return new Batch(List.copyOf(decoded), decoded.size(), coverageEntries);
  }

  public static boolean hasContentAddressedUri(
      ReusableCoverageManifestRef descriptor, String requiredPrefix) {
    byte[] identity = descriptor.getCommitmentIndex().getPayloadSha256().toByteArray();
    return descriptor
        .getUri()
        .equals(requiredPrefix + "reuse-index-" + HexFormat.of().formatHex(identity) + ".bin");
  }

  public static boolean hasValidDescriptor(ReusableCoverageManifestRef descriptor) {
    try {
      validateDescriptor(descriptor);
      return true;
    } catch (IllegalArgumentException | ArithmeticException error) {
      return false;
    }
  }

  public static String shardUri(String prefix, ShardReference reference) {
    return prefix
        + "coverage-shards/"
        + HexFormat.of().formatHex(reference.shardKey())
        + "-"
        + HexFormat.of().formatHex(reference.payloadSha256())
        + ".bin";
  }

  public static String groupLayoutUri(String prefix, ShardReference reference) {
    return prefix
        + "group-layouts/"
        + HexFormat.of().formatHex(reference.shardKey())
        + "-"
        + HexFormat.of().formatHex(reference.groupLayoutPayloadSha256())
        + ".bin";
  }

  public static String aggregationTreeNodeUri(String prefix, AggregationTreeNodeRef reference) {
    return prefix
        + "aggregation-tree/nodes/"
        + HexFormat.of().formatHex(reference.getPayloadSha256().toByteArray())
        + ".pb";
  }

  public static String managedUri(String prefix, Record record) {
    String directory =
        switch (record.outputFamily()) {
          case ROF_PLANNER_STATISTICS -> "statistics/planner";
          case ROF_FILE_STATISTICS -> "statistics/files";
          case ROF_FILE_RANGE_SIDECAR -> "sidecars/file-range";
          case ROF_PAGE_GROUP_RANGE_SIDECAR -> "sidecars/page-group-range";
          default -> throw new IllegalArgumentException("output family is not reusable");
        };
    String suffix =
        record.outputFamily() == ReusableOutputFamily.ROF_PLANNER_STATISTICS
                || record.outputFamily() == ReusableOutputFamily.ROF_FILE_STATISTICS
            ? ".pb"
            : ".parquet";
    return Keys.ownerReusableArtifactBlobUri(
        prefix, directory, HexFormat.of().formatHex(record.coverageId()), suffix);
  }

  private static List<Record> readShard(
      BlobStore blobs,
      ReusableCoverageManifestRef descriptor,
      ShardReference reference,
      String uri) {
    byte[] payload = blobs.get(uri);
    long expectedBytes = Math.multiplyExact(reference.entryCount(), (long) RECORD_BYTES);
    if (payload == null
        || reference.payloadBytes() != expectedBytes
        || payload.length != expectedBytes
        || !MessageDigest.isEqual(sha256(payload), reference.payloadSha256())) {
      throw new IllegalArgumentException("reusable coverage shard does not match its commitment");
    }
    ByteBuffer records = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    List<Record> decoded = new java.util.ArrayList<>(Math.toIntExact(reference.entryCount()));
    HashSet<String> coverageIds = new HashSet<>();
    while (records.hasRemaining()) {
      Record record = decodeRecord(records, descriptor);
      if (!coverageIds.add(HexFormat.of().formatHex(record.coverageId()))) {
        throw new IllegalArgumentException("duplicate coverage ID within reuse shard");
      }
      decoded.add(record);
    }
    return List.copyOf(decoded);
  }

  private static void validateDescriptor(ReusableCoverageManifestRef descriptor) {
    long expected = Math.multiplyExact(descriptor.getShardCount(), (long) SHARD_INDEX_RECORD_BYTES);
    boolean hasTree = descriptor.hasAggregationTreeRoot();
    if (descriptor.getFormatVersion() != FORMAT_VERSION
        || !descriptor.hasCommitmentIndex()
        || descriptor.getShardIndexRecordBytes() != SHARD_INDEX_RECORD_BYTES
        || descriptor.getShardRecordBytes() != RECORD_BYTES
        || descriptor.getPayloadBytes() != expected
        || descriptor.getPayloadSha256().size() != 32
        || (descriptor.getExternalSidecarStorageSha256().size() != 0
            && descriptor.getExternalSidecarStorageSha256().size() != 32)
        || hasTree != (descriptor.getShardCount() > 0L)
        || (hasTree
            && (descriptor.getAggregationTreeRoot().getLeafCount() != descriptor.getShardCount()
                || !hasValidNodeReference(descriptor.getAggregationTreeRoot())))
        || descriptor.getUri().isBlank()) {
      throw new IllegalArgumentException("invalid reusable coverage manifest descriptor");
    }
  }

  static void walkAggregationTree(
      BlobStore blobs,
      AggregationTreeNodeRef reference,
      String reusablePrefix,
      Set<String> completed,
      Consumer<String> root) {
    validateNodeReference(reference);
    String uri = aggregationTreeNodeUri(reusablePrefix, reference);
    if (completed.contains(uri)) {
      return;
    }
    byte[] payload = blobs.get(uri);
    if (payload == null
        || payload.length != reference.getPayloadBytes()
        || !MessageDigest.isEqual(sha256(payload), reference.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException("aggregation-tree node does not match its reference");
    }
    final AggregationTreeNode node;
    try {
      node = AggregationTreeNode.parseFrom(payload);
    } catch (com.google.protobuf.InvalidProtocolBufferException error) {
      throw new IllegalArgumentException("invalid aggregation-tree node", error);
    }
    validateNode(node, reference);
    root.accept(uri);
    if (node.hasBranch()) {
      walkAggregationTree(blobs, node.getBranch().getLeft(), reusablePrefix, completed, root);
      walkAggregationTree(blobs, node.getBranch().getRight(), reusablePrefix, completed, root);
    }
    completed.add(uri);
  }

  private static boolean hasValidNodeReference(AggregationTreeNodeRef reference) {
    try {
      validateNodeReference(reference);
      return true;
    } catch (IllegalArgumentException error) {
      return false;
    }
  }

  private static void validateNodeReference(AggregationTreeNodeRef reference) {
    if (reference.getPayloadBytes() <= 0L
        || reference.getPayloadSha256().size() != 32
        || reference.getMembershipSha256().size() != 32
        || reference.getMinKey().size() != 32
        || reference.getLeafCount() <= 0L) {
      throw new IllegalArgumentException("invalid aggregation-tree node reference");
    }
  }

  private static void validateNode(AggregationTreeNode node, AggregationTreeNodeRef reference) {
    if (node.getFormatVersion() != 1
        || node.getMembershipSha256().size() != 32
        || node.getMinKey().size() != 32
        || node.getLeafCount() != reference.getLeafCount()
        || !node.getMembershipSha256().equals(reference.getMembershipSha256())
        || !node.getMinKey().equals(reference.getMinKey())
        || node.getRecordsCount() == 0) {
      throw new IllegalArgumentException("invalid aggregation-tree node metadata");
    }
    if (node.hasLeaf()) {
      if (node.getLeafCount() != 1L
          || node.getLeaf().getKey().size() != 32
          || !node.getLeaf().getKey().equals(node.getMinKey())
          || node.getLeaf().getArtifactUri().isBlank()
          || node.getLeaf().getArtifactPayloadBytes() <= 0L
          || node.getLeaf().getArtifactPayloadSha256().size() != 32) {
        throw new IllegalArgumentException("invalid aggregation-tree leaf");
      }
      return;
    }
    if (!node.hasBranch()
        || !node.getBranch().hasLeft()
        || !node.getBranch().hasRight()
        || node.getBranch().getBit() >= 256) {
      throw new IllegalArgumentException("invalid aggregation-tree branch");
    }
    AggregationTreeNodeRef left = node.getBranch().getLeft();
    AggregationTreeNodeRef right = node.getBranch().getRight();
    validateNodeReference(left);
    validateNodeReference(right);
    long leafCount = Math.addExact(left.getLeafCount(), right.getLeafCount());
    byte[] leftMin = left.getMinKey().toByteArray();
    byte[] rightMin = right.getMinKey().toByteArray();
    byte[] expectedMin = Arrays.compareUnsigned(leftMin, rightMin) <= 0 ? leftMin : rightMin;
    if (leafCount != node.getLeafCount()
        || !Arrays.equals(expectedMin, node.getMinKey().toByteArray())
        || keyBit(leftMin, node.getBranch().getBit())
        || !keyBit(rightMin, node.getBranch().getBit())) {
      throw new IllegalArgumentException("aggregation-tree branch children are inconsistent");
    }
  }

  private static boolean keyBit(byte[] key, int bit) {
    return (key[bit / 8] & (0x80 >>> (bit % 8))) != 0;
  }

  private static ShardReference decodeShardReference(ByteBuffer records) {
    byte[] shardKey = new byte[32];
    byte[] payloadDigest = new byte[32];
    byte[] plannerStatisticsPayloadDigest = new byte[32];
    byte[] sidecarFormatsDigest = new byte[32];
    byte[] groupLayoutPayloadDigest = new byte[32];
    records.get(shardKey);
    records.get(payloadDigest);
    long payloadBytes = records.getLong();
    long entryCount = records.getLong();
    records.get(plannerStatisticsPayloadDigest);
    long plannerStatisticsPayloadBytes = records.getLong();
    records.get(sidecarFormatsDigest);
    records.get(groupLayoutPayloadDigest);
    long groupLayoutPayloadBytes = records.getLong();
    if (entryCount <= 0L
        || payloadBytes != Math.multiplyExact(entryCount, (long) RECORD_BYTES)
        || plannerStatisticsPayloadBytes <= 0L
        || groupLayoutPayloadBytes <= 0L) {
      throw new IllegalArgumentException("invalid reusable coverage shard reference");
    }
    return new ShardReference(
        shardKey,
        payloadDigest,
        payloadBytes,
        entryCount,
        plannerStatisticsPayloadDigest,
        plannerStatisticsPayloadBytes,
        sidecarFormatsDigest,
        groupLayoutPayloadDigest,
        groupLayoutPayloadBytes);
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

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }
}
