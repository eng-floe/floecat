/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.rpc.AggregationTreeBranch;
import ai.floedb.floecat.reconciler.rpc.AggregationTreeLeaf;
import ai.floedb.floecat.reconciler.rpc.AggregationTreeNode;
import ai.floedb.floecat.reconciler.rpc.AggregationTreeNodeRef;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReusableCoverageManifestAggregationTreeTest {
  @Test
  void rejectsOversizedCoverageShardBeforeReadingIt() {
    BlobStore blobs = mock(BlobStore.class);
    long entries =
        ReusableCoverageManifest.MAX_SHARD_BYTES / ReusableCoverageManifest.RECORD_BYTES + 1L;
    long payloadBytes = entries * ReusableCoverageManifest.RECORD_BYTES;
    var reference =
        new ReusableCoverageManifest.ShardReference(
            new byte[32],
            new byte[32],
            payloadBytes,
            entries,
            new byte[32],
            1L,
            new byte[32],
            new byte[32],
            1L);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ReusableCoverageManifest.readShard(
                blobs,
                ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef.getDefaultInstance(),
                reference,
                "/oversized"));
    verifyNoInteractions(blobs);
  }

  @Test
  void rejectsCoverageShardWhosePhysicalObjectExceedsItsCommitment() {
    BlobStore blobs = mock(BlobStore.class);
    var reference =
        new ReusableCoverageManifest.ShardReference(
            new byte[32],
            new byte[32],
            ReusableCoverageManifest.RECORD_BYTES,
            1L,
            new byte[32],
            1L,
            new byte[32],
            new byte[32],
            1L);
    when(blobs.getRangeAtMost("/oversized", 0L, ReusableCoverageManifest.RECORD_BYTES + 1))
        .thenReturn(new byte[ReusableCoverageManifest.RECORD_BYTES + 1]);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ReusableCoverageManifest.readShard(
                blobs,
                ai.floedb.floecat.reconciler.rpc.ReusableCoverageManifestRef.getDefaultInstance(),
                reference,
                "/oversized"));
  }

  @Test
  void walksAndResumesAContentAddressedBranch() throws Exception {
    byte[] leftKey = new byte[32];
    byte[] rightKey = new byte[32];
    rightKey[0] = (byte) 0x80;
    var left = leaf(leftKey, (byte) 1);
    var right = leaf(rightKey, (byte) 2);
    byte[] membership = sha256(new byte[] {3});
    var branch =
        AggregationTreeNode.newBuilder()
            .setFormatVersion(1)
            .setMembershipSha256(ByteString.copyFrom(membership))
            .setMinKey(ByteString.copyFrom(leftKey))
            .setLeafCount(2L)
            .addRecords(ByteString.copyFrom(new byte[] {4}))
            .setBranch(
                AggregationTreeBranch.newBuilder()
                    .setBit(0)
                    .setLeft(left.reference())
                    .setRight(right.reference()))
            .build();
    var root = reference(branch, membership, leftKey, 2L);
    String prefix = "/accounts/a/tables/t/reusable-artifacts/";
    BlobStore blobs = mock(BlobStore.class);
    stub(blobs, prefix, left.reference(), left.node());
    stub(blobs, prefix, right.reference(), right.node());
    stub(blobs, prefix, root, branch);

    var completed = new HashSet<String>();
    var rooted = new ArrayList<String>();
    ReusableCoverageManifest.walkAggregationTree(blobs, root, prefix, completed, rooted::add);

    List<String> expected =
        List.of(
            ReusableCoverageManifest.aggregationTreeNodeUri(prefix, root),
            ReusableCoverageManifest.aggregationTreeNodeUri(prefix, left.reference()),
            ReusableCoverageManifest.aggregationTreeNodeUri(prefix, right.reference()));
    assertEquals(expected, rooted);
    assertEquals(new HashSet<>(expected), completed);

    rooted.clear();
    ReusableCoverageManifest.walkAggregationTree(blobs, root, prefix, completed, rooted::add);
    assertEquals(List.of(), rooted);
    for (String uri : expected) {
      verify(blobs)
          .getRangeAtMost(
              org.mockito.ArgumentMatchers.eq(uri),
              org.mockito.ArgumentMatchers.eq(0L),
              org.mockito.ArgumentMatchers.anyInt());
    }
  }

  private static LeafFixture leaf(byte[] key, byte value) throws Exception {
    byte[] membership = sha256(new byte[] {value});
    var node =
        AggregationTreeNode.newBuilder()
            .setFormatVersion(1)
            .setMembershipSha256(ByteString.copyFrom(membership))
            .setMinKey(ByteString.copyFrom(key))
            .setLeafCount(1L)
            .addRecords(ByteString.copyFrom(new byte[] {value}))
            .setLeaf(
                AggregationTreeLeaf.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setArtifactUri("/artifact-" + value)
                    .setArtifactPayloadBytes(1L)
                    .setArtifactPayloadSha256(ByteString.copyFrom(sha256(new byte[] {value, 1}))))
            .build();
    return new LeafFixture(node, reference(node, membership, key, 1L));
  }

  private static AggregationTreeNodeRef reference(
      AggregationTreeNode node, byte[] membership, byte[] minKey, long leafCount) throws Exception {
    byte[] payload = node.toByteArray();
    return AggregationTreeNodeRef.newBuilder()
        .setPayloadBytes(payload.length)
        .setPayloadSha256(ByteString.copyFrom(sha256(payload)))
        .setMembershipSha256(ByteString.copyFrom(membership))
        .setMinKey(ByteString.copyFrom(minKey))
        .setLeafCount(leafCount)
        .build();
  }

  private static void stub(
      BlobStore blobs, String prefix, AggregationTreeNodeRef reference, AggregationTreeNode node) {
    when(blobs.getRangeAtMost(
            ReusableCoverageManifest.aggregationTreeNodeUri(prefix, reference),
            0L,
            Math.toIntExact(reference.getPayloadBytes() + 1L)))
        .thenReturn(node.toByteArray());
  }

  private static byte[] sha256(byte[] payload) throws Exception {
    return MessageDigest.getInstance("SHA-256").digest(payload);
  }

  private record LeafFixture(AggregationTreeNode node, AggregationTreeNodeRef reference) {}
}
