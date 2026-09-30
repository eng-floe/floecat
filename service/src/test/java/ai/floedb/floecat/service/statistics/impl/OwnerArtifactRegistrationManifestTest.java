/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.rpc.ExternalManifestChunkCommitment;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndexRef;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestDomain;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactObjectReference;
import ai.floedb.floecat.reconciler.rpc.OwnerArtifactRegistrationManifestRef;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import org.junit.jupiter.api.Test;

class OwnerArtifactRegistrationManifestTest {
  private static final String URI = "/registration.bin";

  @Test
  void readsAndValidatesOneCommittedChunkWithOneRangeGet() throws Exception {
    byte[] payload = chunk(0L, List.of(object("table"), object("column-0000000000000000001")));
    BlobStore blobs = mock(BlobStore.class);
    when(blobs.getRange(URI, 0L, payload.length)).thenReturn(payload);
    var commitment = commitment(payload, 2L, 2L);

    var batch =
        OwnerArtifactRegistrationManifest.readChunk(blobs, descriptor(payload, 2L), 0L, commitment);

    assertEquals(2L, batch.objectCount());
    assertEquals(2L, batch.aggregateStatsTargetCount());
    verify(blobs).getRange(URI, 0L, payload.length);
  }

  @Test
  void rejectsAChunkWhoseBytesDoNotMatchItsCommitment() throws Exception {
    byte[] payload = chunk(0L, List.of(object("table")));
    BlobStore blobs = mock(BlobStore.class);
    when(blobs.getRange(URI, 0L, payload.length)).thenReturn(payload);
    var commitment =
        commitment(payload, 1L, 1L).toBuilder()
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OwnerArtifactRegistrationManifest.readChunk(
                blobs, descriptor(payload, 1L), 0L, commitment));
  }

  @Test
  void rejectsAZeroChunkCommitmentBeforeReadingItsIndex() {
    BlobStore blobs = mock(BlobStore.class);
    byte[] digest = new byte[32];
    var reference =
        ExternalManifestCommitmentIndexRef.newBuilder()
            .setFormatVersion(1)
            .setDomain(ExternalManifestDomain.EMD_OWNER_ARTIFACT_REGISTRATION)
            .setUri(
                Keys.snapshotOwnerManifestCommitmentIndexBlobUri(
                    "acct", "table", 42L, "registration", "00".repeat(32)))
            .setPayloadBytes(1L)
            .setPayloadSha256(ByteString.copyFrom(digest))
            .setChunkCount(0L)
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExternalManifestCommitments.load(
                ExternalManifestCommitmentCache.forTesting(),
                blobs,
                reference,
                ExternalManifestDomain.EMD_OWNER_ARTIFACT_REGISTRATION,
                "acct",
                "table",
                42L,
                1L,
                1L,
                0L,
                0L,
                1L,
                0,
                1024,
                10,
                10));
    verifyNoInteractions(blobs);
  }

  @Test
  void acceptsAZeroChunkCoverageCommitment() throws Exception {
    BlobStore blobs = mock(BlobStore.class);
    byte[] index =
        ExternalManifestCommitmentIndex.newBuilder()
            .setFormatVersion(1)
            .setDomain(ExternalManifestDomain.EMD_REUSABLE_COVERAGE)
            .setChunkSizeLimit(OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES)
            .setFixedRecordBytes(ReusableCoverageManifest.RECORD_BYTES)
            .build()
            .toByteArray();
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(index);
    String uri =
        Keys.snapshotOwnerManifestCommitmentIndexBlobUri(
            "acct", "table", 42L, "coverage", java.util.HexFormat.of().formatHex(digest));
    var reference =
        ExternalManifestCommitmentIndexRef.newBuilder()
            .setFormatVersion(1)
            .setDomain(ExternalManifestDomain.EMD_REUSABLE_COVERAGE)
            .setUri(uri)
            .setPayloadBytes(index.length)
            .setPayloadSha256(ByteString.copyFrom(digest))
            .build();
    when(blobs.getRangeAtMost(uri, 0L, index.length + 1)).thenReturn(index);
    var cache = ExternalManifestCommitmentCache.forTesting();

    var loaded =
        ExternalManifestCommitments.load(
            cache,
            blobs,
            reference,
            ExternalManifestDomain.EMD_REUSABLE_COVERAGE,
            "acct",
            "table",
            42L,
            0L,
            0L,
            0L,
            0L,
            0L,
            ReusableCoverageManifest.RECORD_BYTES,
            OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES,
            OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES
                / ReusableCoverageManifest.RECORD_BYTES,
            0);

    assertEquals(0, loaded.getChunksCount());
    ExternalManifestCommitments.load(
        cache,
        blobs,
        reference,
        ExternalManifestDomain.EMD_REUSABLE_COVERAGE,
        "acct",
        "table",
        42L,
        0L,
        0L,
        0L,
        0L,
        0L,
        ReusableCoverageManifest.RECORD_BYTES,
        OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES,
        OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES
            / ReusableCoverageManifest.RECORD_BYTES,
        0);
    verify(blobs, times(1)).getRangeAtMost(uri, 0L, index.length + 1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExternalManifestCommitments.load(
                cache,
                blobs,
                reference,
                ExternalManifestDomain.EMD_REUSABLE_COVERAGE,
                "acct",
                "table",
                42L,
                1L,
                0L,
                0L,
                0L,
                0L,
                ReusableCoverageManifest.RECORD_BYTES,
                OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES,
                OwnerArtifactRegistrationManifest.DEFAULT_READ_BYTES
                    / ReusableCoverageManifest.RECORD_BYTES,
                0));
    verify(blobs, times(2)).getRangeAtMost(uri, 0L, index.length + 1);
  }

  private static OwnerArtifactObjectReference object(String target) {
    return OwnerArtifactObjectReference.newBuilder()
        .setPayloadUri("/payload/" + target)
        .setPayloadBytes(1L)
        .setPayloadSha256(ByteString.copyFrom(new byte[32]))
        .addAggregateStatsTargetStorageIds(target)
        .build();
  }

  private static byte[] chunk(long ordinal, List<OwnerArtifactObjectReference> objects) {
    int size = OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES;
    for (var object : objects) {
      size += Integer.BYTES + object.getSerializedSize();
    }
    ByteBuffer bytes = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
    bytes.put("FLOREGC1".getBytes(StandardCharsets.US_ASCII));
    bytes.putInt(OwnerArtifactRegistrationManifest.FORMAT_VERSION);
    bytes.putInt(OwnerArtifactRegistrationManifest.CHUNK_HEADER_BYTES);
    bytes.putLong(ordinal);
    bytes.putLong(objects.size());
    bytes.putLong(0L).putLong(0L).putLong(objects.size()).putLong(0L);
    for (var object : objects) {
      byte[] encoded = object.toByteArray();
      bytes.putInt(encoded.length).put(encoded);
    }
    return bytes.array();
  }

  private static ExternalManifestChunkCommitment commitment(
      byte[] payload, long records, long aggregateTargets) throws Exception {
    return ExternalManifestChunkCommitment.newBuilder()
        .setPayloadOffset(0L)
        .setPayloadBytes(payload.length)
        .setRecordCount(records)
        .setAggregateStatsTargetCount(aggregateTargets)
        .setPayloadSha256(ByteString.copyFrom(MessageDigest.getInstance("SHA-256").digest(payload)))
        .build();
  }

  private static OwnerArtifactRegistrationManifestRef descriptor(byte[] payload, long objects) {
    return OwnerArtifactRegistrationManifestRef.newBuilder()
        .setFormatVersion(OwnerArtifactRegistrationManifest.FORMAT_VERSION)
        .setUri(URI)
        .setPayloadBytes(payload.length)
        .setPayloadSha256(ByteString.copyFrom(new byte[32]))
        .setObjectCount(objects)
        .setAggregateStatsTargetCount(objects)
        .setCommitmentIndex(
            ExternalManifestCommitmentIndexRef.newBuilder()
                .setFormatVersion(1)
                .setDomain(ExternalManifestDomain.EMD_OWNER_ARTIFACT_REGISTRATION)
                .setUri("/index.pb")
                .setPayloadBytes(1L)
                .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                .setChunkCount(1L))
        .build();
  }
}
