/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class GenerationArtifactMapTest {
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder().setAccountId("account-1").setId("table-1").build();

  @Test
  void generationUsesTheCaptureIndexWithoutWritingSmallIndexBlobs() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    ReusableArtifactIndexStore indexes = new ReusableArtifactIndexStore(blobs);
    String file = "s3://bucket/data.parquet";
    StatsObjectDescriptor artifact =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri("/bundles/group-1.pb")
            .setPayloadBytes(123L)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    ReusableArtifactBundleReference bundle =
        ReusableArtifactBundleReference.newBuilder()
            .setArtifact(artifact)
            .addFileStats(
                ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(file)
                    .setSourceFingerprint("stats-source")
                    .setStatsCaptureSignature("stats-capture"))
            .addIndexArtifacts(
                ReusableIndexArtifactMetadata.newBuilder()
                    .setFilePath(file)
                    .setSourceFingerprint("index-source")
                    .setIndexCaptureSignature("index-capture"))
            .build();
    var index =
        indexes.append(
            "/accounts/account-1/tables/table-1/reusable-index/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(bundle));
    assertThat(blobs.list("/accounts/account-1/tables/table-1/reusable-index/", 100, "").keys())
        .isEmpty();

    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(TABLE_ID.getAccountId())
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(42L)
            .setReusableArtifactIndex(index)
            .build();
    byte[] manifestBytes = manifest.toByteArray();
    String manifestUri = contentAddressedUri(manifestBytes);
    blobs.put(manifestUri, manifestBytes, "application/x-protobuf");
    GenerationArtifactMap map =
        new GenerationArtifactMap(
            new InMemoryPointerStore(),
            blobs,
            new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)));

    map.register(TABLE_ID, 42L, "generation-1", manifestUri, manifestBytes.length);

    assertThat(map.lookupStats(TABLE_ID, 42L, "generation-1", file)).isPresent();
    assertThat(map.lookupIndex(TABLE_ID, 42L, "generation-1", file)).isPresent();
    assertThat(map.countStats(TABLE_ID, 42L, "generation-1")).isEqualTo(1);
    assertThat(map.countIndexes(TABLE_ID, 42L, "generation-1")).isEqualTo(1);
    assertThat(
            map.page(
                    TABLE_ID,
                    42L,
                    "generation-1",
                    ReusableArtifactIndexStore.EntryKind.FILE_STATS,
                    1,
                    "")
                .entries())
        .hasSize(1);
  }

  @Test
  void registeredGenerationDoesNotSilentlyFallBackWhenItsManifestIsMissing() {
    GenerationArtifactMap map =
        new GenerationArtifactMap(
            new InMemoryPointerStore(),
            new InMemoryBlobStore(),
            new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)));
    String missingUri = "/capture/" + "00".repeat(32) + ".pb";
    map.register(TABLE_ID, 42L, "generation-1", missingUri, 123L);

    assertThatThrownBy(
            () -> map.lookupStats(TABLE_ID, 42L, "generation-1", "s3://bucket/data.parquet"))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("manifest is missing");
  }

  @Test
  void registeredGenerationAuthenticatesManifestLengthAndDigest() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    byte[] original =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(TABLE_ID.getAccountId())
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(42L)
            .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
            .build()
            .toByteArray();
    String uri = contentAddressedUri(original);
    blobs.put(uri, original, "application/x-protobuf");
    GenerationArtifactMap map = new GenerationArtifactMap(new InMemoryPointerStore(), blobs, null);
    map.register(TABLE_ID, 42L, "generation-1", uri, original.length);
    byte[] replacement = original.clone();
    replacement[replacement.length - 1] ^= 1;
    blobs.put(uri, replacement, "application/x-protobuf");

    assertThatThrownBy(() -> map.manifest(TABLE_ID, 42L, "generation-1"))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("metadata mismatch");
  }

  private static String contentAddressedUri(byte[] bytes) {
    try {
      return "/capture/"
          + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
          + ".pb";
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }
}
