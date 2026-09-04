/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IndexArtifactRepositoryTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder().setAccountId("a").setId("t").setKind(ResourceKind.RK_TABLE).build();
  private static final ImmutableBlobCache DISABLED_BLOB_CACHE =
      new ImmutableBlobCache(false, 1L, Duration.ZERO);

  private static IndexArtifactRepository createRepository(PointerStore pointers, BlobStore blobs) {
    return new IndexArtifactRepository(
        pointers, blobs, DISABLED_BLOB_CACHE, new TableBlobReachabilityGuard());
  }

  private static IndexArtifactRepository createRepository(
      PointerStore pointers, BlobStore blobs, ImmutableBlobCache cache) {
    return new IndexArtifactRepository(pointers, blobs, cache, new TableBlobReachabilityGuard());
  }

  private static void installAccountDeletionFence(InMemoryPointerStore pointers, String accountId) {
    List<PointerStore.CasOp> creates = new ArrayList<>();
    String markerKey = Keys.accountDeletionMarker(accountId);
    creates.add(
        new PointerStore.CasUpsert(
            markerKey, 0L, PointerReferences.opaqueMarkerPointer(markerKey, "deleting", 1L)));
    for (String shardKey : Keys.accountDeletionFenceShards(accountId)) {
      creates.add(
          new PointerStore.CasUpsert(
              shardKey, 0L, PointerReferences.opaqueMarkerPointer(shardKey, "deleting", 1L)));
    }
    assertThat(pointers.compareAndSetBatch(creates)).isTrue();
  }

  @Test
  void requiresBlobCacheDependency() {
    assertThatThrownBy(
            () ->
                new IndexArtifactRepository(
                    new InMemoryPointerStore(),
                    new InMemoryBlobStore(),
                    null,
                    new TableBlobReachabilityGuard()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("blobCache");
  }

  @Test
  void directArtifactWriteDeletesBlobWhenAccountFenceWinsPublicationRace() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    AtomicBoolean installFence = new AtomicBoolean(true);
    InMemoryBlobStore blobs =
        new InMemoryBlobStore() {
          @Override
          public void put(String uri, byte[] bytes, String contentType) {
            super.put(uri, bytes, contentType);
            if (uri.startsWith(Keys.tableBlobPrefix(TABLE_ID.getAccountId(), TABLE_ID.getId()))
                && installFence.compareAndSet(true, false)) {
              installAccountDeletionFence(pointers, TABLE_ID.getAccountId());
            }
          }
        };
    IndexArtifactRepository repository = createRepository(pointers, blobs);

    assertThatThrownBy(
            () -> repository.putIndexArtifact(indexRecord(714L, "s3://bucket/file.parquet")))
        .isInstanceOf(BaseResourceRepository.AccountDeletionInProgressException.class);

    assertThat(
            blobs
                .list(Keys.tableBlobPrefix(TABLE_ID.getAccountId(), TABLE_ID.getId()), 100, "")
                .keys())
        .isEmpty();
    assertThat(
            pointers.countByPrefix(
                Keys.snapshotRootPrefix(TABLE_ID.getAccountId(), TABLE_ID.getId())))
        .isZero();
  }

  @Test
  void atomicDirectArtifactWriteFencesConcurrentGenerationFinalization() {
    InMemoryPointerStore delegate = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    long snapshotId = 725L;
    String activeKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId);
    String directPrefix =
        Keys.snapshotIndexArtifactGenerationPrefix(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "direct");
    AtomicBoolean finalizeBeforeArtifactCommit = new AtomicBoolean(true);
    AtomicBoolean discardedCompletion = new AtomicBoolean();
    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(delegate) {
          @Override
          public boolean compareAndSetBatch(List<CasOp> ops) {
            boolean writesDirectArtifact =
                ops.stream().anyMatch(op -> op.key().startsWith(directPrefix));
            if (writesDirectArtifact && finalizeBeforeArtifactCommit.compareAndSet(true, false)) {
              var active = delegate.get(activeKey).orElseThrow();
              assertThat(
                      delegate.compareAndSet(
                          activeKey,
                          active.getVersion(),
                          PointerReferences.opaqueMarkerPointer(
                              activeKey, "final-generation", active.getVersion() + 1L)))
                  .isTrue();
              delegate.deleteByPrefix(directPrefix);
            }
            return super.compareAndSetBatch(ops);
          }
        };
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    IndexArtifactRecord record = indexRecord(snapshotId, "s3://bucket/file.parquet");

    assertThatThrownBy(
            () ->
                repository.putIndexArtifactWithCompletion(
                    record,
                    com.google.protobuf.util.Timestamps.fromMillis(1L),
                    ignored -> List.of(new PointerStore.CasCheckAbsent("/receipt")),
                    ignored -> discardedCompletion.set(true)))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class);

    assertThat(discardedCompletion).isTrue();
    assertThat(pointers.countByPrefix(directPrefix)).isZero();
    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, record.getTarget())).isEmpty();
    assertThat(
            blobs
                .list(Keys.tableBlobPrefix(TABLE_ID.getAccountId(), TABLE_ID.getId()), 100, "")
                .keys())
        .isEmpty();
  }

  @Test
  void bundledIndexWrappersResolveAllTargetsWithOneBlobRead() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = spy(new InMemoryBlobStore());
    IndexArtifactRepository repository =
        createRepository(
            pointers, blobs, new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)));
    long snapshotId = 714L;
    String generationId = "full-rescan-bundled";
    IndexArtifactRecord first = indexRecord(snapshotId, "s3://bucket/first.parquet");
    IndexArtifactRecord second = indexRecord(snapshotId, "s3://bucket/second.parquet");
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(first)
            .addIndexArtifacts(second)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String workerPrefix = "/worker-output/index-artifacts/";
    String bundleUri = "/worker-output/reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");
    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        generationId,
        workerPrefix,
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                "file:s3://bucket/first.parquet", bundleUri, bundle.length, digest),
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                "file:s3://bucket/second.parquet", bundleUri, bundle.length, digest)));
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        generationId,
        captureManifest(snapshotId, 2, 2, "customer_id").toByteArray());

    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, first.getTarget()))
        .contains(first);
    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, second.getTarget()))
        .contains(second);
    verify(blobs, times(1)).get(bundleUri);
  }

  @Test
  void bundledIndexReferenceRejectsUriThatDoesNotMatchDigest() {
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex("bundle"));
    String workerPrefix = "/worker-output/index-artifacts/";

    assertThatThrownBy(
            () ->
                repository.registerPrewrittenIndexArtifactReferencesInGeneration(
                    TABLE_ID,
                    715L,
                    "full-rescan-bundled",
                    workerPrefix,
                    List.of(
                        new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                            "file:s3://bucket/file.parquet",
                            "/worker-output/reuse-bundles/latest.pb",
                            6L,
                            digest))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid prewritten index artifact reference");
    verifyNoInteractions(blobs);
  }

  @Test
  void bundledIndexReferenceRejectsAnOwnedSidecarDeletedBeforePublication() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    String sidecar = managedSidecar(snapshotId, targetStorageId, new byte[] {1, 2, 3});
    IndexArtifactRecord record =
        indexRecord(snapshotId, "s3://bucket/file.parquet").toBuilder()
            .setArtifactUri(sidecar)
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String workerPrefix = workerStatsPrefix(snapshotId) + "index-artifacts/";
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThatThrownBy(
            () ->
                repository.registerPrewrittenIndexArtifactReferencesInGeneration(
                    TABLE_ID,
                    snapshotId,
                    "full-rescan-bundled",
                    workerPrefix,
                    List.of(
                        new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                            targetStorageId, bundleUri, bundle.length, digest))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("shared index sidecar is missing");

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "full-rescan-bundled")))
        .isZero();
  }

  @Test
  void bundledIndexReferenceRejectsSidecarFromDifferentCarrierGeneration() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    byte[] sidecarContent = new byte[] {1, 2, 3};
    String sidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            "other",
            "group-job",
            "lease-1",
            targetStorageId,
            Hashing.sha256Hex(sidecarContent));
    blobs.put(sidecar, sidecarContent, "application/octet-stream");
    IndexArtifactRecord record =
        indexRecord(snapshotId, "s3://bucket/file.parquet").toBuilder()
            .setArtifactUri(sidecar)
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThatThrownBy(
            () ->
                repository.registerPrewrittenIndexArtifactReferencesInGeneration(
                    TABLE_ID,
                    snapshotId,
                    "full-rescan-bundled",
                    workerStatsPrefix(snapshotId),
                    List.of(
                        new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                            targetStorageId, bundleUri, bundle.length, digest))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("sidecar is outside its carrier or inherited generation");
  }

  @Test
  void bundledIndexReferenceRejectsSidecarFromDifferentProducerInCarrierGeneration() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    byte[] sidecarContent = new byte[] {1, 2, 3};
    String sidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            "bundled",
            "other-group-job",
            "other-lease",
            targetStorageId,
            Hashing.sha256Hex(sidecarContent));
    blobs.put(sidecar, sidecarContent, "application/octet-stream");
    IndexArtifactRecord record =
        indexRecord(snapshotId, "s3://bucket/file.parquet").toBuilder()
            .setArtifactUri(sidecar)
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThatThrownBy(
            () ->
                repository.registerPrewrittenIndexArtifactReferencesInGeneration(
                    TABLE_ID,
                    snapshotId,
                    "full-rescan-bundled",
                    workerStatsPrefix(snapshotId) + "index-artifacts/",
                    Keys.reconcileFileGroupIndexSidecarObjectPrefix(
                        TABLE_ID.getAccountId(),
                        TABLE_ID.getId(),
                        snapshotId,
                        "bundled",
                        "group-job",
                        "lease-1"),
                    Set.of(),
                    List.of(
                        new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                            targetStorageId, bundleUri, bundle.length, digest))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("sidecar is outside its producer prefix");
  }

  @Test
  void inheritedBundleSidecarUsesCarrierGenerationRatherThanPublishingGeneration() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    byte[] sidecarContent = new byte[] {4, 5, 6};
    String sidecar = managedSidecar(snapshotId, targetStorageId, sidecarContent);
    blobs.put(sidecar, sidecarContent, "application/octet-stream");
    IndexArtifactRecord record =
        indexRecord(snapshotId, "s3://bucket/file.parquet").toBuilder()
            .setArtifactUri(sidecar)
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-successor",
        Keys.tableTargetStatsBlobPrefix(TABLE_ID.getAccountId(), TABLE_ID.getId()),
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                targetStorageId, bundleUri, bundle.length, digest)));

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(),
                    TABLE_ID.getId(),
                    snapshotId,
                    "full-rescan-successor")))
        .isEqualTo(1);
  }

  @Test
  void currentBundleAcceptsManagedSidecarFromExplicitlyInheritedGeneration() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    byte[] sidecarContent = new byte[] {7, 8, 9};
    String sidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            "prior-parent",
            "prior-group",
            "prior-lease",
            targetStorageId,
            Hashing.sha256Hex(sidecarContent));
    blobs.put(sidecar, sidecarContent, "application/octet-stream");
    IndexArtifactRecord record =
        indexRecord(snapshotId, "s3://bucket/file.parquet").toBuilder()
            .setArtifactUri(sidecar)
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-bundled",
        workerStatsPrefix(snapshotId) + "index-artifacts/",
        Keys.reconcileFileGroupIndexSidecarObjectPrefix(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            "bundled",
            "group-job",
            "lease-1"),
        Set.of(new Keys.GenerationKey(snapshotId, "full-rescan-prior-parent")),
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                targetStorageId, bundleUri, bundle.length, digest)));

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "full-rescan-bundled")))
        .isEqualTo(1);
  }

  @Test
  void selectedBundleAuthenticatesItsActualTransitiveSidecarGeneration() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    String filePath = "s3://bucket/file.parquet";
    String targetStorageId = "file:" + filePath;
    byte[] sidecarContent = new byte[] {10, 11, 12};
    String sidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            714L,
            "ancestor-parent",
            "ancestor-group",
            "ancestor-lease",
            targetStorageId,
            Hashing.sha256Hex(sidecarContent));
    IndexArtifactRecord record =
        indexRecord(714L, filePath).toBuilder().setArtifactUri(sidecar).build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        Keys.reconcileFileGroupStatsObjectPrefix(
                TABLE_ID.getAccountId(),
                TABLE_ID.getId(),
                715L,
                "prior-parent",
                "prior-group",
                "prior-lease")
            + "reuse-bundles/"
            + Hashing.sha256Hex(bundle)
            + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    Set<Keys.GenerationKey> generations =
        repository.inheritedManagedSidecarGenerations(
            TABLE_ID,
            List.of(
                new ReusableArtifactBundleSelection(
                    "reuse-bundle:prior-group",
                    bundleUri,
                    bundle.length,
                    digest,
                    List.of(),
                    List.of(filePath))));

    assertThat(generations)
        .containsExactly(new Keys.GenerationKey(714L, "full-rescan-ancestor-parent"));
  }

  @Test
  void selectedBundleAllowsExternalIndexSidecarsWithoutAnInheritedGeneration() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    String filePath = "s3://bucket/file.parquet";
    IndexArtifactRecord record =
        indexRecord(714L, filePath).toBuilder()
            .setArtifactUri("s3://external-indexes/file.parquet")
            .build();
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(715L) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThat(
            repository.inheritedManagedSidecarGenerations(
                TABLE_ID,
                List.of(
                    new ReusableArtifactBundleSelection(
                        "reuse-bundle:prior-group",
                        bundleUri,
                        bundle.length,
                        digest,
                        List.of(),
                        List.of(filePath)))))
        .isEmpty();
  }

  @Test
  void selectedBundleValidationUsesTheImmutableBlobCache() {
    BlobStore blobs = mock(BlobStore.class);
    ImmutableBlobCache cache = new ImmutableBlobCache(true, 1024 * 1024, Duration.ZERO);
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs, cache);
    String filePath = "s3://bucket/file.parquet";
    String targetStorageId = "file:" + filePath;
    String sidecar = managedSidecar(714L, targetStorageId, new byte[] {1});
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(indexRecord(714L, filePath).toBuilder().setArtifactUri(sidecar))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(715L) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    when(blobs.get(bundleUri)).thenReturn(bundle);
    when(blobs.head(bundleUri))
        .thenReturn(Optional.of(BlobHeader.newBuilder().setContentLength(bundle.length).build()));
    ReusableArtifactBundleSelection selection =
        new ReusableArtifactBundleSelection(
            "reuse-bundle:prior-group",
            bundleUri,
            bundle.length,
            digest,
            List.of(),
            List.of(filePath));

    repository.inheritedManagedSidecarGenerations(TABLE_ID, List.of(selection));
    repository.inheritedManagedSidecarGenerations(TABLE_ID, List.of(selection));

    verify(blobs, times(1)).get(bundleUri);
    verify(blobs, times(2)).head(bundleUri);
  }

  @Test
  void selectedBundleRejectsDeclaredLengthThatDiffersFromStoredLength() {
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    String filePath = "s3://bucket/file.parquet";
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(indexRecord(714L, filePath))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(715L) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    when(blobs.head(bundleUri))
        .thenReturn(
            Optional.of(BlobHeader.newBuilder().setContentLength(bundle.length + 1).build()));

    assertThatThrownBy(
            () ->
                repository.inheritedManagedSidecarGenerations(
                    TABLE_ID,
                    List.of(
                        new ReusableArtifactBundleSelection(
                            "reuse-bundle:prior-group",
                            bundleUri,
                            bundle.length,
                            digest,
                            List.of(),
                            List.of(filePath)))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("metadata does not match");
    verify(blobs, never()).get(bundleUri);
  }

  @Test
  void selectedBundleWrapsUnsupportedFormatAsCorruption() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    String filePath = "s3://bucket/file.parquet";
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(2)
            .addIndexArtifacts(indexRecord(714L, filePath))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(715L) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThatThrownBy(
            () ->
                repository.inheritedManagedSidecarGenerations(
                    TABLE_ID,
                    List.of(
                        new ReusableArtifactBundleSelection(
                            "reuse-bundle:prior-group",
                            bundleUri,
                            bundle.length,
                            digest,
                            List.of(),
                            List.of(filePath)))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("unsupported format");
  }

  @Test
  void selectedBundleRejectsASidecarGenerationOwnedByAnotherTable() {
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(new InMemoryPointerStore(), blobs);
    String filePath = "s3://bucket/file.parquet";
    String selectedSidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            714L,
            "ancestor-parent",
            "ancestor-group",
            "ancestor-lease",
            "file:" + filePath,
            Hashing.sha256Hex(new byte[] {1}));
    String sidecar =
        Keys.reconcileFileGroupIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            "other-table",
            714L,
            "ancestor-parent",
            "ancestor-group",
            "ancestor-lease",
            "file:" + filePath,
            Hashing.sha256Hex(new byte[] {1}));
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(
                indexRecord(714L, filePath).toBuilder().setArtifactUri(selectedSidecar))
            .addIndexArtifacts(
                indexRecord(714L, "s3://bucket/unselected.parquet").toBuilder()
                    .setArtifactUri(sidecar))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(715L) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");

    assertThatThrownBy(
            () ->
                repository.inheritedManagedSidecarGenerations(
                    TABLE_ID,
                    List.of(
                        new ReusableArtifactBundleSelection(
                            "reuse-bundle:prior-group",
                            bundleUri,
                            bundle.length,
                            digest,
                            List.of(),
                            List.of(filePath)))))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("sidecar is outside the owning table generation");
  }

  @Test
  void bundledIndexPublicationChecksOwnedSidecarsConcurrently() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String firstTarget = "file:s3://bucket/first.parquet";
    String secondTarget = "file:s3://bucket/second.parquet";
    String firstSidecar = managedSidecar(snapshotId, firstTarget, new byte[] {1});
    String secondSidecar = managedSidecar(snapshotId, secondTarget, new byte[] {2});
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(
                indexRecord(snapshotId, "s3://bucket/first.parquet").toBuilder()
                    .setArtifactUri(firstSidecar))
            .addIndexArtifacts(
                indexRecord(snapshotId, "s3://bucket/second.parquet").toBuilder()
                    .setArtifactUri(secondSidecar))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    when(blobs.get(bundleUri)).thenReturn(bundle);
    CountDownLatch bothStarted = new CountDownLatch(2);
    AtomicInteger active = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    when(blobs.head(anyString()))
        .thenAnswer(
            invocation -> {
              int current = active.incrementAndGet();
              peak.accumulateAndGet(current, Math::max);
              bothStarted.countDown();
              bothStarted.await(1, TimeUnit.SECONDS);
              active.decrementAndGet();
              return Optional.of(BlobHeader.getDefaultInstance());
            });

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-bundled",
        workerStatsPrefix(snapshotId) + "index-artifacts/",
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                firstTarget, bundleUri, bundle.length, digest),
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                secondTarget, bundleUri, bundle.length, digest)));

    assertThat(peak.get()).isEqualTo(2);
  }

  @Test
  void bundledIndexPublicationChecksEachUniqueSidecarOnceWithoutReadingOrRewritingIt() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 716L;
    String firstTarget = "file:s3://bucket/first.parquet";
    String secondTarget = "file:s3://bucket/second.parquet";
    String sharedSidecar = managedSidecar(snapshotId, firstTarget, new byte[8 * 1024 * 1024]);
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(
                indexRecord(snapshotId, "s3://bucket/first.parquet").toBuilder()
                    .setArtifactUri(sharedSidecar))
            .addIndexArtifacts(
                indexRecord(snapshotId, "s3://bucket/second.parquet").toBuilder()
                    .setArtifactUri(sharedSidecar))
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    when(blobs.get(bundleUri)).thenReturn(bundle);
    when(blobs.head(sharedSidecar)).thenReturn(Optional.of(BlobHeader.getDefaultInstance()));

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-bundled",
        workerStatsPrefix(snapshotId) + "index-artifacts/",
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                firstTarget, bundleUri, bundle.length, digest),
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                secondTarget, bundleUri, bundle.length, digest)));

    verify(blobs, times(1)).head(sharedSidecar);
    verify(blobs, never()).get(sharedSidecar);
    verify(blobs, never()).put(anyString(), any(byte[].class), anyString());
  }

  @Test
  void bundledIndexPublicationLimitsSharedSidecarChecksToFifty() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 717L;
    var bundleBuilder = ReusableArtifactBundlePayload.newBuilder().setFormatVersion(1);
    List<String> targets = new ArrayList<>();
    for (int index = 0; index < 60; index++) {
      String filePath = "s3://bucket/file-" + index + ".parquet";
      String target = "file:" + filePath;
      targets.add(target);
      bundleBuilder.addIndexArtifacts(
          indexRecord(snapshotId, filePath).toBuilder()
              .setArtifactUri(managedSidecar(snapshotId, target, new byte[] {(byte) index})));
    }
    byte[] bundle = bundleBuilder.build().toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String bundleUri =
        workerStatsPrefix(snapshotId) + "reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    when(blobs.get(bundleUri)).thenReturn(bundle);
    CountDownLatch fiftyStarted = new CountDownLatch(50);
    AtomicInteger active = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    when(blobs.head(anyString()))
        .thenAnswer(
            invocation -> {
              int current = active.incrementAndGet();
              peak.accumulateAndGet(current, Math::max);
              fiftyStarted.countDown();
              if (!fiftyStarted.await(2, TimeUnit.SECONDS)) {
                throw new AssertionError("fifty shared-sidecar checks did not start concurrently");
              }
              active.decrementAndGet();
              return Optional.of(BlobHeader.getDefaultInstance());
            });

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-bundled",
        workerStatsPrefix(snapshotId) + "index-artifacts/",
        targets.stream()
            .map(
                target ->
                    new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                        target, bundleUri, bundle.length, digest))
            .toList());

    assertThat(peak.get()).isEqualTo(50);
    verify(blobs, times(60)).head(anyString());
  }

  @Test
  void bundledIndexReadRejectsPayloadThatDoesNotMatchContentAddress() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 716L;
    String generationId = "full-rescan-bundled";
    IndexArtifactRecord record = indexRecord(snapshotId, "s3://bucket/file.parquet");
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String workerPrefix = "/worker-output/index-artifacts/";
    String bundleUri = "/worker-output/reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");
    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        generationId,
        workerPrefix,
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                "file:s3://bucket/file.parquet", bundleUri, bundle.length, digest)));
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        generationId,
        captureManifest(snapshotId, 1, 1, "customer_id").toByteArray());
    blobs.put(
        bundleUri,
        ReusableArtifactBundlePayload.newBuilder().setFormatVersion(1).build().toByteArray(),
        "application/x-protobuf");

    assertThatThrownBy(() -> repository.getIndexArtifact(TABLE_ID, snapshotId, record.getTarget()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("digest mismatch");
  }

  @Test
  void bundledIndexPublicationRejectsABundleDeletedAfterItWasCached() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository =
        createRepository(
            pointers, blobs, new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)));
    long snapshotId = 716L;
    String targetStorageId = "file:s3://bucket/file.parquet";
    IndexArtifactRecord record = indexRecord(snapshotId, "s3://bucket/file.parquet");
    byte[] bundle =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addIndexArtifacts(record)
            .build()
            .toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(bundle));
    String workerPrefix = "/worker-output/index-artifacts/";
    String bundleUri = "/worker-output/reuse-bundles/" + Hashing.sha256Hex(bundle) + ".pb";
    blobs.put(bundleUri, bundle, "application/x-protobuf");
    var reference =
        new IndexArtifactRepository.PrewrittenIndexArtifactReference(
            targetStorageId, bundleUri, bundle.length, digest);
    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID, snapshotId, "generation-one", workerPrefix, List.of(reference));
    blobs.delete(bundleUri);

    assertThatThrownBy(
            () ->
                repository.registerPrewrittenIndexArtifactReferencesInGeneration(
                    TABLE_ID, snapshotId, "generation-two", workerPrefix, List.of(reference)))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("bundle is missing");
    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "generation-two")))
        .isZero();
  }

  @Test
  void prewrittenReferencesUseOptimisticPointerBatchesWithoutPointerReads() {
    InMemoryPointerStore delegate = new InMemoryPointerStore();
    AtomicInteger batchCalls = new AtomicInteger();
    AtomicInteger getCalls = new AtomicInteger();
    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(delegate) {
          @Override
          public boolean compareAndSetBatch(List<CasOp> ops) {
            batchCalls.incrementAndGet();
            return super.compareAndSetBatch(ops);
          }

          @Override
          public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> get(String key) {
            getCalls.incrementAndGet();
            return super.get(key);
          }
        };
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 715L;
    String generationId = "full-rescan-parent";
    String workerPrefix = "/worker-output/index-artifacts/";
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> references = new ArrayList<>();
    for (int index = 1; index <= 205; index++) {
      String targetStorageId = "file:s3://bucket/file-" + index + ".parquet";
      byte[] payload =
          indexRecord(snapshotId, "s3://bucket/file-" + index + ".parquet").toByteArray();
      byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(payload));
      String blobUri =
          workerPrefix
              + Hashing.sha256Hex(targetStorageId)
              + "/"
              + HexFormat.of().formatHex(digest)
              + ".pb";
      blobs.put(blobUri, payload, "application/x-protobuf");
      references.add(
          new IndexArtifactRepository.PrewrittenIndexArtifactReference(
              targetStorageId, blobUri, index, digest));
    }

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID, snapshotId, generationId, workerPrefix, references);

    assertThat(batchCalls).hasValue(3);
    assertThat(getCalls).hasValue(0);
    assertThat(
            delegate.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, generationId)))
        .isEqualTo(205);
  }

  @Test
  void generationActivationPreservesExternallyOverriddenArtifactUri() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = spy(new InMemoryBlobStore());
    IndexArtifactRepository repository =
        createRepository(
            pointers, blobs, new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)));
    long snapshotId = 716L;
    String generationId = "full-rescan-parent";
    String filePath = "s3://source/data.parquet";
    String targetStorageId = "file:" + filePath;
    IndexTarget target =
        IndexTarget.newBuilder()
            .setFile(IndexFileTarget.newBuilder().setFilePath(filePath))
            .build();
    IndexArtifactRecord record =
        IndexArtifactRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(snapshotId)
            .setTarget(target)
            .setArtifactUri("https://external.example/custom/index.parquet")
            .setArtifactFormat("parquet")
            .setArtifactFormatVersion(1)
            .setState(IndexArtifactState.IAS_READY)
            .putProperties("indexed_columns", "customer_id")
            .build();
    byte[] wrapper = record.toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(wrapper));
    String workerPrefix = "/worker-output/index-artifacts/";
    String wrapperUri =
        workerPrefix
            + Hashing.sha256Hex(targetStorageId)
            + "/"
            + HexFormat.of().formatHex(digest)
            + ".pb";
    blobs.put(wrapperUri, wrapper, "application/x-protobuf");

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        generationId,
        workerPrefix,
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                targetStorageId, wrapperUri, wrapper.length, digest)));

    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target)).isEmpty();

    SnapshotCaptureManifest manifest = captureManifest(snapshotId, 1, 1, "customer_id");
    byte[] manifestBytes = manifest.toByteArray();
    String stableManifestUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            Hashing.sha256Hex(manifestBytes));
    activateGeneration(repository, blobs, TABLE_ID, snapshotId, generationId, manifestBytes);

    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target))
        .get()
        .extracting(IndexArtifactRecord::getArtifactUri)
        .isEqualTo("https://external.example/custom/index.parquet");
    assertThat(
            pointers
                .get(
                    Keys.snapshotIndexArtifactCaptureManifestPointer(
                        TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId))
                .orElseThrow()
                .getBlobUri())
        .isEqualTo(stableManifestUri)
        .startsWith(
            Keys.snapshotIndexArtifactCaptureManifestBlobPrefix(
                TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId));
    assertThat(blobs.head(stableManifestUri)).isPresent();
    String reconcileJobPrefix =
        Keys.reconcileJobBlobPrefix(TABLE_ID.getAccountId(), "finalizer-job");
    blobs.put(
        reconcileJobPrefix + "result-payloads/capture-manifest.pb",
        manifestBytes,
        "application/x-protobuf");
    blobs.deletePrefix(reconcileJobPrefix);
    assertThat(repository.indexCaptureComplete(TABLE_ID, snapshotId, Set.of("customer_id")))
        .isTrue();
    assertThat(repository.indexCaptureComplete(TABLE_ID, snapshotId, Set.of("missing"))).isFalse();

    IndexArtifactRecord replacement =
        record.toBuilder()
            .setArtifactUri("https://external.example/replacement/index.parquet")
            .clearProperties()
            .build();
    assertThatThrownBy(() -> repository.putIndexArtifact(replacement))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot mutate finalized generation");
    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target)).contains(record);
    assertThat(repository.indexCaptureComplete(TABLE_ID, snapshotId, Set.of("customer_id")))
        .isTrue();
    verify(blobs, times(1)).get(stableManifestUri);
  }

  @Test
  void directWritesUseSnapshotGenerationBlobStorage() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 719L;
    String filePath = "s3://source/data.parquet";
    String targetStorageId = "file:" + filePath;
    IndexTarget target =
        IndexTarget.newBuilder()
            .setFile(IndexFileTarget.newBuilder().setFilePath(filePath))
            .build();
    IndexArtifactRecord record =
        IndexArtifactRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(snapshotId)
            .setTarget(target)
            .setArtifactUri("s3://indexes/data.parquet")
            .setArtifactFormat("parquet")
            .setArtifactFormatVersion(1)
            .setState(IndexArtifactState.IAS_READY)
            .build();
    String expectedBlobUri =
        Keys.snapshotIndexArtifactGenerationBlobUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            snapshotId,
            "direct",
            targetStorageId,
            Hashing.sha256Hex(record.toByteArray()));

    repository.putIndexArtifact(record);

    assertThat(blobs.head(expectedBlobUri)).isPresent();
    assertThat(
            pointers
                .get(
                    Keys.snapshotIndexArtifactGenerationPointer(
                        TABLE_ID.getAccountId(),
                        TABLE_ID.getId(),
                        snapshotId,
                        "direct",
                        targetStorageId))
                .orElseThrow()
                .getBlobUri())
        .isEqualTo(expectedBlobUri);
    assertThat(blobs.list("/accounts/a/tables/t/index-artifacts/", 10, "").keys()).isEmpty();
    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target)).contains(record);

    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        "full-rescan-parent",
        captureManifest(snapshotId, 0, 0, "").toByteArray());

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "direct")))
        .isZero();
  }

  @Test
  void directWriteRejectsAnOwnedSidecarDeletedBeforePublication() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    byte[] content = new byte[] {1, 2, 3};
    String sidecar =
        Keys.snapshotDirectIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            720L,
            "file:s3://bucket/file.parquet",
            Hashing.sha256Hex(content));
    IndexArtifactRecord record =
        indexRecord(720L, "s3://bucket/file.parquet").toBuilder().setArtifactUri(sidecar).build();

    assertThatThrownBy(() -> repository.putIndexArtifact(record))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("shared index sidecar is missing");

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), 720L, "direct")))
        .isZero();
  }

  @Test
  void directWriteChecksOwnedSidecarWithoutRewritingIt() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    byte[] content = new byte[] {4, 5, 6};
    String sidecar =
        Keys.snapshotDirectIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            721L,
            "file:s3://bucket/file.parquet",
            Hashing.sha256Hex(content));
    blobs.put(sidecar, content, "application/octet-stream");
    String before = blobs.head(sidecar).orElseThrow().getVersionId();
    IndexArtifactRecord record =
        indexRecord(721L, "s3://bucket/file.parquet").toBuilder().setArtifactUri(sidecar).build();

    repository.putIndexArtifact(record);

    assertThat(blobs.head(sidecar).orElseThrow().getVersionId()).isEqualTo(before);
    assertThat(repository.getIndexArtifact(TABLE_ID, 721L, record.getTarget())).contains(record);
  }

  @Test
  void directWriteRejectsASidecarOwnedByAnotherTable() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    byte[] content = new byte[] {7, 8, 9};
    String sidecar =
        Keys.snapshotDirectIndexSidecarObjectUri(
            TABLE_ID.getAccountId(),
            "other-table",
            722L,
            "file:s3://bucket/file.parquet",
            Hashing.sha256Hex(content));
    blobs.put(sidecar, content, "application/octet-stream");
    IndexArtifactRecord record =
        indexRecord(722L, "s3://bucket/file.parquet").toBuilder().setArtifactUri(sidecar).build();

    assertThatThrownBy(() -> repository.putIndexArtifact(record))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("sidecar is outside its carrier or inherited generation");

    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), 722L, "direct")))
        .isZero();
  }

  @Test
  void completenessRequiresFinalizedManifest() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);

    assertThat(repository.indexCaptureComplete(TABLE_ID, 717L, Set.of())).isFalse();

    assertThat(repository.indexCaptureComplete(TABLE_ID, 717L, Set.of())).isFalse();
  }

  @Test
  void finalizedEmptySnapshotIsCompleteWithoutArtifactReads() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore delegate = new InMemoryBlobStore();
    BlobStore blobs = spy(delegate);
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    SnapshotCaptureManifest empty = captureManifest(718L, 0, 0, "");
    byte[] manifestBytes = empty.toByteArray();
    String stableManifestUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), 718L, Hashing.sha256Hex(manifestBytes));
    activateGeneration(repository, blobs, TABLE_ID, 718L, "full-rescan-parent", manifestBytes);

    assertThat(repository.indexCaptureComplete(TABLE_ID, 718L, Set.of())).isTrue();
    verify(blobs, times(1)).get(stableManifestUri);
  }

  @Test
  void allColumnCaptureSatisfiesAnyRequestedSelector() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    SnapshotCaptureManifest manifest =
        captureManifest(720L, 1, 1, "").toBuilder()
            .setCapturePolicy(
                CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX)
                    .setDefaultColumnScope(DefaultColumnScope.DCS_ALL))
            .build();

    activateGeneration(
        repository, blobs, TABLE_ID, 720L, "full-rescan-parent", manifest.toByteArray());

    assertThat(repository.indexCaptureComplete(TABLE_ID, 720L, Set.of("#123"))).isTrue();
  }

  @Test
  void additiveActivationRejectsServiceSideCoverageMerging() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 721L;
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        "full-rescan-parent",
        captureManifest(snapshotId, 1, 1, "#1").toByteArray());
    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();
    SnapshotCaptureManifest incremental =
        captureManifest(snapshotId, 1, 1, "#2").toBuilder().setParentJobId("next").build();

    assertThatThrownBy(
            () ->
                activateGeneration(
                    repository,
                    blobs,
                    TABLE_ID,
                    snapshotId,
                    "full-rescan-next",
                    incremental.toByteArray(),
                    predecessor,
                    true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("materialized by the snapshot finalizer");
  }

  @Test
  void preparedActivationDoesNotExposeIndexPointersBeforeAtomicPublication() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 722L;
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        "generation-one",
        captureManifest(snapshotId, 1, 1, "#1").toByteArray());
    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();

    IndexArtifactRepository.PreparedActivation prepared =
        prepareGenerationActivation(
            repository,
            blobs,
            TABLE_ID,
            snapshotId,
            "generation-two",
            captureManifest(snapshotId, 1, 1, "#2").toByteArray(),
            predecessor,
            false);

    assertThat(
            repository
                .captureGenerationInput(TABLE_ID, snapshotId, List.of())
                .predecessor()
                .generationId())
        .isEqualTo("generation-one");
    assertThat(
            repository
                .captureGenerationInput(TABLE_ID, snapshotId, List.of())
                .predecessor()
                .captureManifestUri())
        .isEqualTo(predecessor.captureManifestUri());

    assertThat(
            pointers.compareAndSetBatch(
                prepared.publicationFence().pointerUpdates().stream()
                    .map(
                        update ->
                            (PointerStore.CasOp)
                                new PointerStore.CasUpsert(
                                    update.pointerKey(), update.expectedVersion(), update.next()))
                    .toList()))
        .isTrue();

    assertThat(
            repository
                .captureGenerationInput(TABLE_ID, snapshotId, List.of())
                .predecessor()
                .generationId())
        .isEqualTo("generation-two");
    assertThat(
            repository
                .captureGenerationInput(TABLE_ID, snapshotId, List.of())
                .predecessor()
                .captureManifestUri())
        .isNotEqualTo(predecessor.captureManifestUri());
  }

  @Test
  void idempotentActivationRetryStillDeletesTheDirectPredecessor() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 723L;
    repository.putIndexArtifact(indexRecord(snapshotId, "s3://source/direct.parquet"));
    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();
    byte[] manifest = captureManifest(snapshotId, 1, 1, "#1").toByteArray();
    IndexArtifactRepository.PreparedActivation prepared =
        prepareGenerationActivation(
            repository,
            blobs,
            TABLE_ID,
            snapshotId,
            "generation-one",
            manifest,
            predecessor,
            false);
    assertThat(
            pointers.compareAndSetBatch(
                prepared.publicationFence().pointerUpdates().stream()
                    .map(
                        update ->
                            (PointerStore.CasOp)
                                new PointerStore.CasUpsert(
                                    update.pointerKey(), update.expectedVersion(), update.next()))
                    .toList()))
        .isTrue();

    IndexArtifactRepository.PreparedActivation retry =
        prepareGenerationActivation(
            repository,
            blobs,
            TABLE_ID,
            snapshotId,
            "generation-one",
            manifest,
            predecessor,
            false);
    repository.completePreparedGenerationActivation(TABLE_ID, snapshotId, retry);

    assertThat(retry.deleteDirectPredecessor()).isTrue();
    assertThat(
            pointers.countByPrefix(
                Keys.snapshotIndexArtifactGenerationPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "direct")))
        .isZero();
  }

  @Test
  void pagedIndexListFailsRetryablyWhenTheActiveGenerationChanges() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = createRepository(pointers, blobs);
    long snapshotId = 724L;
    registerArtifact(repository, blobs, snapshotId, "generation-one", "s3://source/a.parquet");
    registerArtifact(repository, blobs, snapshotId, "generation-one", "s3://source/b.parquet");
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        "generation-one",
        captureManifest(snapshotId, 2, 2, "#1").toByteArray());
    StringBuilder next = new StringBuilder();

    assertThat(repository.listIndexArtifacts(TABLE_ID, snapshotId, 1, "", next)).hasSize(1);
    assertThat(next).isNotEmpty();

    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();
    registerArtifact(repository, blobs, snapshotId, "generation-two", "s3://source/c.parquet");
    activateGeneration(
        repository,
        blobs,
        TABLE_ID,
        snapshotId,
        "generation-two",
        captureManifest(snapshotId, 1, 1, "#2").toBuilder()
            .setParentJobId("next")
            .build()
            .toByteArray(),
        predecessor,
        false);

    assertThatThrownBy(
            () ->
                repository.listIndexArtifacts(
                    TABLE_ID, snapshotId, 1, next.toString(), new StringBuilder()))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class)
        .hasMessageContaining("generation changed");
  }

  private static void registerArtifact(
      IndexArtifactRepository repository,
      InMemoryBlobStore blobs,
      long snapshotId,
      String generationId,
      String filePath) {
    IndexArtifactRecord record = indexRecord(snapshotId, filePath);
    byte[] wrapper = record.toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(wrapper));
    String targetStorageId = "file:" + filePath;
    String prefix = "/worker-output/" + generationId + "/index-artifacts/";
    String wrapperUri =
        prefix
            + Hashing.sha256Hex(targetStorageId)
            + "/"
            + HexFormat.of().formatHex(digest)
            + ".pb";
    blobs.put(wrapperUri, wrapper, "application/x-protobuf");
    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        generationId,
        prefix,
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                targetStorageId, wrapperUri, wrapper.length, digest)));
  }

  private static IndexArtifactRecord indexRecord(long snapshotId, String filePath) {
    return IndexArtifactRecord.newBuilder()
        .setTableId(TABLE_ID)
        .setSnapshotId(snapshotId)
        .setTarget(
            IndexTarget.newBuilder().setFile(IndexFileTarget.newBuilder().setFilePath(filePath)))
        .setArtifactUri("s3://indexes/" + Hashing.sha256Hex(filePath) + ".parquet")
        .setArtifactFormat("parquet")
        .setArtifactFormatVersion(1)
        .setState(IndexArtifactState.IAS_READY)
        .build();
  }

  private static String workerStatsPrefix(long snapshotId) {
    return Keys.reconcileFileGroupStatsObjectPrefix(
        TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, "bundled", "group-job", "lease-1");
  }

  private static String managedSidecar(long snapshotId, String targetStorageId, byte[] content) {
    return Keys.reconcileFileGroupIndexSidecarObjectUri(
        TABLE_ID.getAccountId(),
        TABLE_ID.getId(),
        snapshotId,
        "bundled",
        "group-job",
        "lease-1",
        targetStorageId,
        Hashing.sha256Hex(content));
  }

  private static SnapshotCaptureManifest captureManifest(
      long snapshotId, int sourceFileCount, int indexArtifactCount, String selector) {
    CapturePolicy.Builder policy =
        CapturePolicy.newBuilder().addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX);
    if (selector != null && !selector.isBlank()) {
      policy.addColumns(
          CaptureColumnPolicy.newBuilder().setSelector(selector).setCaptureIndex(true));
    }
    return SnapshotCaptureManifest.newBuilder()
        .setFormatVersion(1)
        .setAccountId(TABLE_ID.getAccountId())
        .setParentJobId("parent")
        .setTableId(TABLE_ID.getId())
        .setSnapshotId(snapshotId)
        .setCapturePolicy(policy)
        .setSourceFileCount(sourceFileCount)
        .setIndexArtifactCount(indexArtifactCount)
        .build();
  }

  private static IndexArtifactRepository.ActivationFence activateGeneration(
      IndexArtifactRepository repository,
      BlobStore blobs,
      ResourceId tableId,
      long snapshotId,
      String generationId,
      byte[] captureManifestBytes) {
    prewriteCaptureManifest(blobs, tableId, snapshotId, captureManifestBytes);
    return repository.activateGeneration(tableId, snapshotId, generationId, captureManifestBytes);
  }

  private static IndexArtifactRepository.ActivationFence activateGeneration(
      IndexArtifactRepository repository,
      BlobStore blobs,
      ResourceId tableId,
      long snapshotId,
      String generationId,
      byte[] captureManifestBytes,
      IndexArtifactRepository.GenerationPredecessor predecessor,
      boolean inheritCoverage) {
    prewriteCaptureManifest(blobs, tableId, snapshotId, captureManifestBytes);
    return repository.activateGeneration(
        tableId, snapshotId, generationId, captureManifestBytes, predecessor, inheritCoverage);
  }

  private static IndexArtifactRepository.PreparedActivation prepareGenerationActivation(
      IndexArtifactRepository repository,
      BlobStore blobs,
      ResourceId tableId,
      long snapshotId,
      String generationId,
      byte[] captureManifestBytes,
      IndexArtifactRepository.GenerationPredecessor predecessor,
      boolean inheritCoverage) {
    prewriteCaptureManifest(blobs, tableId, snapshotId, captureManifestBytes);
    return repository.prepareGenerationActivation(
        tableId, snapshotId, generationId, captureManifestBytes, predecessor, inheritCoverage);
  }

  private static void prewriteCaptureManifest(
      BlobStore blobs, ResourceId tableId, long snapshotId, byte[] captureManifestBytes) {
    blobs.put(
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            Hashing.sha256Hex(captureManifestBytes)),
        captureManifestBytes,
        "application/x-protobuf");
  }
}
