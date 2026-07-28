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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IndexArtifactRepositoryTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder().setAccountId("a").setId("t").setKind(ResourceKind.RK_TABLE).build();

  @Test
  void prewrittenReferencesUseOptimisticPointerBatchesWithoutReads() {
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
    BlobStore blobs = mock(BlobStore.class);
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
    long snapshotId = 715L;
    String generationId = "full-rescan-parent";
    String workerPrefix = "/worker-output/index-artifacts/";
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> references = new ArrayList<>();
    for (int index = 1; index <= 205; index++) {
      String targetStorageId = "file:s3://bucket/file-" + index + ".parquet";
      byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex("payload-" + index));
      String blobUri =
          workerPrefix
              + Hashing.sha256Hex(targetStorageId)
              + "/"
              + HexFormat.of().formatHex(digest)
              + ".pb";
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
    verifyNoInteractions(blobs);
  }

  @Test
  void generationActivationPreservesExternallyOverriddenArtifactUri() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = spy(new InMemoryBlobStore());
    IndexArtifactRepository repository =
        new IndexArtifactRepository(
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
    repository.activateGeneration(TABLE_ID, snapshotId, generationId, manifestBytes);

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
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
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

    repository.activateGeneration(
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
  void completenessRequiresFinalizedManifest() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);

    assertThat(repository.indexCaptureComplete(TABLE_ID, 717L, Set.of())).isFalse();

    assertThat(repository.indexCaptureComplete(TABLE_ID, 717L, Set.of())).isFalse();
  }

  @Test
  void finalizedEmptySnapshotIsCompleteWithoutArtifactReads() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore delegate = new InMemoryBlobStore();
    BlobStore blobs = spy(delegate);
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
    SnapshotCaptureManifest empty = captureManifest(718L, 0, 0, "");
    byte[] manifestBytes = empty.toByteArray();
    String stableManifestUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), 718L, Hashing.sha256Hex(manifestBytes));
    repository.activateGeneration(TABLE_ID, 718L, "full-rescan-parent", manifestBytes);

    assertThat(repository.indexCaptureComplete(TABLE_ID, 718L, Set.of())).isTrue();
    verify(blobs, times(1)).get(stableManifestUri);
  }

  @Test
  void allColumnCaptureSatisfiesAnyRequestedSelector() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
    SnapshotCaptureManifest manifest =
        captureManifest(720L, 1, 1, "").toBuilder()
            .setCapturePolicy(
                CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX)
                    .setDefaultColumnScope(DefaultColumnScope.DCS_ALL))
            .build();

    repository.activateGeneration(TABLE_ID, 720L, "full-rescan-parent", manifest.toByteArray());

    assertThat(repository.indexCaptureComplete(TABLE_ID, 720L, Set.of("#123"))).isTrue();
  }

  @Test
  void additiveActivationRetainsPriorSelectorCoverageAndFencesItsPredecessor() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
    long snapshotId = 721L;
    repository.activateGeneration(
        TABLE_ID,
        snapshotId,
        "full-rescan-parent",
        captureManifest(snapshotId, 1, 1, "#1").toByteArray());
    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();
    SnapshotCaptureManifest incremental =
        captureManifest(snapshotId, 1, 1, "#2").toBuilder().setParentJobId("next").build();

    repository.activateGeneration(
        TABLE_ID, snapshotId, "full-rescan-next", incremental.toByteArray(), predecessor, true);

    assertThat(repository.indexCaptureComplete(TABLE_ID, snapshotId, Set.of("#1", "#2"))).isTrue();
    assertThatThrownBy(
            () ->
                repository.activateGeneration(
                    TABLE_ID,
                    snapshotId,
                    "full-rescan-stale",
                    incremental.toBuilder().setParentJobId("stale").build().toByteArray(),
                    predecessor,
                    true))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("predecessor changed");
  }

  @Test
  void preparedActivationDoesNotExposeIndexPointersBeforeAtomicPublication() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
    long snapshotId = 722L;
    repository.activateGeneration(
        TABLE_ID,
        snapshotId,
        "generation-one",
        captureManifest(snapshotId, 1, 1, "#1").toByteArray());
    IndexArtifactRepository.GenerationPredecessor predecessor =
        repository.captureGenerationInput(TABLE_ID, snapshotId, List.of()).predecessor();

    IndexArtifactRepository.PreparedActivation prepared =
        repository.prepareGenerationActivation(
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
}
