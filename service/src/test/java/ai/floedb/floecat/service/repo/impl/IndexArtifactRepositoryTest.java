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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.types.Hashing;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
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
    String workerPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, generationId);
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
        TABLE_ID, snapshotId, generationId, references);

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
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    IndexArtifactRepository repository = new IndexArtifactRepository(pointers, blobs);
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
            .build();
    byte[] wrapper = record.toByteArray();
    byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex(wrapper));
    String wrapperUri =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
                TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId, generationId)
            + Hashing.sha256Hex(targetStorageId)
            + "/"
            + HexFormat.of().formatHex(digest)
            + ".pb";
    blobs.put(wrapperUri, wrapper, "application/x-protobuf");

    repository.registerPrewrittenIndexArtifactReferencesInGeneration(
        TABLE_ID,
        snapshotId,
        generationId,
        List.of(
            new IndexArtifactRepository.PrewrittenIndexArtifactReference(
                targetStorageId, wrapperUri, wrapper.length, digest)));

    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target)).isEmpty();

    repository.activateGeneration(TABLE_ID, snapshotId, generationId);

    assertThat(repository.getIndexArtifact(TABLE_ID, snapshotId, target))
        .get()
        .extracting(IndexArtifactRecord::getArtifactUri)
        .isEqualTo("https://external.example/custom/index.parquet");
  }
}
