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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
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
    IndexArtifactRepository repository =
        new IndexArtifactRepository(pointers, new InMemoryBlobStore());
    long snapshotId = 715L;
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> references = new ArrayList<>();
    for (int index = 1; index <= 205; index++) {
      String targetStorageId = "file:s3://bucket/file-" + index + ".parquet";
      byte[] digest = HexFormat.of().parseHex(Hashing.sha256Hex("payload-" + index));
      String blobUri =
          "/worker-uploads/"
              + Hashing.sha256Hex(targetStorageId)
              + "/"
              + HexFormat.of().formatHex(digest)
              + ".pb";
      references.add(
          new IndexArtifactRepository.PrewrittenIndexArtifactReference(
              targetStorageId, blobUri, index, digest));
    }

    repository.registerPrewrittenIndexArtifactReferences(TABLE_ID, snapshotId, references);

    assertThat(batchCalls).hasValue(3);
    assertThat(getCalls).hasValue(0);
    assertThat(
            delegate.countByPrefix(
                Keys.snapshotIndexArtifactsPrefix(
                    TABLE_ID.getAccountId(), TABLE_ID.getId(), snapshotId)))
        .isEqualTo(205);
  }
}
