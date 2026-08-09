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

package ai.floedb.floecat.service.repo.util;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

/** Verifies request cancellation remains a control-flow outcome across repository decoding. */
class BaseResourceRepositoryCancellationTest {

  @Test
  void blobCancellationPropagatesFromPointerAndDirectReads() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var writer = repository(pointers, blobs, RepositoryReads.direct(pointers, blobs));
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    CatalogKey key = new CatalogKey(catalogId.getAccountId(), catalogId.getId());
    writer.create(Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    String blobUri =
        pointers.get(Schemas.CATALOG.canonicalPointerForKey.apply(key)).orElseThrow().getBlobUri();

    CancellationException expected = new CancellationException("request cancelled");
    RepositoryReads direct = RepositoryReads.direct(pointers, blobs);
    RepositoryReads cancelledReads =
        new RepositoryReads(
            direct.pointers(),
            new RepositoryReads.Blobs() {
              @Override
              public byte[] get(String uri) {
                throw expected;
              }

              @Override
              public Map<String, byte[]> getBatch(List<String> uris) {
                return direct.blobs().getBatch(uris);
              }

              @Override
              public Optional<BlobHeader> head(String uri) {
                return direct.blobs().head(uri);
              }
            });
    var reader = repository(pointers, blobs, cancelledReads);

    assertThatThrownBy(() -> reader.getByKey(key)).isSameAs(expected);
    assertThatThrownBy(() -> reader.getByBlobUri(blobUri)).isSameAs(expected);
  }

  private static GenericResourceRepository<Catalog, CatalogKey> repository(
      InMemoryPointerStore pointers, InMemoryBlobStore blobs, RepositoryReads reads) {
    return new GenericResourceRepository<>(
        pointers,
        blobs,
        Schemas.CATALOG,
        Catalog::parseFrom,
        Catalog::toByteArray,
        "application/x-protobuf",
        null,
        reads);
  }
}
