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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Verifies repository reads invoke the backend seam only when storage is actually consulted. */
class RepositoryReadsTest {

  @Test
  void immutableBlobCacheHitsStayOutsideTheBackendRead() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    GenericResourceRepository<Catalog, CatalogKey> writer =
        new GenericResourceRepository<>(
            pointers,
            blobs,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf");
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    Catalog catalog = Catalog.newBuilder().setResourceId(id).setDisplayName("sales").build();
    writer.create(catalog);
    String blobUri = writer.metaFor(new CatalogKey("account", "catalog")).getBlobUri();

    AtomicInteger backendGets = new AtomicInteger();
    RepositoryReads counted =
        RepositoryReads.bind(
            pointers,
            blobs,
            new RepositoryReads.ReadPolicy() {
              @Override
              public <T> T read(Supplier<T> operation) {
                backendGets.incrementAndGet();
                return operation.get();
              }
            });
    GenericResourceRepository<Catalog, CatalogKey> reader =
        new GenericResourceRepository<>(
            pointers,
            blobs,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf",
            new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)),
            counted);

    assertThat(reader.getByBlobUri(blobUri)).contains(catalog);
    assertThat(reader.getByBlobUri(blobUri)).contains(catalog);
    assertThat(backendGets).hasValue(1);
  }
}
