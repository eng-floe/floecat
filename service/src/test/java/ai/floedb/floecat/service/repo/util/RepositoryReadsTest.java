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

  private static final CatalogKey KEY = new CatalogKey("account", "catalog");

  @Test
  void immutableBlobCacheHitsStayOutsideTheBackendRead() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    GenericResourceRepository<Catalog, CatalogKey> writer =
        repository(pointers, blobs, null, RepositoryReads.direct(pointers, blobs));
    Catalog catalog = catalog("sales");
    writer.create(catalog);
    String blobUri = writer.metaFor(KEY).getBlobUri();

    AtomicInteger backendGets = new AtomicInteger();
    GenericResourceRepository<Catalog, CatalogKey> reader =
        repository(
            pointers,
            blobs,
            new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5)),
            countedReads(pointers, blobs, backendGets));

    assertThat(reader.getByBlobUri(blobUri)).contains(catalog);
    assertThat(reader.getByBlobUri(blobUri)).contains(catalog);
    assertThat(backendGets).hasValue(1);
  }

  @Test
  void mutationProtocolsKeepPrerequisiteReadsOnRawStores() {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    AtomicInteger admittedReads = new AtomicInteger();
    GenericResourceRepository<Catalog, CatalogKey> repository =
        repository(pointers, blobs, null, countedReads(pointers, blobs, admittedReads));
    repository.create(catalog("sales"));
    long version = repository.metaFor(KEY).getPointerVersion();
    admittedReads.set(0);

    assertThat(repository.update(catalog("marketing"), version)).isTrue();
    assertThat(admittedReads).hasValue(0);
    assertThat(repository.delete(KEY)).isTrue();
    assertThat(admittedReads).hasValue(0);

    repository.create(catalog("sales"));
    version = repository.metaFor(KEY).getPointerVersion();
    admittedReads.set(0);
    assertThat(repository.deleteWithPrecondition(KEY, version)).isTrue();
    assertThat(admittedReads).hasValue(0);
  }

  /** Build a catalog repository with explicit read policy and optional immutable cache. */
  private static GenericResourceRepository<Catalog, CatalogKey> repository(
      InMemoryPointerStore pointers,
      InMemoryBlobStore blobs,
      ImmutableBlobCache cache,
      RepositoryReads reads) {
    return new GenericResourceRepository<>(
        pointers,
        blobs,
        Schemas.CATALOG,
        Catalog::parseFrom,
        Catalog::toByteArray,
        "application/x-protobuf",
        cache,
        reads);
  }

  /** Build one catalog value with the fixture's stable resource identity. */
  private static Catalog catalog(String displayName) {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    return Catalog.newBuilder().setResourceId(id).setDisplayName(displayName).build();
  }

  /** Count every read-policy invocation while delegating to the in-memory stores. */
  private static RepositoryReads countedReads(
      InMemoryPointerStore pointers, InMemoryBlobStore blobs, AtomicInteger readCount) {
    return RepositoryReads.bind(
        pointers,
        blobs,
        new RepositoryReads.ReadPolicy() {
          @Override
          public <T> T read(Supplier<T> operation) {
            readCount.incrementAndGet();
            return operation.get();
          }
        });
  }
}
