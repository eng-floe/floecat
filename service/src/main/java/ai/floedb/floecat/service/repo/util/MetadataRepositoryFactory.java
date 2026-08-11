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

import ai.floedb.floecat.service.concurrent.MetadataResourceReader;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.ResourceKey;
import ai.floedb.floecat.service.repo.model.ResourceSchema;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Function;

/** Composes metadata repositories with raw mutation stores and admitted read-only stores. */
@ApplicationScoped
public class MetadataRepositoryFactory {
  private final PointerStore pointers;
  private final BlobStore blobs;
  private final ImmutableBlobCache cache;
  private final RepositoryReads reads;

  /**
   * Compose the process stores once: mutation transactions retain the raw stores, while repository
   * query reads use adapters governed by {@code admittedReads}.
   */
  @Inject
  public MetadataRepositoryFactory(
      PointerStore pointers,
      BlobStore blobs,
      ImmutableBlobCache cache,
      MetadataResourceReader admittedReads) {
    this.pointers = pointers;
    this.blobs = blobs;
    this.cache = cache;
    this.reads = RepositoryReads.bind(pointers, blobs, admittedReads);
  }

  /** Build one admitted metadata repository while leaving its mutation path direct. */
  public <T, K extends ResourceKey> GenericResourceRepository<T, K> create(
      ResourceSchema<T, K> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes,
      String contentType) {
    return new GenericResourceRepository<>(
        pointers, blobs, schema, parser, toBytes, contentType, cache, reads);
  }
}
