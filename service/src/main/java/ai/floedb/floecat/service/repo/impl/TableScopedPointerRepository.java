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

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.ResourceSchema;
import ai.floedb.floecat.service.repo.model.TableScopedPointerKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.ProtoParser;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.Optional;
import java.util.function.Function;

/**
 * Base for pointer read models that store exactly one record per table, keyed by {@link
 * TableScopedPointerKey}. It adapts {@link GenericResourceRepository} to a {@link ResourceId}-keyed
 * surface so concrete pointers (current-snapshot, current-table-state) share one CAS-backed
 * implementation instead of duplicating the wrapper. Subclasses only supply the schema and the
 * proto codec.
 */
public abstract class TableScopedPointerRepository<T> {
  private static final String CONTENT_TYPE = "application/x-protobuf";

  protected final GenericResourceRepository<T, TableScopedPointerKey> repo;

  /** For the CDI client proxy of {@code @ApplicationScoped} subclasses only; never used. */
  protected TableScopedPointerRepository() {
    this.repo = null;
  }

  protected TableScopedPointerRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      ResourceSchema<T, TableScopedPointerKey> schema,
      ProtoParser<T> parser,
      Function<T, byte[]> toBytes) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore, blobStore, schema, parser, toBytes, CONTENT_TYPE);
  }

  public Optional<T> get(ResourceId tableId) {
    return repo.getByKey(key(tableId));
  }

  public boolean createIfAbsent(T value) {
    return repo.createIfAbsent(value);
  }

  public boolean update(T value, long expectedPointerVersion) {
    return repo.update(value, expectedPointerVersion);
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(key(tableId), expectedPointerVersion);
  }

  public MutationMeta metaFor(ResourceId tableId) {
    return repo.metaFor(key(tableId));
  }

  /**
   * Pointer meta without throwing when absent: version 0 and empty blob uri for a missing record.
   */
  public MutationMeta metaForSafe(ResourceId tableId) {
    return repo.metaForSafe(key(tableId));
  }

  protected static TableScopedPointerKey key(ResourceId tableId) {
    return new TableScopedPointerKey(tableId.getAccountId(), tableId.getId());
  }
}
