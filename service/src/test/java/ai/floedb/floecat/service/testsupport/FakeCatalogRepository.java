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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class FakeCatalogRepository extends CatalogRepository {
  private final Map<ResourceId, Catalog> entries = new HashMap<>();
  private final Map<ResourceId, MutationMeta> metas = new HashMap<>();
  private final Map<ResourceId, Integer> gets = new HashMap<>();

  public FakeCatalogRepository() {
    super(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  public void put(Catalog catalog, MutationMeta meta) {
    entries.put(catalog.getResourceId(), catalog);
    metas.put(catalog.getResourceId(), meta);
  }

  public void putMeta(ResourceId id, MutationMeta meta) {
    metas.put(id, meta);
  }

  @Override
  public Optional<Catalog> getById(ResourceId id) {
    gets.merge(id, 1, Integer::sum);
    return Optional.ofNullable(entries.get(id));
  }

  @Override
  public Optional<Catalog> getByName(String accountId, String displayName) {
    return entries.values().stream()
        .filter(
            c ->
                accountId.equals(c.getResourceId().getAccountId())
                    && displayName.equals(c.getDisplayName()))
        .findFirst();
  }

  @Override
  public MutationMeta metaForSafe(ResourceId id) {
    MutationMeta meta = metas.get(id);
    if (meta == null) {
      throw new StorageNotFoundException("missing catalog meta");
    }
    return meta;
  }

  public int getByIdCount(ResourceId id) {
    return gets.getOrDefault(id, 0);
  }
}
