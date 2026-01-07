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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class FakeTableRepository extends TableRepository {
  private final Map<ResourceId, Table> entries = new HashMap<>();
  private final Map<ResourceId, MutationMeta> metas = new HashMap<>();
  private final Map<ResourceId, Integer> gets = new HashMap<>();

  public FakeTableRepository() {
    super(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  public void put(Table table, MutationMeta meta) {
    entries.put(table.getResourceId(), table);
    metas.put(table.getResourceId(), meta);
  }

  public void putMeta(ResourceId id, MutationMeta meta) {
    metas.put(id, meta);
  }

  @Override
  public Optional<Table> getById(ResourceId id) {
    gets.merge(id, 1, Integer::sum);
    return Optional.ofNullable(entries.get(id));
  }

  @Override
  public Optional<Table> getByName(
      String accountId, String catalogId, String namespaceId, String displayName) {
    return entries.values().stream()
        .filter(
            t ->
                accountId.equals(t.getResourceId().getAccountId())
                    && catalogId.equals(t.getCatalogId().getId())
                    && namespaceId.equals(t.getNamespaceId().getId())
                    && displayName.equals(t.getDisplayName()))
        .findFirst();
  }

  @Override
  public List<Table> list(
      String accountId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    List<Table> sorted = matchingTables(accountId, catalogId, namespaceId);
    int start = startIndexForToken(sorted, pageToken);
    int want = Math.max(1, limit);
    int end = Math.min(sorted.size(), start + want);
    List<Table> slice = new ArrayList<>(sorted.subList(start, end));
    nextOut.setLength(0);
    if (end < sorted.size()) {
      nextOut.append(sorted.get(end - 1).getDisplayName());
    }
    return slice;
  }

  @Override
  public int count(String accountId, String catalogId, String namespaceId) {
    return matchingTables(accountId, catalogId, namespaceId).size();
  }

  @Override
  public MutationMeta metaForSafe(ResourceId id) {
    MutationMeta meta = metas.get(id);
    if (meta == null) {
      throw new StorageNotFoundException("missing table meta");
    }
    return meta;
  }

  public int getByIdCount(ResourceId id) {
    return gets.getOrDefault(id, 0);
  }

  private List<Table> matchingTables(String accountId, String catalogId, String namespaceId) {
    return entries.values().stream()
        .filter(
            t ->
                accountId.equals(t.getResourceId().getAccountId())
                    && catalogId.equals(t.getCatalogId().getId())
                    && namespaceId.equals(t.getNamespaceId().getId()))
        .sorted(Comparator.comparing(Table::getDisplayName))
        .toList();
  }

  private int startIndexForToken(List<Table> sorted, String token) {
    if (token == null || token.isBlank()) {
      return 0;
    }
    for (int i = 0; i < sorted.size(); i++) {
      if (sorted.get(i).getDisplayName().equals(token)) {
        return i + 1;
      }
    }
    throw new IllegalArgumentException("bad token");
  }
}
