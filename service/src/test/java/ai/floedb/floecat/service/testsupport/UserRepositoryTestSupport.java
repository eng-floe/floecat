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
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared in-memory repository fakes to keep resolver tests lean. */
public final class UserRepositoryTestSupport {

  private UserRepositoryTestSupport() {}

  public static final class FakeCatalogRepository extends CatalogRepository {
    private final Map<ResourceId, Catalog> entries = new HashMap<>();

    public FakeCatalogRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    public void put(Catalog catalog) {
      entries.put(catalog.getResourceId(), catalog);
    }

    @Override
    public Optional<Catalog> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Catalog> getByName(String accountId, String displayName) {
      return entries.values().stream()
          .filter(
              cat ->
                  accountId.equals(cat.getResourceId().getAccountId())
                      && displayName.equals(cat.getDisplayName()))
          .findFirst();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }

  public static final class FakeNamespaceRepository extends NamespaceRepository {
    private final Map<ResourceId, Namespace> entries = new HashMap<>();

    public FakeNamespaceRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    public void put(Namespace namespace) {
      entries.put(namespace.getResourceId(), namespace);
    }

    @Override
    public Optional<Namespace> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Namespace> getByPath(String accountId, String catalogId, List<String> path) {
      return entries.values().stream()
          .filter(
              ns ->
                  accountId.equals(ns.getResourceId().getAccountId())
                      && catalogId.equals(ns.getCatalogId().getId())
                      && matches(ns, path))
          .findFirst();
    }

    private boolean matches(Namespace namespace, List<String> path) {
      if (path.isEmpty()) {
        return namespace.getDisplayName().isBlank() && namespace.getParentsList().isEmpty();
      }
      List<String> parents = path.subList(0, path.size() - 1);
      String name = path.get(path.size() - 1);
      return parents.equals(namespace.getParentsList()) && name.equals(namespace.getDisplayName());
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }
  }

  public static final class FakeTableRepository extends TableRepository {
    private final Map<ResourceId, Table> entries = new HashMap<>();

    public FakeTableRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    public void put(Table table) {
      entries.put(table.getResourceId(), table);
    }

    @Override
    public Optional<Table> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<Table> getByName(
        String accountId, String catalogId, String namespaceId, String displayName) {
      return entries.values().stream()
          .filter(
              tbl ->
                  accountId.equals(tbl.getResourceId().getAccountId())
                      && catalogId.equals(tbl.getCatalogId().getId())
                      && namespaceId.equals(tbl.getNamespaceId().getId())
                      && displayName.equals(tbl.getDisplayName()))
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
      List<Table> sorted =
          entries.values().stream()
              .filter(
                  tbl ->
                      accountId.equals(tbl.getResourceId().getAccountId())
                          && catalogId.equals(tbl.getCatalogId().getId())
                          && namespaceId.equals(tbl.getNamespaceId().getId()))
              .sorted(java.util.Comparator.comparing(Table::getDisplayName))
              .toList();
      int start = startIndexForToken(sorted, pageToken);
      int end = Math.min(sorted.size(), start + Math.max(1, limit));
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return sorted.subList(start, end);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return (int)
          entries.values().stream()
              .filter(
                  tbl ->
                      accountId.equals(tbl.getResourceId().getAccountId())
                          && catalogId.equals(tbl.getCatalogId().getId())
                          && namespaceId.equals(tbl.getNamespaceId().getId()))
              .count();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }

    private int startIndexForToken(List<Table> sorted, String pageToken) {
      if (pageToken == null || pageToken.isBlank()) {
        return 0;
      }
      for (int i = 0; i < sorted.size(); i++) {
        if (sorted.get(i).getDisplayName().equals(pageToken)) {
          return i + 1;
        }
      }
      throw new IllegalArgumentException("bad token");
    }
  }

  public static final class FakeViewRepository extends ViewRepository {
    private final Map<ResourceId, View> entries = new HashMap<>();

    public FakeViewRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    public void put(View view) {
      entries.put(view.getResourceId(), view);
    }

    @Override
    public Optional<View> getById(ResourceId id) {
      return Optional.ofNullable(entries.get(id));
    }

    @Override
    public Optional<View> getByName(
        String accountId, String catalogId, String namespaceId, String displayName) {
      return entries.values().stream()
          .filter(
              vw ->
                  accountId.equals(vw.getResourceId().getAccountId())
                      && catalogId.equals(vw.getCatalogId().getId())
                      && namespaceId.equals(vw.getNamespaceId().getId())
                      && displayName.equals(vw.getDisplayName()))
          .findFirst();
    }

    @Override
    public List<View> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String pageToken,
        StringBuilder nextOut) {
      List<View> sorted =
          entries.values().stream()
              .filter(
                  vw ->
                      accountId.equals(vw.getResourceId().getAccountId())
                          && catalogId.equals(vw.getCatalogId().getId())
                          && namespaceId.equals(vw.getNamespaceId().getId()))
              .sorted(java.util.Comparator.comparing(View::getDisplayName))
              .toList();
      int start = startIndexForToken(sorted, pageToken);
      int end = Math.min(sorted.size(), start + Math.max(1, limit));
      if (end < sorted.size()) {
        nextOut.append(sorted.get(end - 1).getDisplayName());
      }
      return sorted.subList(start, end);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return (int)
          entries.values().stream()
              .filter(
                  vw ->
                      accountId.equals(vw.getResourceId().getAccountId())
                          && catalogId.equals(vw.getCatalogId().getId())
                          && namespaceId.equals(vw.getNamespaceId().getId()))
              .count();
    }

    @Override
    public MutationMeta metaForSafe(ResourceId id) {
      throw new StorageNotFoundException("unused");
    }

    private int startIndexForToken(List<View> sorted, String pageToken) {
      if (pageToken == null || pageToken.isBlank()) {
        return 0;
      }
      for (int i = 0; i < sorted.size(); i++) {
        if (sorted.get(i).getDisplayName().equals(pageToken)) {
          return i + 1;
        }
      }
      throw new IllegalArgumentException("bad token");
    }
  }
}
