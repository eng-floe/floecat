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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TableKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TableRepository {

  private final GenericResourceRepository<Table, TableKey> repo;

  @Inject
  public TableRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE,
            Table::parseFrom,
            Table::toByteArray,
            "application/x-protobuf");
  }

  public void create(Table table) {
    repo.create(table);
  }

  public boolean update(Table table, long expectedPointerVersion) {
    return repo.update(table, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableResourceId) {
    return repo.delete(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId tableResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Table> getById(ResourceId tableResourceId) {
    return repo.getByKey(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public Optional<Table> getByName(
      String accountId, String catalogId, String namespaceId, String tableName) {
    return repo.get(Keys.tablePointerByName(accountId, catalogId, namespaceId, tableName));
  }

  public List<Table> list(
      String accountId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.tablePointerByNamePrefix(accountId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.tablePointerByNamePrefix(accountId, catalogId, namespaceId));
  }

  public MutationMeta metaFor(ResourceId tableResourceId) {
    return repo.metaFor(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId tableResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableResourceId) {
    return repo.metaForSafe(new TableKey(tableResourceId.getAccountId(), tableResourceId.getId()));
  }
}
