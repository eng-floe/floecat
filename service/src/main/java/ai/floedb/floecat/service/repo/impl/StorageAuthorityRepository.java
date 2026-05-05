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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.StorageAuthorityKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class StorageAuthorityRepository {

  private final GenericResourceRepository<StorageAuthority, StorageAuthorityKey> repo;

  @Inject
  public StorageAuthorityRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.STORAGE_AUTHORITY,
            StorageAuthority::parseFrom,
            StorageAuthority::toByteArray,
            "application/x-protobuf");
  }

  public void create(StorageAuthority authority) {
    repo.create(authority);
  }

  public boolean update(StorageAuthority authority, long expectedPointerVersion) {
    return repo.update(authority, expectedPointerVersion);
  }

  public boolean delete(ResourceId authorityId) {
    return repo.delete(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId authorityId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()),
        expectedPointerVersion);
  }

  public Optional<StorageAuthority> getById(ResourceId authorityId) {
    return repo.getByKey(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public Optional<StorageAuthority> getByName(String accountId, String displayName) {
    return repo.get(Keys.storageAuthorityPointerByName(accountId, displayName));
  }

  public List<StorageAuthority> list(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.storageAuthorityPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.storageAuthorityPointerByNamePrefix(accountId));
  }

  public MutationMeta metaFor(ResourceId authorityId) {
    return repo.metaFor(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public MutationMeta metaFor(ResourceId authorityId, Timestamp nowTs) {
    return repo.metaFor(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId authorityId) {
    return repo.metaForSafe(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }
}
