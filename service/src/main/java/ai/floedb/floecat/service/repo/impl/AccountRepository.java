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

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.AccountKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class AccountRepository {

  private final GenericResourceRepository<Account, AccountKey> repo;

  @Inject
  public AccountRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.ACCOUNT,
            Account::parseFrom,
            Account::toByteArray,
            "application/x-protobuf");
  }

  public void create(Account account) {
    repo.create(account);
  }

  public boolean update(Account account, long expectedPointerVersion) {
    return repo.update(account, expectedPointerVersion);
  }

  public boolean delete(ResourceId accountResourceId) {
    return repo.delete(new AccountKey(accountResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId accountResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new AccountKey(accountResourceId.getId()), expectedPointerVersion);
  }

  public Optional<Account> getById(ResourceId accountResourceId) {
    return repo.getByKey(new AccountKey(accountResourceId.getId()));
  }

  public Optional<Account> getByName(String displayName) {
    return repo.get(Keys.accountPointerByName(displayName));
  }

  public List<Account> list(int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.accountPointerByNamePrefix(), limit, pageToken, nextOut);
  }

  public int count() {
    return repo.countByPrefix(Keys.accountPointerByNamePrefix());
  }

  public MutationMeta metaFor(ResourceId accountResourceId) {
    return repo.metaFor(new AccountKey(accountResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId accountResourceId, Timestamp nowTs) {
    return repo.metaFor(new AccountKey(accountResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId accountResourceId) {
    return repo.metaForSafe(new AccountKey(accountResourceId.getId()));
  }
}
