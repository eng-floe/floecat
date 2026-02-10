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
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TransactionKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.PointerOverlay;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class TransactionRepository {

  private final GenericResourceRepository<Transaction, TransactionKey> repo;

  @Inject
  public TransactionRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            PointerOverlay.NOOP,
            Schemas.TRANSACTION,
            Transaction::parseFrom,
            Transaction::toByteArray,
            "application/x-protobuf");
  }

  public void create(Transaction txn) {
    repo.create(txn);
  }

  public boolean update(Transaction txn, long expectedPointerVersion) {
    return repo.update(txn, expectedPointerVersion);
  }

  public Optional<Transaction> getById(String accountId, String txId) {
    return repo.getByKey(new TransactionKey(accountId, txId, ""));
  }

  public MutationMeta metaFor(String accountId, String txId) {
    return repo.metaFor(new TransactionKey(accountId, txId, ""));
  }

  public MutationMeta metaFor(String accountId, String txId, Timestamp nowTs) {
    return repo.metaFor(new TransactionKey(accountId, txId, ""), nowTs);
  }
}
