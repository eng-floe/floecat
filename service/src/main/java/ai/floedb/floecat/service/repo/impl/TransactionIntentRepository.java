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

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TransactionIntentKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.PointerOverlay;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TransactionIntentRepository {

  private final GenericResourceRepository<TransactionIntent, TransactionIntentKey> repo;
  private final PointerStore pointerStore;

  @Inject
  public TransactionIntentRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            PointerOverlay.NOOP,
            Schemas.TRANSACTION_INTENT,
            TransactionIntent::parseFrom,
            TransactionIntent::toByteArray,
            "application/x-protobuf");
  }

  public void create(TransactionIntent intent) {
    repo.create(intent);
  }

  public Optional<TransactionIntent> getByTarget(String accountId, String targetPointerKey) {
    return repo.get(Keys.transactionIntentPointerByTarget(accountId, targetPointerKey));
  }

  public List<TransactionIntent> listByTx(String accountId, String txId) {
    String prefix = Keys.transactionIntentPointerByTxPrefix(accountId, txId);
    return repo.listByPrefix(prefix, Integer.MAX_VALUE, "", new StringBuilder());
  }

  public void deleteByTarget(String accountId, String targetPointerKey) {
    String key = Keys.transactionIntentPointerByTarget(accountId, targetPointerKey);
    pointerStore.delete(key);
  }

  public void deleteByTx(String accountId, String txId, String targetPointerKey) {
    String key = Keys.transactionIntentPointerByTx(accountId, txId, targetPointerKey);
    pointerStore.delete(key);
  }
}
