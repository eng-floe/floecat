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
            Schemas.TRANSACTION_INTENT,
            TransactionIntent::parseFrom,
            TransactionIntent::toByteArray,
            "application/x-protobuf");
  }

  public void create(TransactionIntent intent) {
    repo.create(intent);
  }

  public boolean update(TransactionIntent intent) {
    String key =
        Keys.transactionIntentPointerByTarget(intent.getAccountId(), intent.getTargetPointerKey());
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return false;
    }
    return repo.update(intent, ptr.getVersion());
  }

  public Optional<TransactionIntent> getByTarget(String accountId, String targetPointerKey) {
    return repo.get(Keys.transactionIntentPointerByTarget(accountId, targetPointerKey));
  }

  public Optional<ai.floedb.floecat.common.rpc.Pointer> getTargetPointer(
      String accountId, String targetPointerKey) {
    String key = Keys.transactionIntentPointerByTarget(accountId, targetPointerKey);
    return pointerStore.get(key);
  }

  public List<TransactionIntent> listByTx(String accountId, String txId) {
    String prefix = Keys.transactionIntentPointerByTxPrefix(accountId, txId);
    return repo.listByPrefix(prefix, Integer.MAX_VALUE, "", new StringBuilder());
  }

  public boolean deleteByTargetIfOwned(
      String accountId, String targetPointerKey, String expectedOwnerTxId) {
    String key = Keys.transactionIntentPointerByTarget(accountId, targetPointerKey);
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null || !pointerOwnedByTransaction(key, expectedOwnerTxId, targetPointerKey)) {
      return false;
    }
    return pointerStore.compareAndDelete(key, ptr.getVersion());
  }

  public boolean deleteByTxIfOwned(String accountId, String txId, String targetPointerKey) {
    String key = Keys.transactionIntentPointerByTx(accountId, txId, targetPointerKey);
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null || !pointerOwnedByTransaction(key, txId, targetPointerKey)) {
      return false;
    }
    return pointerStore.compareAndDelete(key, ptr.getVersion());
  }

  public void deleteBothIndices(TransactionIntent intent) {
    deleteBothIndicesBestEffort(intent);
  }

  public boolean deleteBothIndicesBestEffort(TransactionIntent intent) {
    if (intent == null) {
      return true;
    }
    for (int attempt = 0; attempt < 3; attempt++) {
      deleteByTxIfOwned(intent.getAccountId(), intent.getTxId(), intent.getTargetPointerKey());
      deleteByTargetIfOwned(intent.getAccountId(), intent.getTargetPointerKey(), intent.getTxId());
      var current = getByTarget(intent.getAccountId(), intent.getTargetPointerKey()).orElse(null);
      boolean canonicalGoneOrNotMine =
          current == null || !intent.getTxId().equals(current.getTxId());
      if (canonicalGoneOrNotMine
          && listByTx(intent.getAccountId(), intent.getTxId()).stream()
              .noneMatch(existing -> matchesIntent(existing, intent))) {
        return true;
      }
    }
    return false;
  }

  private boolean pointerOwnedByTransaction(
      String pointerKey, String txId, String targetPointerKey) {
    if (txId == null || txId.isBlank() || targetPointerKey == null || targetPointerKey.isBlank()) {
      return false;
    }
    return repo.get(pointerKey)
        .map(
            existing ->
                txId.equals(existing.getTxId())
                    && targetPointerKey.equals(existing.getTargetPointerKey()))
        .orElse(false);
  }

  private boolean matchesIntent(TransactionIntent left, TransactionIntent right) {
    if (left == null || right == null) {
      return false;
    }
    return left.getTxId().equals(right.getTxId())
        && left.getAccountId().equals(right.getAccountId())
        && left.getTargetPointerKey().equals(right.getTargetPointerKey());
  }
}
