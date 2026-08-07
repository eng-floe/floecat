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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TransactionKey;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.GuardedBlobPrefixSweeper;
import ai.floedb.floecat.service.repo.util.MarkerStore;
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
  private final BlobStore blobStore;
  @Inject MarkerStore markerStore;

  @Inject
  public TransactionRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.blobStore = blobStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TRANSACTION,
            Transaction::parseFrom,
            Transaction::toByteArray,
            "application/x-protobuf");
  }

  public void create(Transaction txn) {
    repo.create(txn, accountLiveGuard(txn.getAccountId()));
  }

  public void create(Transaction txn, BatchGuard guard) {
    repo.create(txn, BatchGuard.all(accountLiveGuard(txn.getAccountId()), guard));
  }

  public boolean update(Transaction txn, long expectedPointerVersion) {
    return repo.update(txn, expectedPointerVersion, accountLiveGuard(txn.getAccountId()));
  }

  public boolean update(Transaction txn, long expectedPointerVersion, BatchGuard guard) {
    return repo.update(
        txn, expectedPointerVersion, BatchGuard.all(accountLiveGuard(txn.getAccountId()), guard));
  }

  private BatchGuard accountLiveGuard(String accountId) {
    // Directly constructed repositories are used by narrow storage tests. CDI production
    // instances always receive MarkerStore and therefore always fence lifecycle publications.
    if (markerStore == null) {
      return BatchGuard.NONE;
    }
    return markerStore
        .accountLiveGuard(accountId)
        .orElseThrow(
            () ->
                new BaseResourceRepository.BatchGuardFailedException(
                    "account disappeared during transaction mutation: " + accountId));
  }

  /**
   * Removes the complete transaction keyspace while account absence remains pinned.
   *
   * <p>Pointer rows carry the guard in their delete batches. Blobs cannot join a pointer-store
   * batch, so each delete targets the immutable object version observed before the absence guard
   * was rechecked. A replacement account can therefore break the sweep, but it cannot have a new
   * version of the same object removed by an old account's cleanup.
   */
  public CleanupResult deleteAccountResources(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    String prefix = Keys.transactionRootPrefix(accountId);
    int pointersDeleted = repo.deleteByPrefix(prefix, accountGone, deleteProgress);
    int blobsDeleted =
        GuardedBlobPrefixSweeper.delete(blobStore, prefix, accountGone, deleteProgress);
    return new CleanupResult(pointersDeleted, blobsDeleted);
  }

  public record CleanupResult(int pointersDeleted, int blobsDeleted) {}

  public Optional<Transaction> getById(String accountId, String txId) {
    return repo.getByKey(TransactionKey.byId(accountId, txId));
  }

  public MutationMeta metaFor(String accountId, String txId) {
    return repo.metaFor(TransactionKey.byId(accountId, txId));
  }

  public MutationMeta metaFor(String accountId, String txId, Timestamp nowTs) {
    return repo.metaFor(TransactionKey.byId(accountId, txId), nowTs);
  }
}
