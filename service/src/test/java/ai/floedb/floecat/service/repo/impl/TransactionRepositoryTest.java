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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TransactionRepositoryTest {

  @Test
  void lifecycleCreateRefusesToPublishWithoutALiveAccount() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var markers = markerStore(pointers);
    var repository = new TransactionRepository(pointers, blobs);
    inject(repository, "markerStore", markers);
    var transaction = Transaction.newBuilder().setAccountId("acct").setTxId("tx-1").build();

    assertThrows(
        BaseResourceRepository.BatchGuardFailedException.class,
        () -> repository.create(transaction));
    assertThat(pointers.get(Keys.transactionPointerById("acct", "tx-1"))).isEmpty();
  }

  @Test
  void accountCleanupRemovesTransactionPointersAndBlobs() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var repository = new TransactionRepository(pointers, blobs);
    repository.create(Transaction.newBuilder().setAccountId("acct").setTxId("tx-1").build());
    String objectUri = Keys.transactionObjectBlobUri("acct", "tx-1", "object-hash");
    blobs.put(objectUri, new byte[] {1}, "application/octet-stream");
    var guard =
        markerStore(pointers).pointerAbsentGuard("account acct", Keys.accountPointerById("acct"));

    var result =
        repository.deleteAccountResources(
            "acct", guard, new BaseResourceRepository.GuardedDeleteProgress());

    assertThat(result.pointersDeleted()).isPositive();
    assertThat(result.blobsDeleted()).isGreaterThanOrEqualTo(2);
    assertThat(
            pointers.listPointersByPrefix(
                Keys.transactionRootPrefix("acct"), 1, "", new StringBuilder()))
        .isEmpty();
    assertThat(blobs.list(Keys.transactionRootPrefix("acct"), 1, "").keys()).isEmpty();
  }

  @Test
  void accountCleanupToleratesAListedBlobThatIsAlreadyAbsent() throws Exception {
    var staleListing = new AtomicBoolean(true);
    var blobs =
        new InMemoryBlobStore() {
          @Override
          public BlobStore.Page list(String prefix, int limit, String pageToken) {
            if (staleListing.getAndSet(false)) {
              return new BlobStore.Page() {
                @Override
                public List<String> keys() {
                  return List.of(prefix + "already-gone.pb");
                }

                @Override
                public String nextToken() {
                  return "";
                }
              };
            }
            return super.list(prefix, limit, pageToken);
          }
        };
    var pointers = new InMemoryPointerStore();
    var repository = new TransactionRepository(pointers, blobs);
    var guard =
        markerStore(pointers).pointerAbsentGuard("account acct", Keys.accountPointerById("acct"));

    var result =
        repository.deleteAccountResources(
            "acct", guard, new BaseResourceRepository.GuardedDeleteProgress());

    assertThat(result.blobsDeleted()).isZero();
  }

  @Test
  void accountCleanupCarriesTheBlobContinuationTokenAcrossPages() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String prefix = Keys.transactionRootPrefix("acct");
    for (int i = 0; i < 1_005; i++) {
      blobs.put(
          prefix + "objects/" + String.format("%04d", i),
          new byte[] {1},
          "application/octet-stream");
    }
    var repository = new TransactionRepository(pointers, blobs);
    var guard =
        markerStore(pointers).pointerAbsentGuard("account acct", Keys.accountPointerById("acct"));

    var result =
        repository.deleteAccountResources(
            "acct", guard, new BaseResourceRepository.GuardedDeleteProgress());

    assertThat(result.blobsDeleted()).isEqualTo(1_005);
    assertThat(blobs.list(prefix, 1, "").keys()).isEmpty();
  }

  @Test
  @Timeout(2)
  void accountCleanupAbortsWhenAListedBlobRemainsAfterDeleteRefusal() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs =
        new InMemoryBlobStore() {
          @Override
          public boolean delete(String uri, String versionId) {
            return false;
          }
        };
    String key = Keys.transactionRootPrefix("acct") + "objects/refused";
    blobs.put(key, new byte[] {1}, "application/octet-stream");
    var repository = new TransactionRepository(pointers, blobs);
    var guard =
        markerStore(pointers).pointerAbsentGuard("account acct", Keys.accountPointerById("acct"));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () ->
            repository.deleteAccountResources(
                "acct", guard, new BaseResourceRepository.GuardedDeleteProgress()));
    assertThat(blobs.head(key)).isPresent();
  }

  private static MarkerStore markerStore(InMemoryPointerStore pointers) throws Exception {
    var markers = new MarkerStore();
    inject(markers, "pointerStore", pointers);
    return markers;
  }

  private static void inject(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
