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
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.kv.AttrValue;
import ai.floedb.floecat.storage.kv.KvStore;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KvPointerStoreTest {

  @Test
  void deleteByPrefixExcludingPreservesEncodedAccountFence() {
    FailingReadKvStore kv = new FailingReadKvStore(new RuntimeException("unused"));
    KvPointerStore store = new KvPointerStore(new PointerStoreEntity(kv)) {};

    assertEquals(
        7, store.deleteByPrefixExcluding("/accounts/acct%2B1/", "/accounts/acct%2B1/deleting"));
    assertEquals("accounts/acct%2B1", kv.deletedPartitionKey);
    assertEquals("", kv.deletedSortKeyPrefix);
    assertEquals("deleting", kv.excludedSortKey);
  }

  @Test
  void getRethrowsNonClosedPoolRuntimeException() {
    RuntimeException failure = new RuntimeException("validation failed");
    KvPointerStore store = pointerStoreFailingReadsWith(failure);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> store.get("/accounts/acct-1/catalog/cat-1"));

    assertSame(failure, thrown);
  }

  @Test
  void getWrapsClosedPoolRuntimeExceptionAsRetryableAbort() {
    RuntimeException failure = new RuntimeException("Connection pool shut down");
    KvPointerStore store = pointerStoreFailingReadsWith(failure);

    StorageAbortRetryableException thrown =
        assertThrows(
            StorageAbortRetryableException.class,
            () -> store.get("/accounts/acct-1/catalog/cat-1"));

    assertSame(failure, thrown.getCause());
  }

  private static KvPointerStore pointerStoreFailingReadsWith(RuntimeException failure) {
    return new KvPointerStore(new PointerStoreEntity(new FailingReadKvStore(failure))) {};
  }

  private static final class FailingReadKvStore implements KvStore {
    private final RuntimeException failure;
    private String deletedPartitionKey;
    private String deletedSortKeyPrefix;
    private String excludedSortKey;

    private FailingReadKvStore(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public Uni<Optional<Record>> get(Key key) {
      return Uni.createFrom().failure(failure);
    }

    @Override
    public Uni<Boolean> putCas(Record record, long expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Optional<Long>> updateMetadataAttrsIfExists(
        Key key, Map<String, AttrValue> sets, Map<String, Long> increments) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey,
        String sortKeyPrefix,
        int limit,
        Optional<String> pageToken,
        boolean consistentRead) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Integer> deleteByPrefixExcluding(
        String partitionKey, String sortKeyPrefix, String excludedSortKey) {
      this.deletedPartitionKey = partitionKey;
      this.deletedSortKeyPrefix = sortKeyPrefix;
      this.excludedSortKey = excludedSortKey;
      return Uni.createFrom().item(7);
    }

    @Override
    public Uni<Void> reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Boolean> isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Void> dump(String header) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
      throw new UnsupportedOperationException();
    }
  }
}
