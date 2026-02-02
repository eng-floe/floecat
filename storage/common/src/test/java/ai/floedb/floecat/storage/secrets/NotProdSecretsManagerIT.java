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

package ai.floedb.floecat.storage.secrets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.kv.KvStore;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class NotProdSecretsManagerIT {

  @Test
  void put_get_update_delete_round_trip() {
    Fixture fixture = Fixture.sample();
    NotProdSecretsManager manager = fixture.manager();

    manager.put(fixture.accountId, fixture.secretType, fixture.secretId, fixture.payload);
    Optional<byte[]> stored = manager.get(fixture.accountId, fixture.secretType, fixture.secretId);
    assertTrue(stored.isPresent());
    assertArrayEquals(fixture.payload, stored.get());

    byte[] updated = "beta".getBytes();
    manager.update(fixture.accountId, fixture.secretType, fixture.secretId, updated);
    Optional<byte[]> updatedStored =
        manager.get(fixture.accountId, fixture.secretType, fixture.secretId);
    assertTrue(updatedStored.isPresent());
    assertArrayEquals(updated, updatedStored.get());

    manager.delete(fixture.accountId, fixture.secretType, fixture.secretId);
    assertFalse(manager.get(fixture.accountId, fixture.secretType, fixture.secretId).isPresent());
  }

  @Test
  void key_includes_account_and_type() {
    Fixture fixture = Fixture.sample();
    NotProdSecretsManager manager = fixture.manager();
    manager.put(fixture.accountId, fixture.secretType, fixture.secretId, "payload".getBytes());

    KvStore.Key expectedKey =
        new KvStore.Key(
            "secrets",
            SecretsManager.buildSecretKey(fixture.accountId, fixture.secretType, fixture.secretId));
    KvStore.Record record = fixture.kv.getRecord(expectedKey);
    assertNotNull(record);
    assertEquals("secrets", record.key().partitionKey());
    assertEquals(
        SecretsManager.buildSecretKey(fixture.accountId, fixture.secretType, fixture.secretId),
        record.key().sortKey());
  }

  private static final class Fixture {
    final InMemoryKvStore kv;
    final String accountId;
    final String secretType;
    final String secretId;
    final byte[] payload;

    private Fixture(
        InMemoryKvStore kv, String accountId, String secretType, String secretId, byte[] payload) {
      this.kv = kv;
      this.accountId = accountId;
      this.secretType = secretType;
      this.secretId = secretId;
      this.payload = payload;
    }

    static Fixture sample() {
      return new Fixture(
          new InMemoryKvStore(), "acct-1", "connectors", "conn-9", "alpha".getBytes());
    }

    NotProdSecretsManager manager() {
      NotProdSecretsManager manager = new NotProdSecretsManager();
      manager.kv = kv;
      return manager;
    }
  }

  private static final class InMemoryKvStore implements KvStore {
    private final Map<KvStore.Key, KvStore.Record> records = new ConcurrentHashMap<>();

    @Override
    public Uni<Optional<KvStore.Record>> get(Key key) {
      return Uni.createFrom().item(Optional.ofNullable(records.get(key)));
    }

    @Override
    public Uni<Boolean> putCas(Record record, long expectedVersion) {
      return Uni.createFrom()
          .item(
              records.compute(
                      record.key(),
                      (k, existing) -> {
                        if (expectedVersion == 0L) {
                          return existing == null ? record : existing;
                        }
                        if (existing == null || existing.version() != expectedVersion) {
                          return existing;
                        }
                        return record;
                      })
                  == record);
    }

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      return Uni.createFrom()
          .item(
              records.compute(
                      key,
                      (k, existing) -> {
                        if (existing == null || existing.version() != expectedVersion) {
                          return existing;
                        }
                        return null;
                      })
                  == null);
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
      throw new UnsupportedOperationException("queryByPartitionKeyPrefix not supported");
    }

    @Override
    public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
      throw new UnsupportedOperationException("deleteByPrefix not supported");
    }

    @Override
    public Uni<Void> reset() {
      records.clear();
      return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Boolean> isEmpty() {
      return Uni.createFrom().item(records.isEmpty());
    }

    @Override
    public Uni<Void> dump(String header) {
      return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
      throw new UnsupportedOperationException("txnWriteCas not supported");
    }

    KvStore.Record getRecord(KvStore.Key key) {
      return records.get(key);
    }
  }
}
