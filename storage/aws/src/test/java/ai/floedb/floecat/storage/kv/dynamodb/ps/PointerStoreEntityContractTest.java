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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.kv.AbstractEntity;
import ai.floedb.floecat.storage.kv.AbstractEntityTest;
import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import com.google.protobuf.util.Timestamps;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@QuarkusTest
@EnabledIfSystemProperty(named = "floecat.kv", matches = "dynamodb")
public class PointerStoreEntityContractTest extends AbstractEntityTest<Pointer> {

  @Inject PointerStoreEntity pointers;

  @Override
  protected AbstractEntity<Pointer> getEntity() {
    return pointers;
  }

  // ---- Key parsing

  @Test
  void pointerKey_strips_leading_slash() {
    KvStore.Key key = PointerStoreEntity._testKey("/accounts/42/catalog/abc");
    assertEquals("accounts/42", key.partitionKey());
    assertEquals("catalog/abc", key.sortKey());
  }

  @Test
  void pointerKey_global_accounts_by_id_routes_to_GLOBAL_PK() {
    KvStore.Key key = PointerStoreEntity._testKey("/accounts/by-id/123");
    assertEquals(PointerStoreEntity.GLOBAL_PK, key.partitionKey());
    assertEquals("accounts/by-id/123", key.sortKey());
  }

  @Test
  void pointerKey_global_accounts_by_name_routes_to_GLOBAL_PK() {
    KvStore.Key key = PointerStoreEntity._testKey("accounts/by-name/acme");
    assertEquals(PointerStoreEntity.GLOBAL_PK, key.partitionKey());
    assertEquals("accounts/by-name/acme", key.sortKey());
  }

  @Test
  void pointerKey_account_scoped_routes_to_partition_accounts_accountId() {
    KvStore.Key key = PointerStoreEntity._testKey("accounts/77/catalog/xyz");
    assertEquals("accounts/77", key.partitionKey());
    assertEquals("catalog/xyz", key.sortKey());
  }

  @Test
  void pointerKey_rejects_unexpected_key() {
    assertThrows(IllegalArgumentException.class, () -> PointerStoreEntity._testKey("tenants/1/x"));
  }

  @Test
  void pointerKey_rejects_bad_key_missing_second_slash() {
    assertThrows(IllegalArgumentException.class, () -> PointerStoreEntity._testKey("accounts/1"));
  }

  // ---- Prefix parsing

  @Test
  void prefixKey_adds_trailing_slash_when_missing() {
    KvStore.Key key = PointerStoreEntity.prefixKey("accounts/by-id/5");
    assertEquals(PointerStoreEntity.GLOBAL_PK, key.partitionKey());
    assertEquals("accounts/by-id/5/", key.sortKey());
  }

  @Test
  void prefixKey_accounts_root_maps_to_pk_accounts_sk_slash() {
    KvStore.Key key = PointerStoreEntity.prefixKey("accounts");
    assertEquals("accounts", key.partitionKey());
    assertEquals("/", key.sortKey());
  }

  @Test
  void prefixKey_global_prefixes_map_to_GLOBAL_PK() {
    KvStore.Key key = PointerStoreEntity.prefixKey("accounts/by-name/");
    assertEquals(PointerStoreEntity.GLOBAL_PK, key.partitionKey());
    assertEquals("accounts/by-name/", key.sortKey());
  }

  @Test
  void prefixKey_account_prefix_maps_to_partition_accounts_accountId() {
    KvStore.Key key = PointerStoreEntity.prefixKey("accounts/123/catalog");
    assertEquals("accounts/123", key.partitionKey());
    assertEquals("catalog/", key.sortKey());
  }

  @Test
  void prefixKey_rejects_unexpected_prefix() {
    assertThrows(IllegalArgumentException.class, () -> PointerStoreEntity.prefixKey("tenants/1/"));
  }

  @Test
  void prefixKey_rejects_bad_prefix() {
    assertThrows(IllegalArgumentException.class, () -> PointerStoreEntity.prefixKey("accounts//"));
  }

  // ---- CRUD + CAS

  @Test
  void compareAndSet_expectedVersion_0_creates() {
    String key = "accounts/by-id/1/catalog/alpha";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/1").build();

    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    Pointer got = pointers.get(key).await().indefinitely().orElseThrow();
    assertEquals(key, got.getKey());
    assertEquals("s3://b/1", got.getBlobUri());
    assertEquals(1L, got.getVersion());
  }

  @Test
  void compareAndSet_expectedVersion_mismatch_fails() {
    String key = "accounts/by-id/2/catalog/beta";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v1").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    Pointer p2 = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v2").build();
    assertFalse(pointers.compareAndSet(key, 999L, p2).await().indefinitely());

    Pointer got = pointers.get(key).await().indefinitely().orElseThrow();
    assertEquals("s3://b/v1", got.getBlobUri());
    assertEquals(1L, got.getVersion());
  }

  @Test
  void delete_best_effort_absent_returns_false() {
    assertFalse(pointers.delete("accounts/by-id/3/catalog/missing").await().indefinitely());
  }

  @Test
  void delete_best_effort_concurrent_update_can_return_false() {
    String key = "accounts/by-id/4/catalog/race";
    PointerStoreEntity local =
        new PointerStoreEntity(
            new ConcurrentUpdateKvStore(pointers.getKvStore(), PointerStoreEntity._testKey(key)));

    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v1").build();
    assertTrue(local.compareAndSet(key, 0L, p).await().indefinitely());

    assertFalse(local.delete(key).await().indefinitely());
    assertTrue(local.get(key).await().indefinitely().isPresent());
  }

  @Test
  void compareAndDelete_expectedVersion_0_returns_false() {
    String key = "accounts/by-id/5/catalog/gamma";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v1").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    assertFalse(pointers.compareAndDelete(key, 0L).await().indefinitely());
    assertTrue(pointers.get(key).await().indefinitely().isPresent());
  }

  @Test
  void compareAndDelete_expectedVersion_match_deletes() {
    String key = "accounts/by-id/5/catalog/gamma2";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v1").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    assertTrue(pointers.compareAndDelete(key, 1L).await().indefinitely());
    assertTrue(pointers.get(key).await().indefinitely().isEmpty());
  }

  // ---- TTL mapping

  @Test
  void expiresAt_stored_in_ATTR_EXPIRES_AT_seconds() {
    String key = "accounts/by-id/6/catalog/ttl";
    Pointer p =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri("s3://b/ttl")
            .setExpiresAt(Timestamps.fromMillis(1234000L))
            .build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    KvStore.Record rec =
        pointers
            .getKvStore()
            .get(PointerStoreEntity._testKey(key))
            .await()
            .indefinitely()
            .orElseThrow();
    assertEquals("1234", rec.attrs().get(KvAttributes.ATTR_EXPIRES_AT));
  }

  @Test
  void decode_sets_expiresAt_from_ATTR_EXPIRES_AT() {
    KvStore.Record rec =
        new KvStore.Record(
            new KvStore.Key(PointerStoreEntity.GLOBAL_PK, "accounts/by-id/7/catalog/ttl"),
            PointerStoreEntity.KIND_POINTER,
            new byte[0],
            Map.of(KvAttributes.ATTR_EXPIRES_AT, "4321"),
            1L);

    Pointer decoded = pointers.decode(rec);
    assertEquals(4321000L, Timestamps.toMillis(decoded.getExpiresAt()));
  }

  @Test
  void getExpiresAt_returns_0_when_absent() {
    Pointer p = Pointer.newBuilder().setKey("accounts/by-id/8/catalog/ttl").build();
    assertEquals(0L, pointers.getExpiresAt(p));
  }

  // ---- List

  @Test
  void listByPrefix_returns_items_and_nextToken() {
    String prefix = "accounts/by-id/9/p/";
    for (int i = 0; i < 3; i++) {
      String key = prefix + i;
      Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/" + i).build();
      assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());
    }

    var page = pointers.listByPrefix(prefix, 2, Optional.empty()).await().indefinitely();
    assertEquals(2, page.items().size());
    assertTrue(page.nextToken().isPresent());
  }

  @Test
  void listByPrefix_maps_record_to_pointer_correctly() {
    String key = "accounts/by-id/10/catalog/ok";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/ok").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    var page =
        pointers.listByPrefix("accounts/by-id/10/", 10, Optional.empty()).await().indefinitely();
    assertEquals(1, page.items().size());
    Pointer got = page.items().get(0);
    assertEquals(key, got.getKey());
    assertEquals("s3://b/ok", got.getBlobUri());
    assertEquals(1L, got.getVersion());
  }

  @Test
  void listKeysByPrefix_multi_page_returns_all_keys() {
    String prefix = "accounts/by-name/11/p/";
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String key = prefix + i;
      expected.add(key);
      Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/" + i).build();
      assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());
    }

    Set<String> all = new HashSet<>();
    Optional<String> token = Optional.empty();
    do {
      var page = pointers.listKeysByPrefix(prefix, 2, token).await().indefinitely();
      all.addAll(page.items());
      token = page.nextToken();
    } while (token.isPresent());

    assertEquals(new HashSet<>(expected), all);
  }

  @Test
  void listKeysByPrefix_global_partition_returns_keys() {
    String key = "accounts/by-id/99/catalog/x";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/x").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    var page =
        pointers
            .listKeysByPrefix("accounts/by-id/99/", 10, Optional.empty())
            .await()
            .indefinitely();
    assertEquals(List.of("accounts/by-id/99/catalog/x"), page.items());
  }

  @Test
  void listKeysByPrefix_account_partition_returns_keys() {
    String key = "accounts/123/catalog/a";
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/a").build();
    assertTrue(pointers.compareAndSet(key, 0L, p).await().indefinitely());

    var page =
        pointers.listKeysByPrefix("accounts/123/", 10, Optional.empty()).await().indefinitely();
    assertEquals(List.of("/accounts/123/catalog/a"), page.items());
  }

  @Test
  void deleteByPrefix_global_and_account_mix() {
    String globalKey = "accounts/by-id/77/catalog/x";
    String accountKey = "accounts/77/catalog/y";
    Pointer gp = Pointer.newBuilder().setKey(globalKey).setBlobUri("s3://b/x").build();
    Pointer ap = Pointer.newBuilder().setKey(accountKey).setBlobUri("s3://b/y").build();
    assertTrue(pointers.compareAndSet(globalKey, 0L, gp).await().indefinitely());
    assertTrue(pointers.compareAndSet(accountKey, 0L, ap).await().indefinitely());

    assertEquals(1, pointers.deleteByPrefix("accounts/by-id/").await().indefinitely());
    assertTrue(pointers.get(globalKey).await().indefinitely().isEmpty());
    assertTrue(pointers.get(accountKey).await().indefinitely().isPresent());

    assertEquals(1, pointers.deleteByPrefix("accounts/77/").await().indefinitely());
    assertTrue(pointers.get(accountKey).await().indefinitely().isEmpty());
  }

  private static final class ConcurrentUpdateKvStore implements KvStore {
    private final KvStore delegate;
    private final KvStore.Key keyToUpdate;
    private final AtomicBoolean updated = new AtomicBoolean(false);

    private ConcurrentUpdateKvStore(KvStore delegate, KvStore.Key keyToUpdate) {
      this.delegate = delegate;
      this.keyToUpdate = keyToUpdate;
    }

    @Override
    public Uni<Optional<Record>> get(Key key) {
      return delegate
          .get(key)
          .map(
              opt -> {
                if (key.equals(keyToUpdate)
                    && opt.isPresent()
                    && updated.compareAndSet(false, true)) {
                  Record current = opt.get();
                  Map<String, String> attrs = new HashMap<>(current.attrs());
                  attrs.put(PointerStoreEntity.ATTR_BLOB_URI, "s3://b/updated");
                  Record updatedRecord =
                      new Record(
                          current.key(),
                          current.kind(),
                          current.value(),
                          attrs,
                          current.version() + 1);
                  delegate.putCas(updatedRecord, current.version()).await().indefinitely();
                }
                return opt;
              });
    }

    @Override
    public Uni<Boolean> putCas(Record record, long expectedVersion) {
      return delegate.putCas(record, expectedVersion);
    }

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      return delegate.deleteCas(key, expectedVersion);
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
      return delegate.queryByPartitionKeyPrefix(partitionKey, sortKeyPrefix, limit, pageToken);
    }

    @Override
    public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
      return delegate.deleteByPrefix(partitionKey, sortKeyPrefix);
    }

    @Override
    public Uni<Void> reset() {
      return delegate.reset();
    }

    @Override
    public Uni<Boolean> isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public Uni<Void> dump(String header) {
      return delegate.dump(header);
    }

    @Override
    public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
      return delegate.txnWriteCas(ops);
    }
  }
}
