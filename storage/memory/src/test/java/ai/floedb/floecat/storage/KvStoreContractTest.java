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
package ai.floedb.floecat.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.memory.InMemoryKvStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KvStoreContractTest {
  private KvStore kv;

  @BeforeEach
  void setUp() {
    kv = new InMemoryKvStore();
  }

  @Test
  void get_returns_empty_when_absent() {
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isEmpty());
  }

  @Test
  void putCas_and_deleteCas_round_trip() {
    KvStore.Record rec = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec, 0L).await().indefinitely());
    assertEquals(
        "v1",
        new String(
            kv.get(key("pk1", "sk1")).await().indefinitely().orElseThrow().value(),
            StandardCharsets.UTF_8));
    assertTrue(kv.deleteCas(key("pk1", "sk1"), 1L).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isEmpty());
  }

  @Test
  void queryByPartitionKeyPrefix_pages_by_sort_key() {
    putSeries("pk1", "sk", 5);

    Set<String> seen = new TreeSet<>();
    Optional<String> token = Optional.empty();
    do {
      var page = kv.queryByPartitionKeyPrefix("pk1", "sk", 2, token).await().indefinitely();
      for (var rec : page.items()) {
        seen.add(rec.key().sortKey());
      }
      token = page.nextToken();
    } while (token.isPresent());

    assertEquals(Set.of("sk0", "sk1", "sk2", "sk3", "sk4"), seen);
  }

  @Test
  void queryByPartitionKeyPrefix_rejects_bad_token() {
    putSeries("pk1", "sk", 1);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            kv.queryByPartitionKeyPrefix("pk1", "sk", 1, Optional.of("bad-token"))
                .await()
                .indefinitely());
  }

  @Test
  void deleteByPrefix_removes_matching_records() {
    putSeries("pk1", "sk", 3);
    putSeries("pk2", "sk", 2);

    assertEquals(3, kv.deleteByPrefix("pk1", "sk").await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk0")).await().indefinitely().isEmpty());
    assertFalse(kv.get(key("pk2", "sk0")).await().indefinitely().isEmpty());
  }

  @Test
  void txnWriteCas_is_atomic() {
    assertTrue(kv.putCas(record("pk1", "sk1", 1L, "v1"), 0L).await().indefinitely());

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk2", 1L, "v2"), 0L),
            new KvStore.TxnPut(record("pk1", "sk1", 2L, "nope"), 0L));

    assertFalse(kv.txnWriteCas(ops).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk2")).await().indefinitely().isEmpty());
  }

  private void putSeries(String pk, String skPrefix, int count) {
    for (int i = 0; i < count; i++) {
      assertTrue(kv.putCas(record(pk, skPrefix + i, 1L, "v" + i), 0L).await().indefinitely());
    }
  }

  private static KvStore.Key key(String pk, String sk) {
    return new KvStore.Key(pk, sk);
  }

  private static KvStore.Record record(String pk, String sk, long version, String value) {
    return new KvStore.Record(
        key(pk, sk), "KIND", value.getBytes(StandardCharsets.UTF_8), null, version);
  }
}
