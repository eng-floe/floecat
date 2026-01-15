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
package ai.floedb.floecat.storage.kv.dynamodb;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@QuarkusTest
@EnabledIfSystemProperty(named = "floecat.kv", matches = "dynamodb")
public class KvStoreContractTest {

  @Inject
  @KvTable("floecat")
  KvStore kv;

  @BeforeEach
  void resetTable() {
    kv.reset().await().indefinitely();
  }

  @Test
  void get_returns_empty_when_absent() {
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isEmpty());
  }

  @Test
  void putCas_create_if_absent_expectedVersion_0_succeeds() {
    KvStore.Record rec = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec, 0L).await().indefinitely());
  }

  @Test
  void putCas_create_if_absent_expectedVersion_0_fails_if_exists() {
    KvStore.Record rec1 = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec1, 0L).await().indefinitely());

    KvStore.Record rec2 = record("pk1", "sk1", 1L, "v2");
    assertFalse(kv.putCas(rec2, 0L).await().indefinitely());

    KvStore.Record got = kv.get(key("pk1", "sk1")).await().indefinitely().orElseThrow();
    assertEquals("v1", new String(got.value(), StandardCharsets.UTF_8));
  }

  @Test
  void putCas_update_expectedVersion_matches_succeeds() {
    KvStore.Record rec1 = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec1, 0L).await().indefinitely());

    KvStore.Record rec2 = record("pk1", "sk1", 2L, "v2");
    assertTrue(kv.putCas(rec2, 1L).await().indefinitely());

    KvStore.Record got = kv.get(key("pk1", "sk1")).await().indefinitely().orElseThrow();
    assertEquals(2L, got.version());
    assertEquals("v2", new String(got.value(), StandardCharsets.UTF_8));
  }

  @Test
  void putCas_update_expectedVersion_mismatch_returns_false() {
    KvStore.Record rec1 = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec1, 0L).await().indefinitely());

    KvStore.Record rec2 = record("pk1", "sk1", 2L, "v2");
    assertFalse(kv.putCas(rec2, 999L).await().indefinitely());
  }

  @Test
  void deleteCas_expectedVersion_mismatch_returns_false() {
    KvStore.Record rec1 = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec1, 0L).await().indefinitely());

    assertFalse(kv.deleteCas(key("pk1", "sk1"), 999L).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isPresent());
  }

  @Test
  void deleteCas_expectedVersion_matches_deletes() {
    KvStore.Record rec1 = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(rec1, 0L).await().indefinitely());

    assertTrue(kv.deleteCas(key("pk1", "sk1"), 1L).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isEmpty());
  }

  @Test
  void txnWriteCas_all_success_commits() {
    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", 1L, "v1"), 0L),
            new KvStore.TxnPut(record("pk1", "sk2", 1L, "v2"), 0L));

    assertTrue(kv.txnWriteCas(ops).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isPresent());
    assertTrue(kv.get(key("pk1", "sk2")).await().indefinitely().isPresent());
  }

  @Test
  void txnWriteCas_any_condition_failure_returns_false_and_no_partial_writes() {
    KvStore.Record existing = record("pk1", "sk1", 1L, "v1");
    assertTrue(kv.putCas(existing, 0L).await().indefinitely());

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", 1L, "new"), 0L),
            new KvStore.TxnPut(record("pk1", "sk2", 1L, "v2"), 0L));

    assertFalse(kv.txnWriteCas(ops).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isPresent());
    assertTrue(kv.get(key("pk1", "sk2")).await().indefinitely().isEmpty());
  }

  @Test
  void record_null_attrs_becomes_empty_map() {
    KvStore.Record rec = new KvStore.Record(key("pk1", "sk1"), "K", new byte[0], null, 1L);
    assertNotNull(rec.attrs());
    assertTrue(rec.attrs().isEmpty());
  }

  @Test
  void record_null_value_becomes_empty_bytes() {
    KvStore.Record rec = new KvStore.Record(key("pk1", "sk1"), "K", null, null, 1L);
    assertNotNull(rec.value());
    assertEquals(0, rec.value().length);
  }

  @Test
  void queryByPartitionKeyPrefix_single_page_no_token() {
    putSeries("pk1", "sk", 2);

    var page =
        kv.queryByPartitionKeyPrefix("pk1", "sk", 10, Optional.empty()).await().indefinitely();
    assertEquals(2, page.items().size());
    assertTrue(page.nextToken().isEmpty());
  }

  @Test
  void queryByPartitionKeyPrefix_multi_page_respects_limit_and_token_round_trip() {
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
  void queryByPartitionKeyPrefix_token_invalid() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            kv.queryByPartitionKeyPrefix("pk1", "sk", 2, Optional.of("bad-token"))
                .await()
                .indefinitely());
  }

  private void putSeries(String pk, String skPrefix, int count) {
    for (int i = 0; i < count; i++) {
      KvStore.Record rec = record(pk, skPrefix + i, 1L, "v" + i);
      assertTrue(kv.putCas(rec, 0L).await().indefinitely());
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
