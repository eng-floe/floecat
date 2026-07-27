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

import ai.floedb.floecat.storage.kv.AttrValue;
import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.memory.InMemoryKvStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
  void queryByPartitionKeyPrefix_blankPrefix_resumesPositionallyForAbsentKey() {
    assertTrue(kv.putCas(record("pk1", "sk0", 1L, "v0"), 0L).await().indefinitely());
    assertTrue(kv.putCas(record("pk1", "sk2", 1L, "v2"), 0L).await().indefinitely());

    // Token names an absent sort key ("sk1"); a blank prefix has no keyspace boundary, so the
    // scan resumes positionally (DynamoDB exclusiveStartKey semantics) rather than throwing.
    String token = kv.pageTokenAfterKey(key("pk1", "sk1"));
    var page =
        kv.queryByPartitionKeyPrefix("pk1", "", 10, Optional.of(token)).await().indefinitely();

    assertEquals(1, page.items().size());
    assertEquals("sk2", page.items().get(0).key().sortKey());
  }

  @Test
  void pageTokenAfterKey_emptySortKey_throwsInsteadOfEmptyToken() {
    // An empty sort key would base64-encode to "", colliding with the "no more pages" sentinel.
    assertThrows(IllegalArgumentException.class, () -> kv.pageTokenAfterKey(key("pk1", "")));
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

  @Test
  void putCas_rejects_numeric_expiry_stamp() {
    // Refused by the fake exactly as DynamoDB's writer refuses it, so an entity test cannot pass
    // here on a record the real store would reject. Rule and reason: AttrWriteRules.
    KvStore.Key k = key("pk1", "ttl");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            kv.putCas(
                attrsRecord(k, 1L, Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of(2L))), 0L));
    assertTrue(kv.get(k).await().indefinitely().isEmpty());
  }

  @Test
  void putCas_accepts_string_expiry_stamp() {
    KvStore.Key k = key("pk1", "ttl-ok");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of("2"))), 0L)
            .await()
            .indefinitely());
    assertEquals(
        AttrValue.of("2"),
        kv.get(k).await().indefinitely().orElseThrow().attrs().get(KvAttributes.ATTR_EXPIRES_AT));
  }

  @Test
  void txnWriteCas_rejects_numeric_expiry_stamp_before_applying_anything() {
    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", 1L, "v1"), 0L),
            new KvStore.TxnPut(
                attrsRecord(
                    key("pk1", "ttl"), 1L, Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of(2L))),
                0L));

    assertThrows(IllegalArgumentException.class, () -> kv.txnWriteCas(ops).await().indefinitely());
    assertTrue(kv.get(key("pk1", "sk1")).await().indefinitely().isEmpty());
  }

  @Test
  void updateMetadataAttrsIfExists_sets_and_increments_and_bumps_version() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(
                attrsRecord(
                    k,
                    1L,
                    Map.of(
                        "target", AttrValue.of("old"),
                        "hits", AttrValue.of(5L),
                        "keep", AttrValue.of("stay"))),
                0L)
            .await()
            .indefinitely());

    assertEquals(
        Optional.of(2L),
        kv.updateMetadataAttrsIfExists(k, Map.of("target", AttrValue.of("new")), Map.of("hits", 3L))
            .await()
            .indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(2L, got.version());
    assertEquals(AttrValue.of("new"), got.attrs().get("target"));
    assertEquals(AttrValue.of(8L), got.attrs().get("hits"));
    assertEquals(AttrValue.of("stay"), got.attrs().get("keep"));
  }

  @Test
  void updateMetadataAttrsIfExists_increment_of_absent_attr_creates_it_at_the_delta() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("keep", AttrValue.of("stay"))), 0L)
            .await()
            .indefinitely());

    assertEquals(
        Optional.of(2L),
        kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("misses", 4L)).await().indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(AttrValue.of(4L), got.attrs().get("misses"));
    assertEquals(AttrValue.of("stay"), got.attrs().get("keep"));
  }

  @Test
  void updateMetadataAttrsIfExists_absent_key_returns_empty_and_creates_nothing() {
    KvStore.Key k = key("pk1", "ghost");

    assertTrue(
        kv.updateMetadataAttrsIfExists(k, Map.of("target", AttrValue.of("t")), Map.of("hits", 1L))
            .await()
            .indefinitely()
            .isEmpty());

    // No ghost row: the update must never create the record as a side effect.
    assertTrue(kv.get(k).await().indefinitely().isEmpty());
    assertTrue(kv.isEmpty().await().indefinitely());
  }

  @Test
  void updateMetadataAttrsIfExists_refuses_value_carrying_record_and_leaves_it_untouched() {
    // A value payload embeds its own copy of the version, which this update does not rewrite, so
    // the whole record must be refused rather than partially advanced.
    KvStore.Key k = key("pk1", "sk1");
    KvStore.Record stored =
        new KvStore.Record(
            k,
            "KIND",
            "payload".getBytes(StandardCharsets.UTF_8),
            Map.of("target", AttrValue.of("old"), "hits", AttrValue.of(5L)),
            1L);
    assertTrue(kv.putCas(stored, 0L).await().indefinitely());

    assertTrue(
        kv.updateMetadataAttrsIfExists(k, Map.of("target", AttrValue.of("new")), Map.of("hits", 1L))
            .await()
            .indefinitely()
            .isEmpty());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of("old"), got.attrs().get("target"));
    assertEquals(AttrValue.of(5L), got.attrs().get("hits"));
    assertEquals("payload", new String(got.value(), StandardCharsets.UTF_8));
  }

  @Test
  void updateMetadataAttrsIfExists_version_0_record_becomes_version_1() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(kv.putCas(attrsRecord(k, 0L, Map.of()), 0L).await().indefinitely());

    assertEquals(
        Optional.of(1L),
        kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L)).await().indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of(1L), got.attrs().get("hits"));
  }

  @Test
  void updateMetadataAttrsIfExists_sets_only_works() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("target", AttrValue.of("old"))), 0L)
            .await()
            .indefinitely());

    assertEquals(
        Optional.of(2L),
        kv.updateMetadataAttrsIfExists(
                k, Map.of("target", AttrValue.of("new"), "extra", AttrValue.of(9L)), Map.of())
            .await()
            .indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(2L, got.version());
    assertEquals(AttrValue.of("new"), got.attrs().get("target"));
    assertEquals(AttrValue.of(9L), got.attrs().get("extra"));
  }

  @Test
  void updateMetadataAttrsIfExists_increments_only_works() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of(10L))), 0L)
            .await()
            .indefinitely());

    assertEquals(
        Optional.of(2L),
        kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 5L, "misses", 2L))
            .await()
            .indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(2L, got.version());
    assertEquals(AttrValue.of(15L), got.attrs().get("hits"));
    assertEquals(AttrValue.of(2L), got.attrs().get("misses"));
  }

  @Test
  void updateMetadataAttrsIfExists_rejects_empty_updates() {
    // No await(): validation must throw synchronously rather than produce a failed Uni.
    assertThrows(
        IllegalArgumentException.class,
        () -> kv.updateMetadataAttrsIfExists(key("pk1", "meta"), Map.of(), Map.of()));
  }

  @Test
  void updateMetadataAttrsIfExists_rejects_reserved_attr_names_in_sets() {
    for (String name : KvAttributes.RESERVED_ATTRS) {
      Map<String, AttrValue> sets = Map.of(name, AttrValue.of("x"));
      assertThrows(
          IllegalArgumentException.class,
          () -> kv.updateMetadataAttrsIfExists(key("pk1", "meta"), sets, Map.of()));
    }
  }

  @Test
  void updateMetadataAttrsIfExists_rejects_attr_that_is_both_set_and_incremented() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            kv.updateMetadataAttrsIfExists(
                key("pk1", "meta"), Map.of("hits", AttrValue.of(1L)), Map.of("hits", 1L)));
  }

  @Test
  void updateMetadataAttrsIfExists_increment_of_string_attr_fails() {
    // The in-memory store guards on the AttrValue type and throws IllegalStateException; the
    // DynamoDB store has no local view of the stored type and lets the SDK's ValidationException
    // surface instead, so the two contract tests assert different exception types here by design.
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of("abc"))), 0L)
            .await()
            .indefinitely());

    assertThrows(
        IllegalStateException.class,
        () ->
            kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L)).await().indefinitely());

    // Never silently overwritten: the record is left exactly as it was.
    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of("abc"), got.attrs().get("hits"));
  }

  @Test
  void updateMetadataAttrsIfExists_increment_of_numeric_looking_string_attr_fails() {
    // "42" parses, but DynamoDB's ADD rejects an S-typed attribute regardless of its content, so
    // the in-memory store rejects it too — otherwise a caller would pass here and fail in prod.
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of("42"))), 0L)
            .await()
            .indefinitely());

    assertThrows(
        IllegalStateException.class,
        () ->
            kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L)).await().indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of("42"), got.attrs().get("hits"));
  }

  @Test
  void updateMetadataAttrsIfExists_version_bump_is_visible_to_putCas() {
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of(1L))), 0L).await().indefinitely());

    assertEquals(
        Optional.of(2L),
        kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L)).await().indefinitely());

    // A writer that read the record before the metadata bump must be rejected...
    assertFalse(
        kv.putCas(attrsRecord(k, 2L, Map.of("hits", AttrValue.of(99L))), 1L)
            .await()
            .indefinitely());
    // ...and one that re-read afterwards must succeed.
    assertTrue(
        kv.putCas(attrsRecord(k, 3L, Map.of("hits", AttrValue.of(99L))), 2L)
            .await()
            .indefinitely());
    assertEquals(3L, kv.get(k).await().indefinitely().orElseThrow().version());
  }

  @Test
  void updateMetadataAttrsIfExists_concurrent_increments_all_land() throws Exception {
    int writers = 50;
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of(0L))), 0L).await().indefinitely());

    // One call on this thread first, against a key that does not exist (so it changes nothing but
    // still builds a Uni). Mutiny initializes its context-propagation interceptor on first use, and
    // that initialization is not itself thread-safe: without this, several of the writers below can
    // race into it and fail with "ContextManagerProvider already set". A Quarkus process has it set
    // up at startup; a bare JVM test has to warm it explicitly.
    kv.updateMetadataAttrsIfExists(key("pk1", "absent"), Map.of(), Map.of("hits", 1L))
        .await()
        .indefinitely();

    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Optional<Long>>> results = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  return kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L))
                      .await()
                      .indefinitely();
                }));
      }
      start.countDown();

      Set<Long> versions = new TreeSet<>();
      for (Future<Optional<Long>> result : results) {
        versions.add(result.get(30, TimeUnit.SECONDS).orElseThrow());
      }
      // Every caller got a distinct version; a get-then-put implementation would hand out dupes.
      assertEquals(writers, versions.size());
    } finally {
      pool.shutdownNow();
    }

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(AttrValue.of((long) writers), got.attrs().get("hits"));
    assertEquals(1L + writers, got.version());
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

  private static KvStore.Record attrsRecord(
      KvStore.Key key, long version, Map<String, AttrValue> attrs) {
    return new KvStore.Record(key, "META", new byte[0], attrs, version);
  }
}
