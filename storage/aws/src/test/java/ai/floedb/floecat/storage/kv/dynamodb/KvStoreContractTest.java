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

import ai.floedb.floecat.storage.kv.AttrValue;
import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@QuarkusTest
@TestProfile(DynamoDbKvTestProfile.class)
@EnabledIfSystemProperty(named = "floecat.kv", matches = "dynamodb")
public class KvStoreContractTest {

  @Inject
  @KvTable("floecat")
  KvStore kv;

  /** Used only to plant rows the KvStore API cannot express (no version, raw attribute types). */
  @Inject DynamoDbAsyncClient ddb;

  @ConfigProperty(name = "floecat.kv.table")
  String kvTable;

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
  void putCas_rejects_numeric_expiry_stamp() {
    // Written as N, the stamp is invisible to a replica that predates typed attributes, whose next
    // whole-record write then drops the expiry for good. Rule and reason: AttrWriteRules.
    KvStore.Key k = key("pk1", "ttl");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            kv.putCas(
                attrsRecord(k, 1L, Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of(2L))), 0L));
    assertTrue(kv.get(k).await().indefinitely().isEmpty());
  }

  @Test
  void putCas_accepts_string_expiry_stamp_and_stores_it_as_S() {
    KvStore.Key k = key("pk1", "ttl-ok");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of("2"))), 0L)
            .await()
            .indefinitely());

    Map<String, AttributeValue> raw = getRawItem("pk1", "ttl-ok");
    assertEquals("2", raw.get(KvAttributes.ATTR_EXPIRES_AT).s());
    assertNull(raw.get(KvAttributes.ATTR_EXPIRES_AT).n());
  }

  @Test
  void txnWriteCas_rejects_numeric_expiry_stamp_before_issuing_the_transaction() {
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
  void numeric_expiry_stamp_written_out_of_band_stays_readable() {
    // Writes are strict, reads are not: a foreign writer's N-typed stamp must still decode, which
    // is
    // the whole reason AttrValue.asLong accepts both forms.
    Map<String, AttributeValue> item = rawItem("pk1", "foreign-ttl");
    item.put(KvAttributes.ATTR_VERSION, AttributeValue.fromN("1"));
    item.put(KvAttributes.ATTR_EXPIRES_AT, AttributeValue.fromN("4321"));
    putRawItem(item);

    KvStore.Record got = kv.get(key("pk1", "foreign-ttl")).await().indefinitely().orElseThrow();
    assertEquals(AttrValue.of(4321L), got.attrs().get(KvAttributes.ATTR_EXPIRES_AT));
    assertEquals(4321L, got.attrs().get(KvAttributes.ATTR_EXPIRES_AT).asLong());
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
    // putCas refuses a record at version 0, so the row is planted directly. A stored 0 and a
    // missing version attribute are the same starting point per the SPI, so both are checked.
    Map<String, AttributeValue> zero = rawItem("pk1", "zero");
    zero.put(KvAttributes.ATTR_VERSION, AttributeValue.fromN("0"));
    putRawItem(zero);

    assertEquals(
        Optional.of(1L),
        kv.updateMetadataAttrsIfExists(key("pk1", "zero"), Map.of(), Map.of("hits", 1L))
            .await()
            .indefinitely());

    KvStore.Record got = kv.get(key("pk1", "zero")).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of(1L), got.attrs().get("hits"));

    putRawItem(rawItem("pk1", "noversion"));
    assertEquals(
        Optional.of(1L),
        kv.updateMetadataAttrsIfExists(key("pk1", "noversion"), Map.of(), Map.of("hits", 1L))
            .await()
            .indefinitely());
    assertEquals(
        1L, kv.get(key("pk1", "noversion")).await().indefinitely().orElseThrow().version());
  }

  @Test
  void updateMetadataAttrsIfExists_sets_only_works() {
    // The branch where the update expression must carry SET but omit nothing else; an empty ADD
    // or SET clause is a DynamoDB ValidationException.
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
    // The branch where the SET clause must be omitted entirely.
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
    // DynamoDB's ADD on an S-typed attribute is a server-side ValidationException, which the SDK
    // does not model, so it arrives as a plain DynamoDbException. The in-memory store has the
    // stored AttrValue in hand and throws IllegalStateException instead — hence the two contract
    // tests assert different exception types for this case by design.
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of("abc"))), 0L)
            .await()
            .indefinitely());

    DynamoDbException thrown =
        assertThrows(
            DynamoDbException.class,
            () ->
                kv.updateMetadataAttrsIfExists(k, Map.of(), Map.of("hits", 1L))
                    .await()
                    .indefinitely());
    // Must not be the one failure the store recovers into an empty result: a type error is not
    // "the record was absent".
    assertFalse(thrown instanceof ConditionalCheckFailedException);

    // Never silently overwritten, and the version bump in the same request did not land either.
    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertEquals(1L, got.version());
    assertEquals(AttrValue.of("abc"), got.attrs().get("hits"));
  }

  @Test
  void updateMetadataAttrsIfExists_increment_of_numeric_looking_string_attr_fails() {
    // "42" parses fine, but ADD rejects the S type regardless of its content.
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(attrsRecord(k, 1L, Map.of("hits", AttrValue.of("42"))), 0L)
            .await()
            .indefinitely());

    assertThrows(
        DynamoDbException.class,
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
  void number_attr_round_trips_and_stays_visible_on_read() {
    // The read path used to drop every non-string attribute type; a NumberValue written through
    // putCas must come back as a NumberValue, not vanish.
    KvStore.Key k = key("pk1", "meta");
    assertTrue(
        kv.putCas(
                attrsRecord(k, 1L, Map.of("hits", AttrValue.of(7L), "target", AttrValue.of("t"))),
                0L)
            .await()
            .indefinitely());

    KvStore.Record got = kv.get(k).await().indefinitely().orElseThrow();
    assertInstanceOf(AttrValue.NumberValue.class, got.attrs().get("hits"));
    assertEquals(AttrValue.of(7L), got.attrs().get("hits"));
    assertEquals(7L, got.attrs().get("hits").asLong());
    assertEquals(AttrValue.of("t"), got.attrs().get("target"));
  }

  @Test
  void raw_numeric_and_string_attributes_read_back_with_their_native_types() {
    // Written out-of-band so the native DynamoDB type, not this store's writer, decides the type.
    Map<String, AttributeValue> item = rawItem("pk1", "raw");
    item.put(KvAttributes.ATTR_VERSION, AttributeValue.fromN("3"));
    item.put("nativeNumber", AttributeValue.fromN("42"));
    item.put("nativeString", AttributeValue.fromS("42"));
    putRawItem(item);

    KvStore.Record got = kv.get(key("pk1", "raw")).await().indefinitely().orElseThrow();
    assertEquals(3L, got.version());
    assertEquals(new AttrValue.NumberValue(42L), got.attrs().get("nativeNumber"));
    assertEquals(new AttrValue.StringValue("42"), got.attrs().get("nativeString"));
  }

  private void putSeries(String pk, String skPrefix, int count) {
    for (int i = 0; i < count; i++) {
      KvStore.Record rec = record(pk, skPrefix + i, 1L, "v" + i);
      assertTrue(kv.putCas(rec, 0L).await().indefinitely());
    }
  }

  private void putRawItem(Map<String, AttributeValue> item) {
    ddb.putItem(PutItemRequest.builder().tableName(kvTable).item(item).build()).join();
  }

  /**
   * The stored item with its native DynamoDB types, for assertions this store's reader would hide.
   */
  private Map<String, AttributeValue> getRawItem(String pk, String sk) {
    return ddb.getItem(
            GetItemRequest.builder()
                .tableName(kvTable)
                .key(
                    Map.of(
                        KvAttributes.ATTR_PARTITION_KEY,
                        AttributeValue.fromS(pk),
                        KvAttributes.ATTR_SORT_KEY,
                        AttributeValue.fromS(sk)))
                .build())
        .join()
        .item();
  }

  private static Map<String, AttributeValue> rawItem(String pk, String sk) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KvAttributes.ATTR_PARTITION_KEY, AttributeValue.fromS(pk));
    item.put(KvAttributes.ATTR_SORT_KEY, AttributeValue.fromS(sk));
    item.put(KvAttributes.ATTR_KIND, AttributeValue.fromS("META"));
    return item;
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
