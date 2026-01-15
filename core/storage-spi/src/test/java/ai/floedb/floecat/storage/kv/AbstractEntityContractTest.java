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
package ai.floedb.floecat.storage.kv;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Pointer;
import com.google.protobuf.util.Timestamps;
import io.smallrye.mutiny.Uni;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

public class AbstractEntityContractTest {

  @Test
  void listEntities_returns_all_items_across_multiple_pages() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    for (int i = 0; i < 120; i++) {
      kv.records.put(
          key("pk", "sk" + i),
          record("pk", "sk" + i, TestEntity.KIND, pointerBytes("k" + i), Map.of(), 1L));
    }

    var all = entity.listEntities("pk", "sk").await().indefinitely();
    assertEquals(120, all.size());
  }

  @Test
  void listEntities_filters_by_kind_only() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    kv.records.put(
        key("pk", "sk1"), record("pk", "sk1", TestEntity.KIND, pointerBytes("k1"), Map.of(), 1L));
    kv.records.put(
        key("pk", "sk2"), record("pk", "sk2", "OtherKind", pointerBytes("k2"), Map.of(), 1L));

    var all = entity.listEntities("pk", "sk").await().indefinitely();
    assertEquals(1, all.size());
    assertTrue(all.containsKey(key("pk", "sk1")));
    assertFalse(all.containsKey(key("pk", "sk2")));
  }

  @Test
  void listEntities_empty_when_no_matches() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    kv.records.put(
        key("pk", "sk1"), record("pk", "sk1", TestEntity.KIND, pointerBytes("k1"), Map.of(), 1L));

    var all = entity.listEntities("pk", "missing").await().indefinitely();
    assertTrue(all.isEmpty());
  }

  @Test
  void listEntities_handles_empty_page_items_but_token_present() {
    TestKvStore kv = new TestKvStore();
    kv.setCannedPages(
        List.of(
            new KvStore.Page(List.of(), Optional.of("next")),
            new KvStore.Page(
                List.of(record("pk", "sk1", TestEntity.KIND, pointerBytes("k1"), Map.of(), 1L)),
                Optional.empty())));

    TestEntity entity = new TestEntity(kv);
    var all = entity.listEntities("pk", "sk").await().indefinitely();
    assertEquals(1, all.size());
    assertTrue(all.containsKey(key("pk", "sk1")));
  }

  @Test
  void listRecords_returns_all_records_across_multiple_pages() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    for (int i = 0; i < 120; i++) {
      kv.records.put(
          key("pk", "sk" + i),
          record(
              "pk",
              "sk" + i,
              "Kind" + i,
              ("v" + i).getBytes(StandardCharsets.UTF_8),
              Map.of(),
              1L));
    }

    var all = entity.listRecords("pk", "sk").await().indefinitely();
    assertEquals(120, all.size());
  }

  @Test
  void nextVersion_expected_0_returns_1() {
    assertEquals(1L, TestEntity.nextVersionForTest(0L));
  }

  @Test
  void nextVersion_negative_throws() {
    assertThrows(IllegalArgumentException.class, () -> TestEntity.nextVersionForTest(-1L));
  }

  @Test
  void putCanonicalCas_sets_record_version_and_message_version() throws Exception {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    Pointer p = Pointer.newBuilder().setKey("k1").build();
    assertTrue(
        entity.putCanonicalCasForTest(key("pk", "sk1"), p, Map.of(), 0L).await().indefinitely());

    KvStore.Record rec = kv.records.get(key("pk", "sk1"));
    assertNotNull(rec);
    assertEquals(1L, rec.version());

    Pointer stored = Pointer.parseFrom(rec.value());
    assertEquals(1L, stored.getVersion());
  }

  @Test
  void putCanonicalCas_adds_expires_at_attribute_when_nonzero() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    Pointer p =
        Pointer.newBuilder().setKey("k1").setExpiresAt(Timestamps.fromMillis(2000L)).build();

    assertTrue(
        entity.putCanonicalCasForTest(key("pk", "sk1"), p, Map.of(), 0L).await().indefinitely());

    KvStore.Record rec = kv.records.get(key("pk", "sk1"));
    assertEquals("2", rec.attrs().get(KvAttributes.ATTR_EXPIRES_AT));
  }

  @Test
  void deleteCas_calls_kv_deleteCas_with_expectedVersion() {
    RecordingKvStore kv = new RecordingKvStore();
    TestEntity entity = new TestEntity(kv);

    KvStore.Key key = key("pk", "sk");
    kv.records.put(key, record("pk", "sk", TestEntity.KIND, pointerBytes("k1"), Map.of(), 7L));

    assertTrue(entity.deleteCasForTest(key, 7L).await().indefinitely());
    assertEquals(key, kv.lastDeleteKey);
    assertEquals(7L, kv.lastDeleteExpectedVersion);
  }

  @Test
  void getViaPointer_missing_pointer_returns_empty() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    assertTrue(entity.getViaPointerForTest(key("pk", "sk")).await().indefinitely().isEmpty());
  }

  @Test
  void getViaPointer_pointer_missing_target_attrs_returns_empty() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    KvStore.Key ptrKey = key("pk", "sk");
    kv.records.put(ptrKey, record("pk", "sk", "Pointer", new byte[0], Map.of(), 1L));

    assertTrue(entity.getViaPointerForTest(ptrKey).await().indefinitely().isEmpty());
  }

  @Test
  void getViaPointer_reads_target_record() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    KvStore.Key ptrKey = key("p", "ptr");
    KvStore.Key targetKey = key("tp", "ts");
    kv.records.put(
        ptrKey,
        record(
            "p",
            "ptr",
            "Pointer",
            new byte[0],
            Map.of(KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "ts"),
            1L));
    kv.records.put(
        targetKey, record("tp", "ts", TestEntity.KIND, pointerBytes("target"), Map.of(), 1L));

    var got = entity.getViaPointerForTest(ptrKey).await().indefinitely().orElseThrow();
    assertEquals("target", got.getKey());
  }

  @Test
  void getViaPointer_target_missing_returns_empty() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    KvStore.Key ptrKey = key("p", "ptr");
    kv.records.put(
        ptrKey,
        record(
            "p",
            "ptr",
            "Pointer",
            new byte[0],
            Map.of(
                KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "missing"),
            1L));

    assertTrue(entity.getViaPointerForTest(ptrKey).await().indefinitely().isEmpty());
  }

  @Test
  void listByPartitionKeyIndex_returns_entities_for_pointers() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    kv.records.put(
        key("ptr", "p1"),
        record(
            "ptr",
            "p1",
            "Pointer",
            new byte[0],
            Map.of(KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "t1"),
            1L));
    kv.records.put(
        key("ptr", "p2"),
        record(
            "ptr",
            "p2",
            "Pointer",
            new byte[0],
            Map.of(KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "t2"),
            1L));

    kv.records.put(
        key("tp", "t1"), record("tp", "t1", TestEntity.KIND, pointerBytes("t1"), Map.of(), 1L));
    kv.records.put(
        key("tp", "t2"), record("tp", "t2", TestEntity.KIND, pointerBytes("t2"), Map.of(), 1L));

    var page =
        entity
            .listByPartitionKeyIndexForTest("ptr", "p", 10, Optional.empty())
            .await()
            .indefinitely();
    assertEquals(2, page.items().size());
    Set<String> keys = new TreeSet<>();
    page.items().forEach(p -> keys.add(p.getKey()));
    assertEquals(Set.of("t1", "t2"), keys);
  }

  @Test
  void listByPartitionKeyIndex_handles_empty_pointer_page() {
    TestKvStore kv = new TestKvStore();
    kv.setCannedPages(List.of(new KvStore.Page(List.of(), Optional.of("next"))));
    TestEntity entity = new TestEntity(kv);

    var page =
        entity
            .listByPartitionKeyIndexForTest("ptr", "p", 10, Optional.empty())
            .await()
            .indefinitely();
    assertTrue(page.items().isEmpty());
    assertEquals(Optional.of("next"), page.nextToken());
  }

  @Test
  void listByPartitionKeyIndex_skips_missing_targets() {
    TestKvStore kv = new TestKvStore();
    TestEntity entity = new TestEntity(kv);

    kv.records.put(
        key("ptr", "p1"),
        record(
            "ptr",
            "p1",
            "Pointer",
            new byte[0],
            Map.of(KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "t1"),
            1L));
    kv.records.put(
        key("ptr", "p2"),
        record(
            "ptr",
            "p2",
            "Pointer",
            new byte[0],
            Map.of(
                KvAttributes.TARGET_PARTITION_KEY, "tp", KvAttributes.TARGET_SORT_KEY, "missing"),
            1L));
    kv.records.put(
        key("tp", "t1"), record("tp", "t1", TestEntity.KIND, pointerBytes("t1"), Map.of(), 1L));

    var page =
        entity
            .listByPartitionKeyIndexForTest("ptr", "p", 10, Optional.empty())
            .await()
            .indefinitely();
    assertEquals(1, page.items().size());
    assertEquals("t1", page.items().get(0).getKey());
  }

  private static KvStore.Key key(String pk, String sk) {
    return new KvStore.Key(pk, sk);
  }

  private static KvStore.Record record(
      String pk, String sk, String kind, byte[] value, Map<String, String> attrs, long version) {
    return new KvStore.Record(new KvStore.Key(pk, sk), kind, value, attrs, version);
  }

  private static byte[] pointerBytes(String key) {
    return Pointer.newBuilder().setKey(key).build().toByteArray();
  }

  private static final class TestEntity extends AbstractEntity<Pointer> {
    static final String KIND = "TestKind";

    private TestEntity(KvStore kv) {
      super(kv, KIND, Pointer.getDefaultInstance(), (p, v) -> p.toBuilder().setVersion(v).build());
    }

    private Uni<Boolean> putCanonicalCasForTest(
        KvStore.Key key, Pointer pointer, Map<String, String> attrs, long expectedVersion) {
      return putCanonicalCas(key, KIND, pointer, attrs, expectedVersion);
    }

    private Uni<Boolean> deleteCasForTest(KvStore.Key key, long expectedVersion) {
      return deleteCas(key, expectedVersion);
    }

    private Uni<Optional<Pointer>> getViaPointerForTest(KvStore.Key key) {
      return getViaPointer(key);
    }

    private Uni<EntityPage<Pointer>> listByPartitionKeyIndexForTest(
        String pk, String skPrefix, int limit, Optional<String> token) {
      return listByPartitionKeyIndex(pk, skPrefix, limit, token);
    }

    private static long nextVersionForTest(long expectedVersion) {
      return nextVersion(expectedVersion);
    }

    @Override
    protected Pointer setExpiresAt(Pointer pointer, long timestamp) {
      return pointer.toBuilder().setExpiresAt(Timestamps.fromMillis(timestamp * 1000L)).build();
    }

    @Override
    protected long getExpiresAt(Pointer pointer) {
      if (pointer.hasExpiresAt()) {
        return Timestamps.toMillis(pointer.getExpiresAt()) / 1000L;
      }
      return 0L;
    }
  }

  private static class TestKvStore implements KvStore {
    final Map<Key, Record> records = new HashMap<>();
    List<Page> cannedPages;
    int cannedIndex;

    private void setCannedPages(List<Page> pages) {
      this.cannedPages = pages;
      this.cannedIndex = 0;
    }

    @Override
    public Uni<Optional<Record>> get(Key key) {
      return Uni.createFrom().item(Optional.ofNullable(records.get(key)));
    }

    @Override
    public Uni<Boolean> putCas(Record record, long expectedVersion) {
      Record existing = records.get(record.key());
      if (expectedVersion == 0L) {
        if (existing != null) {
          return Uni.createFrom().item(false);
        }
        records.put(record.key(), record);
        return Uni.createFrom().item(true);
      }
      if (existing != null && existing.version() == expectedVersion) {
        records.put(record.key(), record);
        return Uni.createFrom().item(true);
      }
      return Uni.createFrom().item(false);
    }

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      Record existing = records.get(key);
      if (existing != null && existing.version() == expectedVersion) {
        records.remove(key);
        return Uni.createFrom().item(true);
      }
      return Uni.createFrom().item(false);
    }

    @Override
    public Uni<Page> queryByPartitionKeyPrefix(
        String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
      if (cannedPages != null) {
        if (cannedIndex >= cannedPages.size()) {
          return Uni.createFrom().item(new Page(List.of(), Optional.empty()));
        }
        return Uni.createFrom().item(cannedPages.get(cannedIndex++));
      }

      List<Record> filtered =
          records.values().stream()
              .filter(
                  r ->
                      Objects.equals(r.key().partitionKey(), partitionKey)
                          && (sortKeyPrefix == null
                              || sortKeyPrefix.isEmpty()
                              || r.key().sortKey().startsWith(sortKeyPrefix)))
              .sorted(Comparator.comparing(r -> r.key().sortKey()))
              .toList();

      int start = 0;
      if (pageToken != null && pageToken.isPresent() && !pageToken.get().isEmpty()) {
        try {
          start = Integer.parseInt(pageToken.get());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("bad page token");
        }
      }
      if (start < 0 || start > filtered.size()) {
        throw new IllegalArgumentException("bad page token");
      }

      int end = Math.min(filtered.size(), start + limit);
      List<Record> items = new ArrayList<>(filtered.subList(start, end));
      Optional<String> nextToken =
          end < filtered.size() ? Optional.of(Integer.toString(end)) : Optional.empty();
      return Uni.createFrom().item(new Page(items, nextToken));
    }

    @Override
    public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
      int before = records.size();
      records
          .entrySet()
          .removeIf(
              e ->
                  Objects.equals(e.getKey().partitionKey(), partitionKey)
                      && (sortKeyPrefix == null
                          || sortKeyPrefix.isEmpty()
                          || e.getKey().sortKey().startsWith(sortKeyPrefix)));
      return Uni.createFrom().item(before - records.size());
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
      if (ops == null || ops.isEmpty()) {
        return Uni.createFrom().item(true);
      }

      for (TxnOp op : ops) {
        if (op instanceof TxnPut put) {
          Record existing = records.get(put.record().key());
          if (put.expectedVersion() == 0L) {
            if (existing != null) return Uni.createFrom().item(false);
          } else {
            if (existing == null || existing.version() != put.expectedVersion()) {
              return Uni.createFrom().item(false);
            }
          }
        } else if (op instanceof TxnDelete del) {
          Record existing = records.get(del.key());
          if (existing == null || existing.version() != del.expectedVersion()) {
            return Uni.createFrom().item(false);
          }
        }
      }

      for (TxnOp op : ops) {
        if (op instanceof TxnPut put) {
          records.put(put.record().key(), put.record());
        } else if (op instanceof TxnDelete del) {
          records.remove(del.key());
        }
      }

      return Uni.createFrom().item(true);
    }
  }

  private static final class RecordingKvStore extends TestKvStore {
    private Key lastDeleteKey;
    private long lastDeleteExpectedVersion;

    @Override
    public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
      this.lastDeleteKey = key;
      this.lastDeleteExpectedVersion = expectedVersion;
      return super.deleteCas(key, expectedVersion);
    }
  }
}
