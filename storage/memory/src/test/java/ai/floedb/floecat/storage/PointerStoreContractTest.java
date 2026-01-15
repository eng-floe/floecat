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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Pointer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PointerStoreContractTest {

  private PointerStore store;

  @BeforeEach
  void setUp() {
    store = new InMemoryPointerStore();
  }

  @Test
  void isEmpty_returns_true_on_fresh_store() {
    assertTrue(store.isEmpty());
  }

  @Test
  void isEmpty_returns_false_after_put() {
    assertTrue(store.compareAndSet(key(1), 0L, pointer(key(1), 1L)));
    assertFalse(store.isEmpty());
  }

  @Test
  void dump_is_noop_or_does_not_throw() {
    store.dump("PointerStoreContractTest.dump");
  }

  @Test
  void deleteByPrefix_deletes_all_matching_keys() {
    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    store.compareAndSet(key(2), 0L, pointer(key(2), 1L));
    store.compareAndSet(otherKey(1), 0L, pointer(otherKey(1), 1L));

    int deleted = store.deleteByPrefix(prefix());
    assertEquals(2, deleted);
    assertTrue(store.get(key(1)).isEmpty());
    assertTrue(store.get(key(2)).isEmpty());
    assertTrue(store.get(otherKey(1)).isPresent());
  }

  @Test
  void countByPrefix_matches_number_of_keys() {
    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    store.compareAndSet(key(2), 0L, pointer(key(2), 1L));
    store.compareAndSet(otherKey(1), 0L, pointer(otherKey(1), 1L));

    assertEquals(2, store.countByPrefix(prefix()));
    assertEquals(1, store.countByPrefix(otherPrefix()));
  }

  @Test
  void listPointersByPrefix_returns_nextToken_and_all_items_across_pages() {
    for (int i = 0; i < 5; i++) {
      store.compareAndSet(key(i), 0L, pointer(key(i), 1L));
    }

    StringBuilder firstNext = new StringBuilder();
    List<Pointer> firstPage = store.listPointersByPrefix(prefix(), 2, "", firstNext);
    assertEquals(2, firstPage.size());
    assertFalse(firstNext.toString().isEmpty());

    List<String> seen = new ArrayList<>();
    seen.addAll(keysOfList(firstPage));
    String token = firstNext.toString();
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> page = store.listPointersByPrefix(prefix(), 2, token, next);
      page.forEach(p -> seen.add(p.getKey()));
      token = next.toString();
    } while (!token.isEmpty());

    assertEquals(5, seen.size());
    for (int i = 0; i < 5; i++) {
      assertTrue(seen.contains(key(i)));
    }
  }

  @Test
  void listPointersByPrefix_handles_blank_or_null_pageToken() {
    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    store.compareAndSet(key(2), 0L, pointer(key(2), 1L));

    StringBuilder nextBlank = new StringBuilder();
    List<Pointer> pageBlank = store.listPointersByPrefix(prefix(), 10, "", nextBlank);

    StringBuilder nextNull = new StringBuilder();
    List<Pointer> pageNull = store.listPointersByPrefix(prefix(), 10, null, nextNull);

    assertEquals(pageBlank.size(), pageNull.size());
    assertEquals(keysOf(pageBlank), keysOf(pageNull));
  }

  @Test
  void deleteByPrefix_empty_or_root_deletes_all() {
    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    store.compareAndSet(key(2), 0L, pointer(key(2), 1L));

    int deleted = store.deleteByPrefix("");
    assertEquals(2, deleted);
    assertTrue(store.isEmpty());

    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    assertEquals(1, store.deleteByPrefix("/"));
    assertTrue(store.isEmpty());
  }

  @Test
  void compareAndDelete_expectedVersion_matches() {
    String key = key(1);
    store.compareAndSet(key, 0L, pointer(key, 1L));
    assertTrue(store.compareAndDelete(key, 1L));
    assertTrue(store.get(key).isEmpty());
  }

  @Test
  void listPointersByPrefix_pageToken_invalid_throws() {
    store.compareAndSet(key(1), 0L, pointer(key(1), 1L));
    StringBuilder next = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> store.listPointersByPrefix(prefix(), 10, "bad-token", next));
  }

  private static Pointer pointer(String key, long version) {
    return Pointer.newBuilder().setKey(key).setVersion(version).build();
  }

  private static String prefix() {
    return "accounts/1/p/";
  }

  private static String otherPrefix() {
    return "accounts/2/p/";
  }

  private static String key(int id) {
    return prefix() + id;
  }

  private static String otherKey(int id) {
    return otherPrefix() + id;
  }

  private static String keysOf(List<Pointer> pointers) {
    StringJoiner joiner = new StringJoiner(",");
    for (Pointer pointer : pointers) {
      joiner.add(pointer.getKey());
    }
    return joiner.toString();
  }

  private static List<String> keysOfList(List<Pointer> pointers) {
    List<String> out = new ArrayList<>(pointers.size());
    for (Pointer pointer : pointers) {
      out.add(pointer.getKey());
    }
    return out;
  }
}
