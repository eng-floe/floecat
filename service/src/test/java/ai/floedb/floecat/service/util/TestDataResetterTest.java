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
package ai.floedb.floecat.service.util;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

public class TestDataResetterTest {

  @Test
  void wipeAll_removes_by_id_global_keys_and_account_prefixes() {
    FakePointerStore ptr = new FakePointerStore();
    ptr.putPointer("/accounts/by-id/1", 1L);
    ptr.putPointer("/accounts/by-id/1/catalog/a", 1L);
    ptr.putPointer("accounts/by-id/2/catalog/b", 1L);
    ptr.putPointer("/accounts/1/catalog/x", 1L);
    ptr.putPointer("/accounts/2/catalog/y", 1L);
    ptr.putPointer("/other/keep", 1L);

    TestDataResetter resetter = resetter(ptr, new FakeBlobStore());
    resetter.wipeAll();

    assertTrue(ptr.containsKey("/other/keep"));
    assertTrue(ptr.keysMatching("/accounts/by-id/").isEmpty());
    assertTrue(ptr.keysMatching("/accounts/1/").isEmpty());
    assertTrue(ptr.keysMatching("/accounts/2/").isEmpty());
  }

  @Test
  void listAccountIds_parses_keys_correctly_for_both_with_and_without_leading_slash()
      throws Exception {
    FakePointerStore ptr = new FakePointerStore();
    ptr.putPointer("/accounts/by-id/123/catalog/a", 1L);
    ptr.putPointer("accounts/by-id/456/catalog/b", 1L);

    TestDataResetter resetter = resetter(ptr, new FakeBlobStore());
    List<String> ids = invokeList(resettersMethod("listAccountIds"), resetter);
    assertEquals(Set.of("123", "456"), new TreeSet<>(ids));
  }

  @Test
  void listAccountNames_parses_keys_correctly() throws Exception {
    FakePointerStore ptr = new FakePointerStore();
    ptr.putPointer("/accounts/by-name/acme/catalog/a", 1L);
    ptr.putPointer("accounts/by-name/omega/catalog/b", 1L);

    TestDataResetter resetter = resetter(ptr, new FakeBlobStore());
    List<String> names = invokeList(resettersMethod("listAccountNames"), resetter);
    assertEquals(Set.of("acme", "omega"), new TreeSet<>(names));
  }

  @Test
  void wipeAll_results_in_empty_pointer_store() {
    FakePointerStore ptr = new FakePointerStore();
    ptr.putPointer("/accounts/by-id/1", 1L);
    ptr.putPointer("/accounts/1/catalog/x", 1L);
    ptr.putPointer("/accounts/by-name/acme", 1L);

    TestDataResetter resetter = resetter(ptr, new FakeBlobStore());
    resetter.wipeAll();
    assertTrue(ptr.isEmpty());
  }

  @Test
  void wipeAll_does_not_throw_on_empty_store() {
    TestDataResetter resetter = resetter(new FakePointerStore(), new FakeBlobStore());
    resetter.wipeAll();
  }

  private static TestDataResetter resetter(PointerStore ptr, BlobStore blobs) {
    TestDataResetter resetter = new TestDataResetter();
    resetter.ptr = ptr;
    resetter.blobs = blobs;
    return resetter;
  }

  private static Method resettersMethod(String name) throws Exception {
    Method method = TestDataResetter.class.getDeclaredMethod(name);
    method.setAccessible(true);
    return method;
  }

  @SuppressWarnings("unchecked")
  private static List<String> invokeList(Method method, TestDataResetter resetter)
      throws Exception {
    return (List<String>) method.invoke(resetter);
  }

  private static final class FakePointerStore implements PointerStore {
    private final Map<String, Pointer> map = new HashMap<>();

    private void putPointer(String key, long version) {
      map.put(key, Pointer.newBuilder().setKey(key).setVersion(version).build());
    }

    private Set<String> keysMatching(String prefix) {
      Set<String> out = new TreeSet<>();
      for (String key : map.keySet()) {
        if (matchesPrefix(key, prefix)) {
          out.add(key);
        }
      }
      return out;
    }

    private boolean containsKey(String key) {
      return map.containsKey(key);
    }

    @Override
    public Optional<Pointer> get(String key) {
      return Optional.ofNullable(map.get(key));
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      Pointer cur = map.get(key);
      if (cur == null) {
        if (expectedVersion == 0L) {
          map.put(key, next);
          return true;
        }
        return false;
      }
      if (cur.getVersion() == expectedVersion) {
        map.put(key, next);
        return true;
      }
      return false;
    }

    @Override
    public boolean delete(String key) {
      return map.remove(key) != null;
    }

    @Override
    public boolean compareAndDelete(String key, long expectedVersion) {
      Pointer cur = map.get(key);
      if (cur != null && cur.getVersion() == expectedVersion) {
        map.remove(key);
        return true;
      }
      return false;
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      final String pfx = prefix == null ? "" : prefix;
      List<String> keys = new ArrayList<>();
      for (String k : map.keySet()) {
        if (matchesPrefix(k, pfx)) {
          keys.add(k);
        }
      }
      Collections.sort(keys);

      int start = 0;
      if (pageToken != null && !pageToken.isBlank()) {
        int idx = Collections.binarySearch(keys, pageToken);
        if (idx < 0) {
          throw new IllegalArgumentException("bad page token");
        }
        start = idx + 1;
      }

      if (start >= keys.size()) {
        if (nextTokenOut != null) {
          nextTokenOut.setLength(0);
        }
        return List.of();
      }

      int end = Math.min(keys.size(), start + Math.max(1, limit));
      List<Pointer> out = new ArrayList<>(end - start);
      for (int i = start; i < end; i++) {
        out.add(map.get(keys.get(i)));
      }

      if (nextTokenOut != null) {
        nextTokenOut.setLength(0);
        if (end < keys.size()) {
          nextTokenOut.append(keys.get(end - 1));
        }
      }
      return out;
    }

    @Override
    public int deleteByPrefix(String prefix) {
      final String pfx = prefix == null ? "" : prefix;
      int before = map.size();
      map.keySet().removeIf(k -> matchesPrefix(k, pfx));
      return before - map.size();
    }

    @Override
    public int countByPrefix(String prefix) {
      final String pfx = prefix == null ? "" : prefix;
      int count = 0;
      for (String k : map.keySet()) {
        if (matchesPrefix(k, pfx)) {
          count++;
        }
      }
      return count;
    }

    @Override
    public boolean isEmpty() {
      return map.isEmpty();
    }

    private static boolean matchesPrefix(String key, String prefix) {
      if (key.startsWith(prefix)) {
        return true;
      }
      if (prefix.startsWith("/") && key.startsWith(prefix.substring(1))) {
        return true;
      }
      return false;
    }
  }

  private static final class FakeBlobStore implements BlobStore {
    @Override
    public byte[] get(String uri) {
      return null;
    }

    @Override
    public void put(String uri, byte[] bytes, String contentType) {}

    @Override
    public Optional<ai.floedb.floecat.common.rpc.BlobHeader> head(String uri) {
      return Optional.empty();
    }

    @Override
    public boolean delete(String uri) {
      return false;
    }

    @Override
    public void deletePrefix(String prefix) {}

    @Override
    public Page list(String prefix, int limit, String pageToken) {
      return new Page() {
        @Override
        public List<String> keys() {
          return List.of();
        }

        @Override
        public String nextToken() {
          return "";
        }
      };
    }
  }
}
