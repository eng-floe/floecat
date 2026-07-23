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

package ai.floedb.floecat.storage.memory;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class InMemoryPointerStore implements PointerStore {
  private final ConcurrentNavigableMap<String, Pointer> map = new ConcurrentSkipListMap<>();

  @Override
  public Optional<Pointer> get(String key) {
    return Optional.ofNullable(map.get(key));
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    final boolean[] updated = {false};
    map.compute(
        key,
        (k, cur) -> {
          if (cur == null) {
            if (expectedVersion == 0L) {
              updated[0] = true;
              return next.toBuilder().setKey(key).setVersion(1L).build();
            }
            return null;
          }
          if (cur.getVersion() == expectedVersion) {
            updated[0] = true;
            return next.toBuilder().setKey(key).setVersion(expectedVersion + 1L).build();
          }
          return cur;
        });

    return updated[0];
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    final String pfx = prefix == null ? "" : prefix;
    final int lim = Math.max(1, limit);
    String token = pageToken == null ? "" : pageToken;
    if (!token.isEmpty() && !pfx.isEmpty() && !token.startsWith(pfx)) {
      throw new IllegalArgumentException("bad page token");
    }
    NavigableMap<String, Pointer> tail =
        token.isEmpty() ? map.tailMap(pfx, true) : map.tailMap(token, false);
    // Callers use Integer.MAX_VALUE to request all matching pointers. Do not use the requested
    // limit as the initial capacity: ArrayList rejects that capacity before the scan begins.
    List<Pointer> page = new ArrayList<>();
    boolean hasMore = false;
    for (Map.Entry<String, Pointer> entry : tail.entrySet()) {
      if (!entry.getKey().startsWith(pfx)) {
        break;
      }
      if (page.size() == lim) {
        hasMore = true;
        break;
      }
      page.add(entry.getValue());
    }

    if (nextTokenOut != null) {
      nextTokenOut.setLength(0);
      if (hasMore) {
        nextTokenOut.append(page.getLast().getKey());
      }
    }
    return page;
  }

  @Override
  public String pageTokenAfterKey(String key) {
    // This store's page tokens are raw pointer keys ("resume after this key"), so the key itself
    // is the token. Resuming after a since-deleted key fails the same way an ordinary end-of-page
    // token for that key would.
    return key;
  }

  @Override
  public int countByPrefix(String prefix) {
    final String pfx = prefix == null ? "" : prefix;
    int n = 0;
    for (String key : map.tailMap(pfx, true).keySet()) {
      if (!key.startsWith(pfx)) {
        break;
      }
      n++;
    }
    return n;
  }

  @Override
  public boolean delete(String key) {
    return map.remove(key) != null;
  }

  @Override
  public int deleteByPrefix(String prefix) {
    final String pfx = (prefix == null) ? "" : prefix;
    if (pfx.isEmpty() || "/".equals(pfx)) {
      int n = map.size();
      map.clear();
      return n;
    }

    List<String> keys = new ArrayList<>();
    for (String key : map.tailMap(pfx, true).keySet()) {
      if (!key.startsWith(pfx)) {
        break;
      }
      keys.add(key);
    }
    keys.forEach(map::remove);
    return keys.size();
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    final boolean[] deleted = {false};
    map.compute(
        key,
        (k, cur) -> {
          if (cur == null) {
            return null;
          }

          if (cur.getVersion() == expectedVersion) {
            deleted[0] = true;
            return null;
          }

          return cur;
        });

    return deleted[0];
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return true;
    }
    synchronized (this) {
      for (CasOp op : ops) {
        if (op instanceof CasUpsert upsert) {
          Pointer cur = map.get(upsert.key());
          if (cur == null) {
            if (upsert.expectedVersion() != 0L) {
              return false;
            }
          } else if (cur.getVersion() != upsert.expectedVersion()) {
            return false;
          }
        } else if (op instanceof CasDelete delete) {
          Pointer cur = map.get(delete.key());
          if (cur == null || cur.getVersion() != delete.expectedVersion()) {
            return false;
          }
        } else if (op instanceof CasCheck check) {
          Pointer cur = map.get(check.key());
          if (cur == null || cur.getVersion() != check.expectedVersion()) {
            return false;
          }
        } else if (op instanceof CasCheckAbsent check) {
          if (map.get(check.key()) != null) {
            return false;
          }
        }
      }

      for (CasOp op : ops) {
        if (op instanceof CasUpsert upsert) {
          map.put(
              upsert.key(),
              upsert.next().toBuilder()
                  .setKey(upsert.key())
                  .setVersion(upsert.expectedVersion() + 1L)
                  .build());
        } else if (op instanceof CasDelete delete) {
          map.remove(delete.key());
        }
      }
      return true;
    }
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public void dump(String header) {
    for (Map.Entry<String, Pointer> entry : map.entrySet()) {
      System.out.println("Key: " + entry.getKey() + ", Pointer: " + entry.getValue());
    }
  }
}
