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

import ai.floedb.floecat.storage.kv.KvStore;
import io.smallrye.mutiny.Uni;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryKvStore implements KvStore {
  private final Map<Key, Record> records = new ConcurrentHashMap<>();

  @Override
  public Uni<Optional<Record>> get(Key key) {
    return Uni.createFrom().item(Optional.ofNullable(records.get(key)));
  }

  @Override
  public Uni<Boolean> putCas(Record record, long expectedVersion) {
    return Uni.createFrom()
        .item(
            records.compute(
                    record.key(),
                    (key, existing) -> {
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
                    (unused, existing) -> {
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
    String pk = partitionKey == null ? "" : partitionKey;
    String skPrefix = sortKeyPrefix == null ? "" : sortKeyPrefix;
    int pageSize = Math.max(1, limit);

    List<Record> matching =
        records.values().stream()
            .filter(record -> pk.equals(record.key().partitionKey()))
            .filter(record -> record.key().sortKey().startsWith(skPrefix))
            .sorted((left, right) -> left.key().sortKey().compareTo(right.key().sortKey()))
            .toList();

    int start = 0;
    if (pageToken.isPresent()) {
      String token = decodeToken(pageToken.get());
      int index = -1;
      for (int i = 0; i < matching.size(); i++) {
        if (matching.get(i).key().sortKey().equals(token)) {
          index = i;
          break;
        }
      }
      if (index < 0) {
        throw new IllegalArgumentException("Bad page token");
      }
      start = index + 1;
    }

    if (start >= matching.size()) {
      return Uni.createFrom().item(new Page(List.of(), Optional.empty()));
    }

    int end = Math.min(matching.size(), start + pageSize);
    List<Record> items = new ArrayList<>(matching.subList(start, end));
    Optional<String> nextToken =
        end < matching.size()
            ? Optional.of(encodeToken(items.get(items.size() - 1).key().sortKey()))
            : Optional.empty();
    return Uni.createFrom().item(new Page(List.copyOf(items), nextToken));
  }

  @Override
  public String pageTokenAfterKey(Key key) {
    // Same encoding as end-of-page tokens: base64 of the sort key to resume after.
    return encodeToken(key.sortKey());
  }

  @Override
  public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
    String pk = partitionKey == null ? "" : partitionKey;
    String skPrefix = sortKeyPrefix == null ? "" : sortKeyPrefix;
    int[] deleted = {0};
    records
        .keySet()
        .removeIf(
            key -> {
              boolean match = pk.equals(key.partitionKey()) && key.sortKey().startsWith(skPrefix);
              if (match) {
                deleted[0]++;
              }
              return match;
            });
    return Uni.createFrom().item(deleted[0]);
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
    for (Map.Entry<Key, Record> entry : records.entrySet()) {
      System.out.println(header + " " + entry.getKey() + " => " + entry.getValue());
    }
    return Uni.createFrom().voidItem();
  }

  @Override
  public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return Uni.createFrom().item(true);
    }
    synchronized (this) {
      for (TxnOp op : ops) {
        if (op instanceof TxnPut put) {
          Record existing = records.get(put.record().key());
          if (put.expectedVersion() == 0L) {
            if (existing != null) {
              return Uni.createFrom().item(false);
            }
          } else if (existing == null || existing.version() != put.expectedVersion()) {
            return Uni.createFrom().item(false);
          }
        } else if (op instanceof TxnDelete delete) {
          Record existing = records.get(delete.key());
          if (existing == null || existing.version() != delete.expectedVersion()) {
            return Uni.createFrom().item(false);
          }
        } else if (op instanceof TxnCheck check) {
          Record existing = records.get(check.key());
          if (existing == null || existing.version() != check.expectedVersion()) {
            return Uni.createFrom().item(false);
          }
        } else if (op instanceof TxnCheckAbsent check) {
          if (records.get(check.key()) != null) {
            return Uni.createFrom().item(false);
          }
        }
      }

      for (TxnOp op : ops) {
        if (op instanceof TxnPut put) {
          records.put(put.record().key(), put.record());
        } else if (op instanceof TxnDelete delete) {
          records.remove(delete.key());
        }
      }
      return Uni.createFrom().item(true);
    }
  }

  private static String encodeToken(String sortKey) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(sortKey.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeToken(String token) {
    return new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
  }
}
