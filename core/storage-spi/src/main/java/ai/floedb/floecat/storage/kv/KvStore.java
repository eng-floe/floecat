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

import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal KV surface area for entity storage.
 *
 * <p>This interface is intentionally CAS-only for writes: all puts and deletes are conditional on
 * an expected version.
 */
public interface KvStore {

  record Key(String partitionKey, String sortKey) {

    @Override
    public String toString() {
      if (partitionKey == null || partitionKey.isEmpty() || partitionKey.equals(Keys.SEP)) {
        return Keys.join(sortKey);
      } else if (sortKey == null || sortKey.isEmpty() || sortKey.equals(Keys.SEP)) {
        return Keys.join(partitionKey);
      }
      return Keys.join(partitionKey, sortKey);
    }
  }

  /**
   * A single record in the KV store.
   *
   * <ul>
   *   <li>{@code key} is the primary key (pk/sk)
   *   <li>{@code kind} is a small discriminator (useful for debugging)
   *   <li>{@code value} is raw bytes (protobuf bytes for canonical entities; usually empty for
   *       pointer/index items)
   *   <li>{@code attrs} are small string attributes for pointers/indexes/metadata
   *   <li>{@code version} is the monotonically increasing optimistic-concurrency version
   * </ul>
   */
  record Record(Key key, String kind, byte[] value, Map<String, String> attrs, long version) {
    public Record {
      attrs = (attrs == null) ? Map.of() : Map.copyOf(attrs);
      value = (value == null) ? new byte[0] : value;
      if (version < 0) throw new IllegalArgumentException("version must be >= 0");
    }
  }

  record Page(List<Record> items, Optional<String> nextToken) {}

  // Reads
  Uni<Optional<Record>> get(Key key);

  /**
   * Conditional put.
   *
   * <ul>
   *   <li>{@code expectedVersion == 0} means "create if absent" (no existing item).
   *   <li>{@code expectedVersion > 0} means "update only if current ver matches".
   * </ul>
   *
   * <p>On success, the backend should store {@code record.version} as the new ver attribute.
   *
   * @return true if write succeeded; false if the condition failed
   */
  Uni<Boolean> putCas(Record record, long expectedVersion);

  /**
   * Conditional delete.
   *
   * @return true if deleted; false if the condition failed
   */
  Uni<Boolean> deleteCas(Key key, long expectedVersion);

  /**
   * Query within a partition key, ordered by sk, with a prefix constraint. This is the
   * "hierarchical keyspace" operation.
   */
  Uni<Page> queryByPartitionKeyPrefix(
      String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken);

  /**
   * Remove items
   *
   * @param partitionKey
   * @param sortKeyPrefix
   * @return count of items removed
   */
  Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix);

  /**
   * Remove all records in store. <br>
   * NB: for testing purposes only.
   */
  Uni<Void> reset();

  /**
   * Check if the store is empty.
   *
   * @return true if empty
   */
  Uni<Boolean> isEmpty();

  /**
   * Debug dump of all records in the store to stdout. <br>
   * NB: for testing purposes only.
   *
   * @param header Header string to print with dump
   */
  Uni<Void> dump(String header);

  /**
   * Transactionally perform CAS puts/deletes.
   *
   * @return true if committed; false if any condition failed
   */
  Uni<Boolean> txnWriteCas(List<TxnOp> ops);

  sealed interface TxnOp permits TxnPut, TxnDelete {}

  /**
   * CAS put in a transaction.
   *
   * <p>expectedVersion==0 => create-if-absent; expectedVersion>0 => update-if-version-matches.
   */
  record TxnPut(Record record, long expectedVersion) implements TxnOp {
    public TxnPut {
      if (expectedVersion < 0) throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
  }

  /** CAS delete in a transaction (expectedVersion must be > 0). */
  record TxnDelete(Key key, long expectedVersion) implements TxnOp {
    public TxnDelete {
      if (expectedVersion <= 0) {
        throw new IllegalArgumentException("expectedVersion must be > 0 for delete");
      }
    }
  }
}
