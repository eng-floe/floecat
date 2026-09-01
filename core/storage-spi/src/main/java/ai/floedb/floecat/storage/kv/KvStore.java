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
import java.util.LinkedHashMap;
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
        return Keys.SEP + sortKey;
      } else if (sortKey == null || sortKey.isEmpty() || sortKey.equals(Keys.SEP)) {
        return Keys.SEP + partitionKey;
      }
      return Keys.SEP + partitionKey + Keys.SEP + sortKey;
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
   *   <li>{@code attrs} are small typed attributes for pointers/indexes/metadata (see {@link
   *       AttrValue})
   *   <li>{@code version} is the monotonically increasing optimistic-concurrency version
   * </ul>
   */
  record Record(Key key, String kind, byte[] value, Map<String, AttrValue> attrs, long version) {
    public Record {
      attrs = (attrs == null) ? Map.of() : Map.copyOf(attrs);
      for (var name : attrs.keySet()) {
        if (KvAttributes.RESERVED_ATTRS.contains(name)) {
          throw new IllegalArgumentException("attr name is reserved by the backend: " + name);
        }
      }
      value = (value == null) ? new byte[0] : value;
      if (version < 0) throw new IllegalArgumentException("version must be >= 0");
    }
  }

  record Page(List<Record> items, Optional<String> nextToken) {}

  // Reads
  Uni<Optional<Record>> get(Key key);

  /** Batch read with a default implementation for embedded and specialized stores. */
  default Uni<Map<Key, Record>> getBatch(List<Key> keys) {
    List<Key> stable = keys == null ? List.of() : List.copyOf(keys);
    if (stable.isEmpty()) {
      return Uni.createFrom().item(Map.of());
    }
    List<Uni<Optional<Record>>> reads = stable.stream().map(this::get).toList();
    return Uni.combine()
        .all()
        .unis(reads)
        .with(
            values -> {
              Map<Key, Record> out = new LinkedHashMap<>();
              for (int index = 0; index < stable.size(); index++) {
                @SuppressWarnings("unchecked")
                Optional<Record> value = (Optional<Record>) values.get(index);
                if (value.isPresent()) {
                  out.put(stable.get(index), value.get());
                }
              }
              return Map.copyOf(out);
            });
  }

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
   * @throws IllegalArgumentException if the record's attrs break {@link AttrWriteRules}
   */
  Uni<Boolean> putCas(Record record, long expectedVersion);

  /**
   * Conditional delete.
   *
   * @return true if deleted; false if the condition failed
   */
  Uni<Boolean> deleteCas(Key key, long expectedVersion);

  /**
   * Atomically updates metadata attributes on an <em>existing</em> record and advances its version,
   * in a single request — the one write here that is not a whole-record CAS, so concurrent bumps
   * cannot lose each other.
   *
   * <ul>
   *   <li>{@code sets} replaces attribute values.
   *   <li>{@code increments} adds a delta to a numeric attribute, creating it at the delta if
   *       absent. Incrementing a string-valued attribute, or past the range of {@code long}, fails
   *       the {@link Uni} with a store-specific error; neither overwrites nor wraps.
   *   <li>The version advances by one; a missing stored version counts as 0.
   *   <li>The record is never created as a side effect.
   *   <li>A record carrying a {@code value} payload is refused, since the payload embeds its own
   *       copy of the version and this update does not rewrite it.
   * </ul>
   *
   * @param sets attribute values to replace; must not name a {@link KvAttributes#RESERVED_ATTRS}
   *     attribute, nor {@link KvAttributes#ATTR_EXPIRES_AT}, which only a whole-record write may
   *     touch
   * @param increments deltas to add to numeric attributes; must not overlap {@code sets}
   * @return the new version, or empty if the record was absent or refused for carrying a value. A
   *     failed increment arrives as a failed {@link Uni}, never as empty.
   * @throws IllegalArgumentException if both maps are empty, or an attribute name is reserved, the
   *     expiry stamp, blank, or present in both maps
   */
  Uni<Optional<Long>> updateMetadataAttrsIfExists(
      Key key, Map<String, AttrValue> sets, Map<String, Long> increments);

  /**
   * Query within a partition key, ordered by sk, with a prefix constraint. This is the
   * "hierarchical keyspace" operation.
   */
  Uni<Page> queryByPartitionKeyPrefix(
      String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken);

  Uni<Page> queryByPartitionKeyPrefix(
      String partitionKey,
      String sortKeyPrefix,
      int limit,
      Optional<String> pageToken,
      boolean consistentRead);

  /**
   * Returns a page token that resumes a {@link #queryByPartitionKeyPrefix} scan immediately after
   * the given key, in this store's native token encoding. The default throws; stores that serve
   * paging must override.
   */
  default String pageTokenAfterKey(Key key) {
    throw new UnsupportedOperationException("pageTokenAfterKey is not supported by this store");
  }

  /**
   * Remove items
   *
   * @param partitionKey
   * @param sortKeyPrefix
   * @return count of items removed
   */
  Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix);

  /** Deletes a prefix while continuously preserving one exact sort key. */
  Uni<Integer> deleteByPrefixExcluding(
      String partitionKey, String sortKeyPrefix, String excludedSortKey);

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
   * @throws ai.floedb.floecat.storage.errors.StorageTransactionConflictException if the backend
   *     definitively cancelled the transaction without applying any mutation
   */
  Uni<Boolean> txnWriteCas(List<TxnOp> ops);

  sealed interface TxnOp permits TxnPut, TxnPutUnconditional, TxnDelete, TxnCheck, TxnCheckAbsent {}

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

  /** Unconditional put in a transaction. */
  record TxnPutUnconditional(Record record) implements TxnOp {}

  /** CAS delete in a transaction (expectedVersion must be > 0). */
  record TxnDelete(Key key, long expectedVersion) implements TxnOp {
    public TxnDelete {
      if (expectedVersion <= 0) {
        throw new IllegalArgumentException("expectedVersion must be > 0 for delete");
      }
    }
  }

  /** Condition-only version check in a transaction (expectedVersion must be > 0). */
  record TxnCheck(Key key, long expectedVersion) implements TxnOp {
    public TxnCheck {
      if (expectedVersion <= 0) {
        throw new IllegalArgumentException("expectedVersion must be > 0 for check");
      }
    }
  }

  /** Condition-only absence check in a transaction. */
  record TxnCheckAbsent(Key key) implements TxnOp {}
}
