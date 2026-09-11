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

package ai.floedb.floecat.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Read-through memory storage for immutable, content-addressed values.
 *
 * <p>Durable state is committed first. The cache only remembers values already identified by an
 * immutable key; callers never publish a replacement value through this contract. A new durable
 * identity gets a new key and eviction is memory hygiene, not a versioning protocol.
 *
 * @param <K> immutable key
 * @param <V> immutable value
 */
public interface MemoryCache<K, V> {

  /** Returns a value, loading it when absent. Caffeine coordinates matching concurrent loads. */
  V get(K key, Loader<K, V> loader);

  /** Returns values for distinct keys; omitted loader results are absent and not cached. */
  Map<K, V> getAll(Collection<K> keys, BulkLoader<K, V> loader);

  /** Returns a resident value without loading or recording a hit. */
  Optional<V> peek(K key);

  /** Drops one key as a memory-hygiene operation. */
  void evict(K key);

  /** Drops resident keys belonging to a partition. */
  void evictPartition(Predicate<K> belongsToPartition);

  /** Retained-heap bytes held by this cache family. */
  long bytes();

  /** Cache family used as the metric and budget dimension. */
  CacheFamily family();

  /** Approximate resident entry count. */
  long entryCount();

  @FunctionalInterface
  interface Loader<K, V> {
    V load(K key);
  }

  @FunctionalInterface
  interface BulkLoader<K, V> {
    Map<K, V> load(Set<K> keys);
  }
}
