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
 * The contract an in-memory cache family implements, so heap-resident families are bounded the same
 * way and, within a pool, report comparable numbers. {@link CacheFamily} names the families.
 *
 * <p>Values must be immutable once cached: they are weighed on entry and may be weighed again on
 * exit. Nothing expires, so an entry a write fails to reach is wrong until something drops it.
 *
 * <p><b>Atomicity against an in-flight load.</b> A value loaded concurrently with {@link #put},
 * {@link #evict}, or {@link #evictPartition} must not overwrite that mutation. The mutator's result
 * wins when it installs a value; otherwise the key is left absent.
 *
 * @param <K> key
 * @param <V> value
 */
public interface MemoryCache<K, V> {

  /**
   * The value for {@code key}, loading it if absent. Single-flight per key. Returns {@code null}
   * and caches nothing when the loader does: absence is never cached, and this is the only null the
   * contract accepts.
   *
   * <p>The caller always receives what the loader returned. A write that raced the load drops the
   * cached entry, not the answer.
   */
  V get(K key, Loader<K, V> loader);

  /**
   * The values for the distinct {@code keys}, loading all misses in one call. Existing values and
   * loaded values are returned; keys omitted by the loader are absent and are not cached. The
   * loader is not called when every key is already held or {@code keys} is empty. If another load
   * fills a miss first, that retained value wins and is returned.
   *
   * <p>Hit and miss events are reported per distinct key. Load duration and failure are reported
   * once for the bulk loader invocation.
   */
  Map<K, V> getAll(Collection<K> keys, BulkLoader<K, V> loader);

  /**
   * The value for {@code key} if held, without loading. Reports no hit or miss. Not otherwise
   * inert: the read still counts towards the implementation's eviction policy.
   */
  Optional<V> peek(K key);

  /** Insert or replace {@code key}, including when it is cold. */
  void put(K key, V value);

  /** Drop {@code key}. */
  void evict(K key);

  /**
   * Drop every key belonging to a partition. The caller supplies the membership test because this
   * generic cache does not own key layout. The in-memory implementation scans its resident keys;
   * this is intended for infrequent account/relation lifecycle events, not the request path.
   */
  void evictPartition(Predicate<K> belongsToPartition);

  /**
   * Retained-heap bytes held for the budget. Comparable between families sharing a memory pool.
   * Implementations count them with {@link CacheWeights}.
   *
   * <p>May force maintenance and so take the implementation's eviction lock: metrics scrape, not
   * request path.
   */
  long bytes();

  /** Which cache this is. The tag is the metric dimension and the budget key. */
  CacheFamily family();

  /**
   * Entries held, approximately -- it forces no maintenance, so entries selected for eviction may
   * still count. Read with {@link #bytes()} to tell a few large values from many small ones.
   */
  long entryCount();

  /** Loads a value on a miss. Named, rather than a {@link java.util.function.Function}. */
  @FunctionalInterface
  interface Loader<K, V> {
    V load(K key);
  }

  /** Loads the values present for a set of misses. Omit keys that are absent. */
  @FunctionalInterface
  interface BulkLoader<K, V> {
    Map<K, V> load(Set<K> keys);
  }
}
