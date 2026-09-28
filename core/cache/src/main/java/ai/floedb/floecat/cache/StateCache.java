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

/**
 * A small concurrent cache for mutable process-local state.
 *
 * <p>This is deliberately separate from {@link MemoryCache}: state caches may replace values and
 * may be used for coordination or performance only. They must not be used as the source of truth
 * for durable catalogue state.
 */
public interface StateCache<K, V> extends AutoCloseable {

  V getIfPresent(K key);

  /** Returns the existing value, or {@code null} when {@code value} was installed. */
  V putIfAbsent(K key, V value);

  V remove(K key);

  /** Atomically computes an existing value; absent keys are left absent. */
  V computeIfPresent(K key, java.util.function.BiFunction<? super K, ? super V, ? extends V> fn);

  long estimatedSize();

  void invalidateAll();

  @Override
  default void close() {
    invalidateAll();
  }
}
