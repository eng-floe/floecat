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

import java.util.function.Function;

/**
 * Read-through cache for answers that expire, loaded from a source that may be slow.
 *
 * <p>Separate from {@link MemoryCache}, whose values are immutable and never expire, and from
 * {@link StateCache}, which holds mutable state and never loads. Each value is held for a duration
 * derived from the value itself, fixed when it is loaded.
 *
 * @param <K> key
 * @param <V> value
 */
public interface ExpiringCache<K, V> {

  /**
   * Returns the held value for {@code key}, or loads it on the calling thread. Callers that miss
   * the same key together each load; the last value put is held.
   *
   * <p>A load that throws or returns null is not held.
   */
  V get(K key, Function<? super K, ? extends V> loader);
}
