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
 * Read-through cache for expiring answers whose same-key source loads must be coalesced.
 *
 * <p>This is separate from {@link ExpiringCache}: that contract deliberately lets concurrent misses
 * load independently so one slow request context never owns another caller's load. This contract
 * instead runs at most one load per key at a time and lets concurrent callers share its result.
 * Callers may explicitly invalidate mutable source answers after a write attempt.
 */
public interface CoalescingExpiringCache<K, V> extends AutoCloseable {

  /**
   * Returns the held value for {@code key}, or loads it on the calling thread. Concurrent callers
   * for the same key share its in-flight result. A load that throws or returns null is not held.
   */
  V get(K key, Function<? super K, ? extends V> loader);

  /** Removes the held value and prevents an in-flight load for {@code key} from being held. */
  void invalidate(K key);

  /** Approximate number of currently held entries, after pending maintenance. */
  long estimatedSize();

  /** Removes every held value. */
  void invalidateAll();

  @Override
  default void close() {
    invalidateAll();
  }
}
