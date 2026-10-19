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

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Caffeine-backed {@link ExpiringCache}. A miss loads on the calling thread, outside any lock, and
 * puts the value; callers that miss the same key together each load.
 */
public final class CaffeineExpiringCache<K, V> implements ExpiringCache<K, V> {

  private final CaffeineExpiringCacheSupport<K, V> support;

  private CaffeineExpiringCache(CaffeineExpiringCacheSupport<K, V> support) {
    this.support = support;
  }

  /**
   * A cache of at most {@code maximumSize} entries, each held for {@code holdFor(value)} from its
   * load, as measured by {@code ticker} in nanoseconds; a zero or negative hold holds none. Events
   * go to {@code events}, and a report that throws does not fail the read.
   */
  public static <K, V> ExpiringCache<K, V> create(
      long maximumSize,
      Function<? super V, Duration> holdFor,
      LongSupplier ticker,
      CacheEvents events) {
    return new CaffeineExpiringCache<>(
        CaffeineExpiringCacheSupport.create(maximumSize, holdFor, ticker, events));
  }

  @Override
  public V get(K key, Function<? super K, ? extends V> loader) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    long startNanos = System.nanoTime();
    V held = support.getIfPresent(key);
    if (held != null) {
      support.hit(startNanos);
      return held;
    }
    V value;
    try {
      value = loader.apply(key);
    } catch (RuntimeException failure) {
      support.loadFailed(startNanos, failure);
      throw failure;
    }
    support.putIfRetained(key, value);
    support.loaded(startNanos);
    return value;
  }
}
