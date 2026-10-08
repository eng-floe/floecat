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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Caffeine-backed {@link ExpiringCache}. A miss loads on the calling thread, outside any lock, and
 * puts the value; callers that miss the same key together each load.
 */
public final class CaffeineExpiringCache<K, V> implements ExpiringCache<K, V> {

  private final Cache<K, V> cache;
  private final Function<? super V, Duration> holdFor;
  private final CacheEvents events;

  private CaffeineExpiringCache(
      Cache<K, V> cache, Function<? super V, Duration> holdFor, CacheEvents events) {
    this.cache = cache;
    this.holdFor = holdFor;
    this.events = events;
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
    if (maximumSize <= 0) {
      throw new IllegalArgumentException("maximumSize must be positive");
    }
    Objects.requireNonNull(holdFor, "holdFor");
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(events, "events");
    Cache<K, V> cache =
        Caffeine.newBuilder()
            .maximumSize(maximumSize)
            .expireAfter(new HoldFor<K, V>(holdFor))
            .ticker(ticker::getAsLong)
            .build();
    return new CaffeineExpiringCache<>(cache, holdFor, events);
  }

  @Override
  public V get(K key, Function<? super K, ? extends V> loader) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    long startNanos = System.nanoTime();
    V held = cache.getIfPresent(key);
    if (held != null) {
      report(() -> events.hit(since(startNanos)));
      return held;
    }
    V value;
    try {
      value = loader.apply(key);
    } catch (RuntimeException failure) {
      report(events::miss);
      report(() -> events.loadFailed(since(startNanos), failure));
      throw failure;
    }
    if (value != null && nanos(holdFor.apply(value)) > 0) {
      cache.put(key, value);
    }
    report(events::miss);
    report(() -> events.loadTime(since(startNanos)));
    return value;
  }

  private static Duration since(long startNanos) {
    return Duration.ofNanos(System.nanoTime() - startNanos);
  }

  private static void report(Runnable callback) {
    try {
      callback.run();
    } catch (RuntimeException ignored) {
      // Telemetry must not change what a read returns.
    }
  }

  /** {@code duration} in nanoseconds, zero when null or not positive and capped on overflow. */
  private static long nanos(Duration duration) {
    if (duration == null || duration.isNegative() || duration.isZero()) {
      return 0L;
    }
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  /** Expiry fixed from the value when it is put; reads do not extend it. */
  private record HoldFor<K, V>(Function<? super V, Duration> holdFor) implements Expiry<K, V> {
    @Override
    public long expireAfterCreate(K key, V value, long currentTime) {
      return nanos(holdFor.apply(value));
    }

    @Override
    public long expireAfterUpdate(K key, V value, long currentTime, long currentDuration) {
      return nanos(holdFor.apply(value));
    }

    @Override
    public long expireAfterRead(K key, V value, long currentTime, long currentDuration) {
      return currentDuration;
    }
  }
}
