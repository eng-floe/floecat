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

/** Shared Caffeine storage, expiry and telemetry mechanics for expiring cache policies. */
final class CaffeineExpiringCacheSupport<K, V> {

  private final Cache<K, V> cache;
  private final Function<? super V, Duration> holdFor;
  private final CacheEvents events;

  private CaffeineExpiringCacheSupport(
      Cache<K, V> cache, Function<? super V, Duration> holdFor, CacheEvents events) {
    this.cache = cache;
    this.holdFor = holdFor;
    this.events = events;
  }

  static <K, V> CaffeineExpiringCacheSupport<K, V> create(
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
    return new CaffeineExpiringCacheSupport<>(cache, holdFor, events);
  }

  V getIfPresent(K key) {
    return cache.getIfPresent(Objects.requireNonNull(key, "key"));
  }

  void putIfRetained(K key, V value) {
    if (value != null && nanos(holdFor.apply(value)) > 0) {
      cache.put(Objects.requireNonNull(key, "key"), value);
    }
  }

  void invalidate(K key) {
    cache.invalidate(Objects.requireNonNull(key, "key"));
  }

  long estimatedSize() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  void invalidateAll() {
    cache.invalidateAll();
  }

  void hit(long startNanos) {
    report(() -> events.hit(since(startNanos)));
  }

  void miss() {
    report(events::miss);
  }

  void loaded(long startNanos) {
    miss();
    report(() -> events.loadTime(since(startNanos)));
  }

  void loadFailed(long startNanos, RuntimeException failure) {
    miss();
    report(() -> events.loadFailed(since(startNanos), failure));
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
