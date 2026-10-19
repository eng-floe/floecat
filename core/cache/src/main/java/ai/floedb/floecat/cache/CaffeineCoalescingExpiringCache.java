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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Caffeine-backed {@link CoalescingExpiringCache}. */
public final class CaffeineCoalescingExpiringCache<K, V> implements CoalescingExpiringCache<K, V> {

  private final Cache<K, V> cache;
  private final CacheEvents events;

  private CaffeineCoalescingExpiringCache(Cache<K, V> cache, CacheEvents events) {
    this.cache = cache;
    this.events = events;
  }

  /**
   * A cache of at most {@code maximumSize} entries, each held for {@code holdFor(value)} from its
   * load, as measured by {@code ticker} in nanoseconds. A zero or negative hold retains nothing.
   */
  public static <K, V> CoalescingExpiringCache<K, V> create(
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
    return new CaffeineCoalescingExpiringCache<>(cache, events);
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

    AtomicBoolean invoked = new AtomicBoolean();
    V value =
        cache.get(
            key,
            ignored -> {
              invoked.set(true);
              try {
                V loaded = loader.apply(key);
                report(events::miss);
                report(() -> events.loadTime(since(startNanos)));
                return loaded;
              } catch (RuntimeException failure) {
                report(events::miss);
                report(() -> events.loadFailed(since(startNanos), failure));
                throw failure;
              }
            });
    if (!invoked.get()) {
      report(() -> events.hit(since(startNanos)));
    }
    return value;
  }

  @Override
  public void invalidate(K key) {
    cache.invalidate(Objects.requireNonNull(key, "key"));
  }

  @Override
  public long estimatedSize() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
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
