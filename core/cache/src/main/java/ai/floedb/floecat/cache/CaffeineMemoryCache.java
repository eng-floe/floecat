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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/** Caffeine-backed implementation of the immutable {@link MemoryCache} contract. */
public final class CaffeineMemoryCache<K, V> implements MemoryCache<K, V> {

  private static final long CACHED_VALUE_BYTES = 16L;

  private final CacheFamily family;
  private final com.github.benmanes.caffeine.cache.Cache<K, CachedValue<V>> entries;
  private final ToLongFunction<K> keyWeight;
  private final CacheEvents events;
  private final long weightUnitBytes;

  public CaffeineMemoryCache(
      CacheFamily family, long maxBytes, ToLongFunction<K> keyWeight, CacheEvents events) {
    if (maxBytes <= 0) {
      throw new IllegalArgumentException(
          "cache " + family.tag() + " needs a positive budget, but got " + maxBytes + " bytes");
    }
    this.family = Objects.requireNonNull(family, "family");
    this.keyWeight = Objects.requireNonNull(keyWeight, "keyWeight");
    this.events = Objects.requireNonNull(events, "events");
    this.weightUnitBytes = divideRoundUp(maxBytes, Integer.MAX_VALUE - 1L);
    long maximumWeightUnits = maxBytes / weightUnitBytes;
    this.entries =
        Caffeine.<K, CachedValue<V>>newBuilder()
            .maximumWeight(maximumWeightUnits)
            .weigher((K key, CachedValue<V> value) -> weightUnits(key, value.value()))
            .evictionListener(
                (K key, CachedValue<V> value, RemovalCause cause) ->
                    report(() -> events.evicted(weightBytes(key, value.value()))))
            .build();
  }

  /** Changes the admission share reserved for this cache family. */
  public void maximumBytes(long maxBytes) {
    if (maxBytes < 0L) {
      throw new IllegalArgumentException("cache maximum must be >= 0 bytes");
    }
    entries
        .policy()
        .eviction()
        .orElseThrow(() -> new IllegalStateException("weighted cache has no eviction policy"))
        .setMaximum(maxBytes / weightUnitBytes);
    entries.cleanUp();
  }

  private int weightUnits(K key, V value) {
    long units = divideRoundUp(weightBytes(key, value), weightUnitBytes);
    return units > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) units;
  }

  private long weightBytes(K key, V value) {
    long bytes = CacheWeights.entry(value, keyWeight.applyAsLong(key));
    return bytes > Long.MAX_VALUE - CACHED_VALUE_BYTES
        ? Long.MAX_VALUE
        : bytes + CACHED_VALUE_BYTES;
  }

  private static long divideRoundUp(long dividend, long divisor) {
    return 1L + (dividend - 1L) / divisor;
  }

  @Override
  public V get(K key, Loader<K, V> loader) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    long startNanos = System.nanoTime();
    CachedValue<V> resident = entries.getIfPresent(key);
    if (resident != null) {
      report(() -> events.hit(Duration.ofNanos(System.nanoTime() - startNanos)));
      return resident.value();
    }
    java.util.concurrent.atomic.AtomicBoolean invoked =
        new java.util.concurrent.atomic.AtomicBoolean();
    CachedValue<V> value =
        entries.get(
            key,
            ignored -> {
              invoked.set(true);
              try {
                V result = loader.load(key);
                report(events::miss);
                report(() -> events.loadTime(Duration.ofNanos(System.nanoTime() - startNanos)));
                return result == null ? null : new CachedValue<>(result);
              } catch (RuntimeException failure) {
                report(
                    () ->
                        events.loadFailed(
                            Duration.ofNanos(System.nanoTime() - startNanos), failure));
                throw failure;
              }
            });
    if (!invoked.get()) {
      report(() -> events.hit(Duration.ofNanos(System.nanoTime() - startNanos)));
    }
    return value == null ? null : value.value();
  }

  @Override
  public Optional<V> peek(K key) {
    Objects.requireNonNull(key, "key");
    return Optional.ofNullable(entries.getIfPresent(key)).map(CachedValue::value);
  }

  /**
   * Publishes a value for the legacy mutable pointer view. Immutable cache clients use {@link
   * #get(Object, Loader)} and never call this method.
   */
  public void put(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    entries.put(key, new CachedValue<>(value));
  }

  @Override
  public Map<K, V> getAll(Collection<K> keys, BulkLoader<K, V> loader) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(loader, "loader");
    Set<K> distinct = new LinkedHashSet<>(keys);
    if (distinct.isEmpty()) {
      return Map.of();
    }
    distinct.forEach(key -> Objects.requireNonNull(key, "cache keys must not be null"));
    Map<K, CachedValue<V>> resident = entries.getAllPresent(distinct);
    resident.keySet().forEach(ignored -> report(() -> events.hit(Duration.ZERO)));
    Set<K> missing = new LinkedHashSet<>(distinct);
    missing.removeAll(resident.keySet());
    Map<K, CachedValue<V>> values = new LinkedHashMap<>(resident);
    if (!missing.isEmpty()) {
      long startNanos = System.nanoTime();
      Map<K, CachedValue<V>> loaded;
      try {
        loaded =
            entries.getAll(
                missing,
                requested -> {
                  Map<K, V> result =
                      Objects.requireNonNull(loader.load(Set.copyOf(requested)), "loader result");
                  for (Map.Entry<K, V> entry : result.entrySet()) {
                    if (!missing.contains(entry.getKey())) {
                      throw new IllegalArgumentException(
                          "loader returned an unrequested key: " + entry.getKey());
                    }
                    Objects.requireNonNull(entry.getValue(), "a bulk loader must omit absent keys");
                  }
                  report(() -> events.loadTime(Duration.ofNanos(System.nanoTime() - startNanos)));
                  requested.forEach(ignored -> report(events::miss));
                  Map<K, CachedValue<V>> wrapped = new LinkedHashMap<>();
                  result.forEach((key, value) -> wrapped.put(key, new CachedValue<>(value)));
                  return wrapped;
                });
      } catch (RuntimeException failure) {
        report(() -> events.loadFailed(Duration.ofNanos(System.nanoTime() - startNanos), failure));
        throw failure;
      }
      values.putAll(loaded);
    }
    Map<K, V> result = new LinkedHashMap<>();
    values.forEach((key, value) -> result.put(key, value.value()));
    return Map.copyOf(result);
  }

  @Override
  public void evict(K key) {
    Objects.requireNonNull(key, "key");
    entries.invalidate(key);
  }

  @Override
  public void evictPartition(Predicate<K> belongsToPartition) {
    Objects.requireNonNull(belongsToPartition, "belongsToPartition");
    entries.asMap().keySet().removeIf(belongsToPartition);
  }

  @Override
  public long bytes() {
    entries.cleanUp();
    return entries.policy().eviction().orElseThrow().weightedSize().orElseThrow() * weightUnitBytes;
  }

  @Override
  public CacheFamily family() {
    return family;
  }

  @Override
  public long entryCount() {
    return entries.estimatedSize();
  }

  private record CachedValue<V>(V value) {}

  private static void report(Runnable callback) {
    try {
      callback.run();
    } catch (RuntimeException ignored) {
      // Telemetry must not change source-read correctness.
    }
  }
}
