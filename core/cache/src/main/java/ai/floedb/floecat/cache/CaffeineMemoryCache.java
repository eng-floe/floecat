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

/**
 * The Caffeine {@link MemoryCache} implementation. A family is one independently budgeted instance.
 *
 * <p>Eviction is Caffeine's W-TinyLFU: admission by frequency, so a listing or a statistics sweep
 * does not flush the hot set as it would an LRU. No expiry -- a time bound buys staleness bounds
 * worth nothing once writes are published, and costs a stampede at every window boundary.
 *
 * @param <K> key
 * @param <V> value
 */
public final class CaffeineMemoryCache<K, V> implements MemoryCache<K, V> {

  // Every stored value has a distinct wrapper so a raced load can remove its own installation
  // without removing a writer's publication of the exact same value object.
  private static final long CACHED_VALUE_BYTES = 16L;

  private final CacheFamily family;
  private final com.github.benmanes.caffeine.cache.Cache<K, CachedValue<V>> entries;
  private final ToLongFunction<K> keyWeight;
  private final CacheEvents events;
  private final long weightUnitBytes;
  private final LoadCoordinator<K, V> loads;

  /**
   * @param family which cache this is; its tag is the metric dimension
   * @param maxBytes the budget, from the container-derived split
   * @param keyWeight the key's contribution to an entry's weight, in bytes
   * @param events where behaviour is reported; {@link CacheEvents#none()} to report nothing
   */
  public CaffeineMemoryCache(
      CacheFamily family, long maxBytes, ToLongFunction<K> keyWeight, CacheEvents events) {
    if (maxBytes <= 0) {
      // The last door a zero can arrive at, after CacheBudget.split. See CacheBudget#split for why
      // it is refused rather than treated as a very small cache.
      throw new IllegalArgumentException(
          "cache " + family.tag() + " needs a positive budget, but got " + maxBytes + " bytes");
    }
    this.family = family;
    this.keyWeight = keyWeight;
    this.events = events;
    this.loads = new LoadCoordinator<>(Object::hashCode);
    // Caffeine accepts a long total but an int per-entry weight. Use larger units when the budget
    // itself cannot be represented in bytes, rounding entries up and the budget down so the real
    // byte ceiling is never exceeded. Leave one int unit unused: if an entry is larger than the
    // whole budget, clamping its weight to Integer.MAX_VALUE must still put it over the ceiling.
    this.weightUnitBytes = divideRoundUp(maxBytes, Integer.MAX_VALUE - 1L);
    long maximumWeightUnits = maxBytes / weightUnitBytes;
    this.entries =
        Caffeine.<K, CachedValue<V>>newBuilder()
            .maximumWeight(maximumWeightUnits)
            .weigher((K key, CachedValue<V> value) -> weightUnits(key, value.value()))
            // An eviction listener, not a removal listener: Caffeine routes an explicit invalidate
            // to the latter, which this cache does not install, so everything here is size
            // pressure.
            .evictionListener(
                (K key, CachedValue<V> value, RemovalCause cause) ->
                    events.evicted(weightBytes(key, value.value())))
            .build();
  }

  /**
   * The entry's weight in the units this cache gives Caffeine's int-valued {@code Weigher}.
   *
   * <p>One arithmetic for the weigher and the eviction listener, so they cannot disagree. The
   * listener recomputes it -- Caffeine passes the key and value, never the stamped weight -- under
   * the entry monitor and the eviction lock. Sound because {@link MemoryCache} requires immutable
   * values; cheap because a {@link WeightedValue} reports a field and a protobuf its serialized
   * size. A container-shaped value pays its walk there, so give it a {@link WeightedValue}.
   */
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

  /**
   * Changes this cache's share of its family budget.
   *
   * <p>A specialized cache may reserve part of one family budget for state that cannot be evicted
   * without losing correctness. Shrinking this admission-controlled remainder keeps both stores
   * under one ceiling instead of pretending that two independently bounded caches share a budget.
   * Zero is valid here: it disables admission after construction while the reserved state owns the
   * whole budget.
   */
  public void maximumBytes(long maxBytes) {
    if (maxBytes < 0L) {
      throw new IllegalArgumentException("cache maximum must be >= 0 bytes");
    }
    long maximumWeightUnits = maxBytes / weightUnitBytes;
    entries
        .policy()
        .eviction()
        .orElseThrow(() -> new IllegalStateException("weighted cache has no eviction policy"))
        .setMaximum(maximumWeightUnits);
    entries.cleanUp();
  }

  @Override
  public V get(K key, Loader<K, V> loader) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    long startNanos = System.nanoTime();
    LoadCoordinator.Sample sample = loads.sample(key);
    CachedValue<V> cached = entries.getIfPresent(key);
    if (cached != null) {
      events.hit(Duration.ofNanos(System.nanoTime() - startNanos));
      return cached.value();
    }

    LoadCoordinator.Acquisition<K, V> acquisition = loads.acquire(key, sample);
    if (!acquisition.owner()) {
      V value = loads.await(acquisition.token());
      events.hit(Duration.ofNanos(System.nanoTime() - startNanos));
      return value;
    }

    LoadCoordinator.Token<K, V> token = acquisition.token();
    V value;
    try {
      value = loader.load(key);
      CachedValue<V> loaded = value == null ? null : new CachedValue<>(value);
      if (loaded != null) {
        boolean retained =
            loads.publishIfCurrent(token, () -> entries.asMap().putIfAbsent(key, loaded));
        if (!retained) {
          events.loadDiscarded();
        }
      }
      // A mutation that won while the source read was in flight is the answer for followers and for
      // this call when it is resident. An eviction deliberately leaves no answer, so the caller
      // keeps the value it loaded even though it was not retained.
      CachedValue<V> current = entries.getIfPresent(key);
      V served = current == null ? value : current.value();
      loads.complete(token, served);
      events.miss();
      events.loadTime(Duration.ofNanos(System.nanoTime() - startNanos));
      return served;
    } catch (RuntimeException e) {
      loads.fail(token, e);
      events.miss();
      if (!(e instanceof UnweighableValueException)) {
        events.loadFailed(Duration.ofNanos(System.nanoTime() - startNanos), e);
      }
      throw e;
    } catch (Error e) {
      loads.fail(token, e);
      throw e;
    }
  }

  @Override
  public Optional<V> peek(K key) {
    return Optional.ofNullable(entries.getIfPresent(key)).map(CachedValue::value);
  }

  @Override
  public Map<K, V> getAll(Collection<K> keys, BulkLoader<K, V> loader) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(loader, "loader");

    Set<K> distinctKeys = new LinkedHashSet<>(keys);
    if (distinctKeys.isEmpty()) {
      return Map.of();
    }

    Map<K, V> result = new LinkedHashMap<>();
    Map<K, LoadCoordinator.Acquisition<K, V>> acquisitions = new LinkedHashMap<>();
    try {
      for (K key : distinctKeys) {
        Objects.requireNonNull(key, "cache keys must not be null");
        long startNanos = System.nanoTime();
        LoadCoordinator.Sample sample = loads.sample(key);
        CachedValue<V> value = entries.getIfPresent(key);
        if (value != null) {
          result.put(key, value.value());
          events.hit(Duration.ofNanos(System.nanoTime() - startNanos));
        } else {
          acquisitions.put(key, loads.acquire(key, sample));
          events.miss();
        }
      }
    } catch (RuntimeException | Error failure) {
      acquisitions.values().stream()
          .filter(LoadCoordinator.Acquisition::owner)
          .map(LoadCoordinator.Acquisition::token)
          .forEach(token -> loads.fail(token, failure));
      throw failure;
    }
    if (acquisitions.isEmpty()) {
      return Map.copyOf(result);
    }

    Map<K, LoadCoordinator.Token<K, V>> owned = new LinkedHashMap<>();
    for (Map.Entry<K, LoadCoordinator.Acquisition<K, V>> entry : acquisitions.entrySet()) {
      if (entry.getValue().owner()) {
        owned.put(entry.getKey(), entry.getValue().token());
      }
    }

    Map<K, V> ownedResults = new LinkedHashMap<>();
    if (!owned.isEmpty()) {
      long startNanos = System.nanoTime();
      try {
        Map<K, V> loaded =
            Objects.requireNonNull(loader.load(Set.copyOf(owned.keySet())), "loader result");
        for (Map.Entry<K, V> entry : loaded.entrySet()) {
          if (!owned.containsKey(entry.getKey())) {
            throw new IllegalArgumentException(
                "loader returned an unrequested key: " + entry.getKey());
          }
          Objects.requireNonNull(entry.getValue(), "a bulk loader must omit absent keys");
        }
        for (Map.Entry<K, LoadCoordinator.Token<K, V>> entry : owned.entrySet()) {
          K key = entry.getKey();
          V value = loaded.get(key);
          ownedResults.put(key, completeLoaded(key, value, entry.getValue()));
        }
      } catch (RuntimeException e) {
        owned.values().forEach(token -> loads.fail(token, e));
        if (!(e instanceof UnweighableValueException)) {
          events.loadFailed(Duration.ofNanos(System.nanoTime() - startNanos), e);
        }
        throw e;
      } catch (Error e) {
        owned.values().forEach(token -> loads.fail(token, e));
        throw e;
      }
      events.loadTime(Duration.ofNanos(System.nanoTime() - startNanos));
    }

    for (Map.Entry<K, LoadCoordinator.Acquisition<K, V>> entry : acquisitions.entrySet()) {
      V value =
          entry.getValue().owner()
              ? ownedResults.get(entry.getKey())
              : loads.await(entry.getValue().token());
      if (value != null) {
        result.put(entry.getKey(), value);
      }
    }
    return Map.copyOf(result);
  }

  private V completeLoaded(K key, V value, LoadCoordinator.Token<K, V> token) {
    CachedValue<V> loaded = value == null ? null : new CachedValue<>(value);
    if (loaded != null) {
      boolean retained =
          loads.publishIfCurrent(token, () -> entries.asMap().putIfAbsent(key, loaded));
      if (!retained) {
        events.loadDiscarded();
      }
    }
    CachedValue<V> current = entries.getIfPresent(key);
    V served = current == null ? value : current.value();
    loads.complete(token, served);
    return served;
  }

  @Override
  public void put(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "a cache holds no nulls; to drop a key use evict");
    loads.mutate(key, () -> entries.put(key, new CachedValue<>(value)));
  }

  /**
   * The fence stripe index for {@code key}. Spread first: keys here are structured strings whose
   * low bits carry little of the difference between them.
   */
  // Package-private so a test can assert that two keys it relies on still share a stripe.
  int stripeFor(K key) {
    return loads.stripeFor(key);
  }

  private record CachedValue<V>(V value) {}

  @Override
  public void evict(K key) {
    Objects.requireNonNull(key, "key");
    loads.mutate(key, () -> entries.invalidate(key));
  }

  @Override
  public void evictPartition(Predicate<K> belongsToPartition) {
    Objects.requireNonNull(belongsToPartition, "belongsToPartition");
    loads.mutatePartition(
        belongsToPartition, () -> entries.asMap().keySet().removeIf(belongsToPartition));
  }

  @Override
  public long bytes() {
    // Caffeine already maintains this because a weigher is set; a second counter beside it would
    // only be a way for the two to disagree. Its maintenance is asynchronous, so the figure lags
    // writes and would read under the limit while over it -- hence forcing it. cleanUp takes the
    // eviction lock outright, unlike the tryLock the write path uses to schedule drains, which is
    // the blocking MemoryCache#bytes warns about.
    entries.cleanUp();
    // Both are present because the builder always sets a weigher and a maximum weight. Asserted
    // rather than defaulted: a zero here would report an empty cache to the budget gauge, which is
    // the one number this contract exists to publish.
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
}
