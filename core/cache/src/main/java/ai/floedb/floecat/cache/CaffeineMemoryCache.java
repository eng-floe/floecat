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
import java.util.concurrent.locks.StampedLock;
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

  // Striped, so an unrelated key's write is a collision rather than a certainty; one fence for the
  // cache would refuse every install on a cache taking a publish per commit. Not a tunable: the
  // count only buys a lower collision rate, and a collision merely costs a reload.
  private static final int FENCE_STRIPES = 256;
  // Every stored value has a distinct wrapper so a raced load can remove its own installation
  // without removing a writer's publication of the exact same value object.
  private static final long CACHED_VALUE_BYTES = 16L;

  private final CacheFamily family;
  private final com.github.benmanes.caffeine.cache.Cache<K, CachedValue<V>> entries;
  private final ToLongFunction<K> keyWeight;
  private final CacheEvents events;
  private final long weightUnitBytes;
  private final StampedLock[] fences = new StampedLock[FENCE_STRIPES];

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
    // Caffeine accepts a long total but an int per-entry weight. Use larger units when the budget
    // itself cannot be represented in bytes, rounding entries up and the budget down so the real
    // byte ceiling is never exceeded. Leave one int unit unused: if an entry is larger than the
    // whole budget, clamping its weight to Integer.MAX_VALUE must still put it over the ceiling.
    this.weightUnitBytes = divideRoundUp(maxBytes, Integer.MAX_VALUE - 1L);
    long maximumWeightUnits = maxBytes / weightUnitBytes;
    for (int stripe = 0; stripe < fences.length; stripe++) {
      fences[stripe] = new StampedLock();
    }
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
    // Timed and counted here rather than read off Caffeine's cumulative stats: see CacheEvents.
    LoadedValue<V> loaded = new LoadedValue<>();
    long startNanos = System.nanoTime();
    // Sampled before the load because the load runs under a map reservation that some mutations
    // cannot observe. If one wins the race, the load undoes only its own installation afterwards.
    StampedLock fence = fenceFor(key);
    long stamp = fence.tryOptimisticRead();
    CachedValue<V> cached;
    try {
      cached =
          entries.get(
              key,
              k -> {
                loaded.invoked = true;
                V value = loader.load(k);
                loaded.entry = value == null ? null : new CachedValue<>(value);
                return loaded.entry;
              });
    } catch (RuntimeException e) {
      if (loaded.invoked) {
        events.miss();
      }
      // An unweighable value is a defect in the value type, not a failed store read -- the weigher
      // runs inside this compute, so its refusal comes back out of here. Reporting it as a load
      // failure would point an operator at a store that is working.
      if (!(e instanceof UnweighableValueException)) {
        events.loadFailed(Duration.ofNanos(System.nanoTime() - startNanos), e);
      }
      throw e;
    }
    // Outside the try on purpose: inside it, a metrics implementation that threw would be recorded
    // and rethrown as a load failure -- a broken exporter reported as a broken store.
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
    if (loaded.invoked) {
      if (loaded.entry != null) {
        // A loader that returned null cached nothing, so there is nothing to drop and no refusal
        // to report -- counting one would inflate the signal that means "this cache stopped
        // warming" on a path that never fills.
        dropIfFenceMoved(key, loaded.entry, fence, stamp);
      }
      events.miss();
      events.loadTime(elapsed);
    } else {
      // Includes a follower that waited on another caller's load: Caffeine never invokes the
      // mapping function for it, so it is a hit -- one that took as long as the load.
      events.hit(elapsed);
    }
    return cached == null ? null : cached.value();
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
    Map<K, ReadStamp> misses = new LinkedHashMap<>();
    for (K key : distinctKeys) {
      long startNanos = System.nanoTime();
      CachedValue<V> value = entries.getIfPresent(key);
      if (value != null) {
        result.put(key, value.value());
        events.hit(Duration.ofNanos(System.nanoTime() - startNanos));
      } else {
        StampedLock fence = fenceFor(key);
        misses.put(key, new ReadStamp(fence, fence.tryOptimisticRead()));
        events.miss();
      }
    }
    if (misses.isEmpty()) {
      return Map.copyOf(result);
    }

    long startNanos = System.nanoTime();
    Map<K, V> loaded;
    try {
      loaded = Objects.requireNonNull(loader.load(Set.copyOf(misses.keySet())), "loader result");
      for (Map.Entry<K, V> entry : loaded.entrySet()) {
        if (!misses.containsKey(entry.getKey())) {
          throw new IllegalArgumentException(
              "loader returned an unrequested key: " + entry.getKey());
        }
        Objects.requireNonNull(entry.getValue(), "a bulk loader must omit absent keys");
      }
    } catch (RuntimeException e) {
      if (!(e instanceof UnweighableValueException)) {
        events.loadFailed(Duration.ofNanos(System.nanoTime() - startNanos), e);
      }
      throw e;
    }
    events.loadTime(Duration.ofNanos(System.nanoTime() - startNanos));

    for (Map.Entry<K, ReadStamp> miss : misses.entrySet()) {
      K key = miss.getKey();
      V value = loaded.get(key);
      if (value == null) {
        continue;
      }
      LoadedValue<V> installed = new LoadedValue<>();
      CachedValue<V> retained =
          entries.get(
              key,
              ignored -> {
                installed.invoked = true;
                installed.entry = new CachedValue<>(value);
                return installed.entry;
              });
      if (installed.invoked) {
        ReadStamp stamp = miss.getValue();
        dropIfFenceMoved(key, installed.entry, stamp.fence(), stamp.stamp());
      }
      // A concurrent cache load may have won before this batch reached the key. Serve that winner
      // rather than the batch's older answer; a mutation race still serves this batch's answer and
      // leaves the cache absent or with the mutator's value.
      result.put(key, retained.value());
    }
    return Map.copyOf(result);
  }

  @Override
  public void put(K key, V value) {
    Objects.requireNonNull(value, "a cache holds no nulls; to drop a key use evict");
    StampedLock fence = fenceFor(key);
    long stamp = fence.writeLock();
    try {
      entries.put(key, new CachedValue<>(value));
    } finally {
      fence.unlockWrite(stamp);
    }
  }

  /**
   * Drops what a load installed for {@code key} when a write moved its stripe's fence since {@code
   * fence} was sampled.
   *
   * <p>The fence moving means a write MAY have raced this key: the stripe is shared, and a range
   * eviction moves every fence. Leaving the key absent rather than holding a pre-write value is the
   * safe resolution either way, and the caller keeps the value it loaded.
   */
  private void dropIfFenceMoved(K key, CachedValue<V> loaded, StampedLock fence, long stamp) {
    if (!fence.validate(stamp)) {
      // Remove only this load's installation. A writer may already have replaced it with the value
      // it published, and dropping that value would turn write-through back into invalidation.
      entries
          .asMap()
          .computeIfPresent(key, (ignored, current) -> current == loaded ? null : current);
      events.loadDiscarded();
    }
  }

  private StampedLock fenceFor(K key) {
    return fences[stripeFor(key)];
  }

  /**
   * The fence stripe index for {@code key}. Spread first: keys here are structured strings whose
   * low bits carry little of the difference between them.
   */
  // Package-private so a test can assert that two keys it relies on still share a stripe.
  int stripeFor(K key) {
    int spread = key.hashCode() * 0x9E3779B9;
    return (spread >>> 16) & (FENCE_STRIPES - 1);
  }

  private record ReadStamp(StampedLock fence, long stamp) {}

  private record CachedValue<V>(V value) {}

  private static final class LoadedValue<V> {
    private boolean invoked;
    private CachedValue<V> entry;
  }

  @Override
  public void evict(K key) {
    StampedLock fence = fenceFor(key);
    long stamp = fence.writeLock();
    try {
      entries.invalidate(key);
    } finally {
      fence.unlockWrite(stamp);
    }
  }

  @Override
  public void evictPartition(Predicate<K> belongsToPartition) {
    // A range has no one stripe. Holding all of them makes every concurrent load's optimistic read
    // fail, including one that starts after the sweep began but before the key-set walk reaches it.
    long[] stamps = new long[fences.length];
    for (int stripe = 0; stripe < fences.length; stripe++) {
      stamps[stripe] = fences[stripe].writeLock();
    }
    try {
      entries.asMap().keySet().removeIf(belongsToPartition);
    } finally {
      for (int stripe = fences.length - 1; stripe >= 0; stripe--) {
        fences[stripe].unlockWrite(stamps[stripe]);
      }
    }
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
