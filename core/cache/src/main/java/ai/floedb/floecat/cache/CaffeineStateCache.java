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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.Objects;

/** Caffeine-backed implementation of the mutable {@link StateCache} contract. */
public final class CaffeineStateCache<K, V> implements StateCache<K, V> {

  private final Cache<K, V> entries;

  private CaffeineStateCache(Cache<K, V> entries) {
    this.entries = entries;
  }

  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  @Override
  public V getIfPresent(K key) {
    return entries.getIfPresent(Objects.requireNonNull(key, "key"));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    return entries.asMap().putIfAbsent(key, value);
  }

  @Override
  public void put(K key, V value) {
    entries.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
  }

  @Override
  public V remove(K key) {
    return entries.asMap().remove(Objects.requireNonNull(key, "key"));
  }

  @Override
  public V compute(K key, java.util.function.BiFunction<? super K, ? super V, ? extends V> fn) {
    return entries.asMap().compute(Objects.requireNonNull(key, "key"), fn);
  }

  @Override
  public V computeIfPresent(
      K key, java.util.function.BiFunction<? super K, ? super V, ? extends V> fn) {
    return entries.asMap().computeIfPresent(Objects.requireNonNull(key, "key"), fn);
  }

  @Override
  public long estimatedSize() {
    return entries.estimatedSize();
  }

  @Override
  public void invalidateAll() {
    entries.invalidateAll();
  }

  public static final class Builder<K, V> {
    private Long maximumSize;
    private Long maximumWeight;
    private Weigher<? super K, ? super V> weigher;
    private Duration expireAfterWrite;
    private RemovalListener<? super K, ? super V> removalListener;
    private boolean recordStats;

    public Builder<K, V> maximumSize(long maximumSize) {
      if (maximumSize <= 0) {
        throw new IllegalArgumentException("maximumSize must be positive");
      }
      this.maximumSize = maximumSize;
      this.maximumWeight = null;
      this.weigher = null;
      return this;
    }

    public Builder<K, V> maximumWeight(long maximumWeight, Weigher<? super K, ? super V> weigher) {
      maximumWeight(maximumWeight);
      return weigher(weigher);
    }

    public Builder<K, V> maximumWeight(long maximumWeight) {
      if (maximumWeight <= 0) {
        throw new IllegalArgumentException("maximumWeight must be positive");
      }
      this.maximumWeight = maximumWeight;
      this.maximumSize = null;
      return this;
    }

    public Builder<K, V> weigher(Weigher<? super K, ? super V> weigher) {
      this.weigher = Objects.requireNonNull(weigher, "weigher");
      return this;
    }

    public Builder<K, V> expireAfterWrite(Duration duration) {
      this.expireAfterWrite = Objects.requireNonNull(duration, "duration");
      return this;
    }

    public Builder<K, V> removalListener(RemovalListener<? super K, ? super V> listener) {
      this.removalListener = Objects.requireNonNull(listener, "listener");
      return this;
    }

    public Builder<K, V> recordStats() {
      this.recordStats = true;
      return this;
    }

    public StateCache<K, V> build() {
      if (maximumSize == null && maximumWeight == null) {
        throw new IllegalStateException("a maximum size or weight is required");
      }
      Caffeine<Object, Object> builder = Caffeine.newBuilder();
      if (maximumSize != null) {
        builder.maximumSize(maximumSize);
      } else {
        if (weigher == null) {
          throw new IllegalStateException("a weigher is required with maximumWeight");
        }
        builder.maximumWeight(maximumWeight).weigher((Weigher<Object, Object>) weigher);
      }
      if (expireAfterWrite != null) {
        builder.expireAfterWrite(expireAfterWrite);
      }
      if (recordStats) {
        builder.recordStats();
      }
      if (removalListener != null) {
        builder.removalListener((RemovalListener<Object, Object>) removalListener);
      }
      return new CaffeineStateCache<>((Cache<K, V>) builder.build());
    }
  }
}
