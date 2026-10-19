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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Caffeine-backed {@link CoalescingExpiringCache}. */
public final class CaffeineCoalescingExpiringCache<K, V> implements CoalescingExpiringCache<K, V> {

  private final CaffeineExpiringCacheSupport<K, V> support;
  private final ConcurrentMap<K, InFlight<V>> inFlight = new ConcurrentHashMap<>();
  private final Object invalidateAllLock = new Object();
  private long generation;

  private CaffeineCoalescingExpiringCache(CaffeineExpiringCacheSupport<K, V> support) {
    this.support = support;
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
    return new CaffeineCoalescingExpiringCache<>(
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

    InFlight<V> load;
    InFlight<V> existing;
    synchronized (invalidateAllLock) {
      load = new InFlight<>(generation);
      existing = inFlight.putIfAbsent(key, load);
    }
    if (existing != null) {
      support.miss();
      return await(existing.result);
    }

    try {
      V raced = support.getIfPresent(key);
      if (raced != null) {
        synchronized (load) {
          load.result.complete(raced);
        }
        support.hit(startNanos);
        return raced;
      }
      V value = loader.apply(key);
      synchronized (load) {
        synchronized (invalidateAllLock) {
          if (!load.invalidated && load.generation == generation) {
            support.putIfRetained(key, value);
          }
        }
        load.result.complete(value);
      }
      support.loaded(startNanos);
      return value;
    } catch (RuntimeException failure) {
      completeExceptionally(load, failure);
      support.loadFailed(startNanos, failure);
      throw failure;
    } catch (Error failure) {
      completeExceptionally(load, failure);
      throw failure;
    } finally {
      inFlight.remove(key, load);
    }
  }

  @Override
  public void invalidate(K key) {
    Objects.requireNonNull(key, "key");
    InFlight<V> load = inFlight.get(key);
    if (load == null) {
      support.invalidate(key);
      return;
    }
    synchronized (load) {
      load.invalidated = true;
      inFlight.remove(key, load);
      support.invalidate(key);
    }
  }

  @Override
  public long estimatedSize() {
    return support.estimatedSize();
  }

  @Override
  public void invalidateAll() {
    synchronized (invalidateAllLock) {
      generation++;
      inFlight.clear();
      support.invalidateAll();
    }
  }

  private static <V> void completeExceptionally(InFlight<V> load, Throwable failure) {
    synchronized (load) {
      load.result.completeExceptionally(failure);
    }
  }

  private static <V> V await(CompletableFuture<V> result) {
    try {
      return result.get();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for in-flight cache load", failure);
    } catch (ExecutionException failure) {
      if (failure.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      if (failure.getCause() instanceof Error cause) {
        throw cause;
      }
      throw new IllegalStateException("In-flight cache load failed", failure.getCause());
    }
  }

  private static final class InFlight<V> {
    private final CompletableFuture<V> result = new CompletableFuture<>();
    private final long generation;
    private boolean invalidated;

    private InFlight(long generation) {
      this.generation = generation;
    }
  }
}
