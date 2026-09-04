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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.CaffeineMemoryCache;
import ai.floedb.floecat.cache.MemoryCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.cache.AuthoritativePointerStore;
import ai.floedb.floecat.service.repo.cache.CachingPointerStore;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.RawPointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Where a cache on the {@link MemoryCache} contract is built, and the pointer store that wraps one.
 *
 * <p>A cache here is a plain {@link CaffeineMemoryCache} differing only in its budget and key
 * weight. Pointer-specific version ordering belongs to {@link CachingPointerStore}, not to the
 * generic memory primitive.
 */
@ApplicationScoped
public class MetadataCaches {

  /**
   * The query-serving pointer view: the real store with its cache in front, or the real store alone
   * when the cache is switched off. The raw store is reachable only by asking for it with {@link
   * RawPointerStore}, which nothing but this does.
   *
   * <p>Off means the decorator is not installed, not that a cache is installed and holds nothing. A
   * budget of zero is refused at startup rather than read as "off" -- a cache sized zero reports a
   * 0% hit rate, which looks like a cache that is not helping rather than one that was turned off.
   * So disabling is its own switch, and it is the operator's way back to the pre-cache read path
   * without a rollback.
   */
  // @Singleton, not @ApplicationScoped: a normal scope is injected as a client proxy, and a proxy
  // is not the object -- an instanceof or a field read sees the proxy, not what it stands for.
  // Nothing here needs scope semantics, and the proxy only hides what is wired.
  @Produces
  @Singleton
  @CachedPointerStore
  public PointerStore cachedPointerStore(
      @RawPointerStore PointerStore raw,
      MemoryCache<String, Pointer> pointers,
      @ConfigProperty(name = "floecat.cache.pointer.enabled", defaultValue = "true")
          boolean enabled) {
    return enabled ? new CachingPointerStore(raw, pointers) : raw;
  }

  /**
   * The safe default for mutation, GC and reconciliation code. Ordinary reads on this view are
   * authoritative; query-serving code opts into {@link CachedPointerStore} once at injection.
   */
  @Produces
  @Singleton
  public PointerStore pointerStore(@CachedPointerStore PointerStore cached) {
    return AuthoritativePointerStore.of(cached);
  }

  /** Pointers: mutable addressing, with publication semantics owned by the store decorator. */
  @Produces
  @ApplicationScoped
  public MemoryCache<String, Pointer> pointers(
      CacheBudgetResolver budgets, Observability observability) {
    var metrics = metricsFor(CacheFamily.POINTER, observability);
    var cache =
        new CaffeineMemoryCache<String, Pointer>(
            CacheFamily.POINTER,
            budgets.bytesFor(CacheFamily.POINTER),
            // The key's own bytes, which the weigher adds to the entry's machinery and payload.
            key -> 2L * key.length(),
            events(metrics));
    report(cache, budgets, metrics);
    return cache;
  }

  private static CacheMetrics metricsFor(CacheFamily family, Observability observability) {
    return new CacheMetrics(observability, "service", "metadata-cache", family.tag());
  }

  /**
   * Counts hits and misses and times the loads. Enough, with the gauges below, to answer the
   * questions asked of a cache that is behaving oddly: whether it is on, whether it is being used,
   * what a miss costs, how full it is, whether loads are failing, and whether it has stopped
   * retaining what it loads.
   */
  private static CacheEvents events(CacheMetrics metrics) {
    return new CacheEvents() {
      @Override
      public void hit(java.time.Duration served) {
        metrics.recordHit();
        metrics.recordLoad(served, true);
      }

      @Override
      public void miss() {
        metrics.recordMiss();
      }

      @Override
      public void loadTime(java.time.Duration elapsed) {
        metrics.recordLoad(elapsed, false);
      }

      @Override
      public void loadFailed(java.time.Duration elapsed, RuntimeException error) {
        metrics.recordLoadFailure(elapsed, error);
      }

      @Override
      public void loadDiscarded() {
        metrics.recordLoadDiscarded();
      }

      @Override
      public void evicted(long weightBytes) {
        metrics.recordEviction(weightBytes);
      }
    };
  }

  /**
   * One metric shape for every family, registered where the cache is built rather than inside it.
   * Keyed by the family tag, so families are directly comparable and adding a cache brings its
   * telemetry with it instead of being remembered separately.
   */
  private static void report(
      MemoryCache<?, ?> cache, CacheBudgetResolver budgets, CacheMetrics metrics) {
    String tag = cache.family().tag();
    // Fixed at construction, so both gauges read the same captured value rather than one of them
    // re-resolving the budget on every scrape.
    long budget = budgets.bytesFor(cache.family());
    metrics.trackEnabled(
        () -> budget > 0 ? 1.0 : 0.0, "Whether the " + tag + " cache holds anything");
    metrics.trackSize(cache::entryCount, "Entries held by the " + tag + " cache");
    metrics.trackWeightedSize(
        () -> (double) cache.bytes(), "Retained bytes held by the " + tag + " cache");
    metrics.trackMaxWeight(() -> (double) budget, "Byte budget for the " + tag + " cache");
    // Hits, misses, load latency and evictions are not registered here: they are events, recorded
    // per read by the cache itself through CacheEvents, not gauges sampled from a running total.
  }
}
