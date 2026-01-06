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

package ai.floedb.floecat.service.metagraph.cache;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.cache.GraphCacheKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-account cache manager for {@link GraphNode}s.
 *
 * <p>This manager keeps a dedicated Caffeine cache for each account so eviction pressure is
 * isolated between accounts while sharing the common configuration knobs (enable flag, max size,
 * TTL). It also exposes global gauges and per-account hit/miss counters so operators can understand
 * cache health across multi-account deployments.
 */
public class GraphCacheManager {

  private final boolean cacheEnabled;
  private final long cacheMaxSize;
  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Cache<GraphCacheKey, GraphNode>> accountCaches =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> accountHitCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> accountMissCounters = new ConcurrentHashMap<>();

  public GraphCacheManager(boolean cacheEnabled, long cacheMaxSize, MeterRegistry meterRegistry) {
    this.cacheEnabled = cacheEnabled;
    this.cacheMaxSize = cacheMaxSize;
    this.meterRegistry = meterRegistry;
    if (meterRegistry != null) {
      Gauge.builder("floecat.metadata.graph.cache.enabled", () -> cacheEnabled ? 1.0 : 0.0)
          .register(meterRegistry);
      Gauge.builder(
              "floecat.metadata.graph.cache.max_size",
              () -> cacheEnabled ? (double) cacheMaxSize : 0.0)
          .register(meterRegistry);
      Gauge.builder("floecat.metadata.graph.cache.accounts", accountCaches::size)
          .register(meterRegistry);
      Gauge.builder(
              "floecat.metadata.graph.cache.entries",
              () ->
                  accountCaches.values().stream()
                      .mapToDouble(cache -> (double) cache.estimatedSize())
                      .sum())
          .register(meterRegistry);
    }
  }

  /**
   * Returns the cached node for the provided resource, or {@code null} when caching is disabled or
   * the entry is missing.
   */
  public GraphNode get(ResourceId id, GraphCacheKey key) {
    if (!cacheEnabled) {
      return null;
    }
    Cache<GraphCacheKey, GraphNode> cache = accountCache(id.getAccountId());
    if (cache == null) {
      return null;
    }
    GraphNode node = cache.getIfPresent(key);
    incrementCounter(id.getAccountId(), node != null);
    return node;
  }

  /** Stores the resolved node inside the account cache. */
  public void put(ResourceId id, GraphCacheKey key, GraphNode node) {
    if (!cacheEnabled || node == null) {
      return;
    }
    Cache<GraphCacheKey, GraphNode> cache = accountCache(id.getAccountId());
    if (cache != null) {
      cache.put(key, node);
    }
  }

  /** Evicts every cached version of the resource. */
  public void invalidate(ResourceId id) {
    if (!cacheEnabled) {
      return;
    }
    Cache<GraphCacheKey, GraphNode> cache = accountCaches.get(id.getAccountId());
    if (cache != null) {
      cache.asMap().keySet().removeIf(key -> key.id().equals(id));
      if (cache.estimatedSize() == 0) {
        accountCaches.remove(id.getAccountId(), cache);
      }
    }
  }

  public MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  private Cache<GraphCacheKey, GraphNode> accountCache(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      return null;
    }
    return accountCaches.computeIfAbsent(
        accountId,
        id ->
            Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .expireAfterAccess(Duration.ofMinutes(15))
                .build());
  }

  private void incrementCounter(String accountId, boolean hit) {
    if (meterRegistry == null || accountId == null || accountId.isBlank()) {
      return;
    }
    ConcurrentMap<String, Counter> counters = hit ? accountHitCounters : accountMissCounters;
    Counter counter =
        counters.computeIfAbsent(
            accountId,
            id ->
                meterRegistry.counter(
                    "floecat.metadata.graph.cache", "result", hit ? "hit" : "miss", "account", id));
    counter.increment();
  }
}
