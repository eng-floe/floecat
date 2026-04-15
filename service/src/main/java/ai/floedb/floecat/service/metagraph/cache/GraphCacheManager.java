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

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.cache.GraphCacheKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-account cache manager for {@link GraphNode}s.
 *
 * <p>This manager keeps a dedicated Caffeine cache for each account so eviction pressure is
 * isolated between accounts while sharing the common configuration knobs (enable flag, max size,
 * TTL). It also exposes global gauges and per-account hit/miss counters so operators can understand
 * cache health across multi-account deployments.
 *
 * <p>Consistency model: the optional mutation-meta cache is best-effort and eventually consistent.
 * After a write, callers may observe a stale pointer version for at most the configured meta cache
 * TTL window.
 */
public class GraphCacheManager {

  private final boolean graphCacheEnabled;
  private final boolean metaCacheEnabled;
  private final long cacheMaxSize;
  private final Observability observability;
  private final CacheMetrics cacheMetrics;
  private final CacheMetrics metaCacheMetrics;
  private final ConcurrentMap<String, Cache<GraphCacheKey, GraphNode>> accountCaches =
      new ConcurrentHashMap<>();
  private final Cache<ResourceId, MutationMeta> metaCache;

  public GraphCacheManager(
      boolean cacheEnabled,
      long cacheMaxSize,
      long metaCacheTtlSeconds,
      Observability observability) {
    this.graphCacheEnabled = cacheEnabled;
    this.metaCacheEnabled = metaCacheTtlSeconds > 0;
    this.cacheMaxSize = cacheMaxSize;
    this.observability = Objects.requireNonNull(observability, "observability");
    this.metaCache =
        metaCacheEnabled
            ? Caffeine.newBuilder()
                .maximumSize(Math.max(1L, cacheMaxSize))
                .expireAfterWrite(Duration.ofSeconds(metaCacheTtlSeconds))
                .build()
            : null;
    this.cacheMetrics =
        new CacheMetrics(this.observability, "service", "graph-cache", "graph-cache");
    this.metaCacheMetrics =
        new CacheMetrics(this.observability, "service", "graph-cache", "graph-meta-cache");
    registerGauges();
    cacheMetrics.trackSize(
        () -> accountCaches.values().stream().mapToDouble(Cache::estimatedSize).sum(),
        "Estimated graph cache entries");
    metaCacheMetrics.trackEnabled(
        () -> metaCacheEnabled ? 1.0 : 0.0, "Graph metadata cache enabled");
    metaCacheMetrics.trackMaxEntries(
        () -> metaCacheEnabled ? (double) cacheMaxSize : 0.0,
        "Graph metadata cache configured max entries");
    metaCacheMetrics.trackSize(
        () -> metaCacheEnabled && metaCache != null ? (double) metaCache.estimatedSize() : 0.0,
        "Estimated graph metadata cache entries");
  }

  public GraphCacheManager(boolean cacheEnabled, long cacheMaxSize, Observability observability) {
    this(cacheEnabled, cacheMaxSize, 0L, observability);
  }

  /**
   * Returns the cached node for the provided resource, or {@code null} when caching is disabled or
   * the entry is missing.
   */
  public GraphNode get(ResourceId id, GraphCacheKey key) {
    if (!graphCacheEnabled) {
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
    if (!graphCacheEnabled || node == null) {
      return;
    }
    Cache<GraphCacheKey, GraphNode> cache = accountCache(id.getAccountId());
    if (cache != null) {
      cache.put(key, node);
    }
  }

  /** Evicts every cached version of the resource. */
  public void invalidate(ResourceId id) {
    if (graphCacheEnabled) {
      Cache<GraphCacheKey, GraphNode> cache = accountCaches.get(id.getAccountId());
      if (cache != null) {
        cache.asMap().keySet().removeIf(key -> key.id().equals(id));
        if (cache.estimatedSize() == 0) {
          accountCaches.remove(id.getAccountId(), cache);
        }
      }
    }
    if (metaCacheEnabled && metaCache != null) {
      metaCache.invalidate(id);
    }
  }

  public MutationMeta getMeta(ResourceId id) {
    if (!metaCacheEnabled || metaCache == null) {
      return null;
    }
    MutationMeta meta = metaCache.getIfPresent(id);
    incrementMetaCounter(id.getAccountId(), meta != null);
    return meta;
  }

  public void putMeta(ResourceId id, MutationMeta meta) {
    if (!metaCacheEnabled || metaCache == null || meta == null) {
      return;
    }
    metaCache.put(id, meta);
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
    if (accountId == null || accountId.isBlank()) {
      return;
    }
    Tag accountTag = Tag.of(TagKey.ACCOUNT, accountId);
    if (hit) {
      cacheMetrics.recordHit(accountTag);
    } else {
      cacheMetrics.recordMiss(accountTag);
    }
  }

  private void incrementMetaCounter(String accountId, boolean hit) {
    if (accountId == null || accountId.isBlank()) {
      return;
    }
    Tag accountTag = Tag.of(TagKey.ACCOUNT, accountId);
    if (hit) {
      metaCacheMetrics.recordHit(accountTag);
    } else {
      metaCacheMetrics.recordMiss(accountTag);
    }
  }

  private void registerGauges() {
    cacheMetrics.trackEnabled(() -> graphCacheEnabled ? 1.0 : 0.0, "Graph cache enabled");
    cacheMetrics.trackMaxEntries(
        () -> graphCacheEnabled ? (double) cacheMaxSize : 0.0,
        "Graph cache configured max entries");
    cacheMetrics.trackAccounts(() -> (double) accountCaches.size(), "Graph cache account count");
  }

  public void recordLoad(Duration duration) {
    if (duration == null) {
      return;
    }
    if (!graphCacheEnabled) {
      return;
    }
    cacheMetrics.recordLoad(duration, true);
  }

  public void recordLoadFailure(Duration duration, Throwable error) {
    if (duration == null || !graphCacheEnabled) {
      return;
    }
    cacheMetrics.recordLoadFailure(duration, error);
  }
}
