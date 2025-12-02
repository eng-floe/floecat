package ai.floedb.metacat.service.query.graph.cache;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.query.graph.model.RelationNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-tenant cache manager for {@link RelationNode}s.
 *
 * <p>This manager keeps a dedicated Caffeine cache for each tenant so eviction pressure is isolated
 * between tenants while sharing the common configuration knobs (enable flag, max size, TTL). It
 * also exposes global gauges and per-tenant hit/miss counters so operators can understand cache
 * health across multi-tenant deployments.
 */
public class GraphCacheManager {

  private final boolean cacheEnabled;
  private final long cacheMaxSize;
  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Cache<GraphCacheKey, RelationNode>> tenantCaches =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> tenantHitCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> tenantMissCounters = new ConcurrentHashMap<>();

  public GraphCacheManager(boolean cacheEnabled, long cacheMaxSize, MeterRegistry meterRegistry) {
    this.cacheEnabled = cacheEnabled;
    this.cacheMaxSize = cacheMaxSize;
    this.meterRegistry = meterRegistry;
    if (meterRegistry != null) {
      Gauge.builder("metacat.metadata.graph.cache.enabled", () -> cacheEnabled ? 1.0 : 0.0)
          .register(meterRegistry);
      Gauge.builder(
              "metacat.metadata.graph.cache.max_size",
              () -> cacheEnabled ? (double) cacheMaxSize : 0.0)
          .register(meterRegistry);
      Gauge.builder("metacat.metadata.graph.cache.tenants", tenantCaches::size)
          .register(meterRegistry);
      Gauge.builder(
              "metacat.metadata.graph.cache.entries",
              () ->
                  tenantCaches.values().stream()
                      .mapToDouble(cache -> (double) cache.estimatedSize())
                      .sum())
          .register(meterRegistry);
    }
  }

  /**
   * Returns the cached node for the provided resource, or {@code null} when caching is disabled or
   * the entry is missing.
   */
  public RelationNode get(ResourceId id, GraphCacheKey key) {
    if (!cacheEnabled) {
      return null;
    }
    Cache<GraphCacheKey, RelationNode> cache = tenantCache(id.getTenantId());
    if (cache == null) {
      return null;
    }
    RelationNode node = cache.getIfPresent(key);
    incrementCounter(id.getTenantId(), node != null);
    return node;
  }

  /** Stores the resolved node inside the tenant cache. */
  public void put(ResourceId id, GraphCacheKey key, RelationNode node) {
    if (!cacheEnabled || node == null) {
      return;
    }
    Cache<GraphCacheKey, RelationNode> cache = tenantCache(id.getTenantId());
    if (cache != null) {
      cache.put(key, node);
    }
  }

  /** Evicts every cached version of the resource. */
  public void invalidate(ResourceId id) {
    if (!cacheEnabled) {
      return;
    }
    Cache<GraphCacheKey, RelationNode> cache = tenantCaches.get(id.getTenantId());
    if (cache != null) {
      cache.asMap().keySet().removeIf(key -> key.id().equals(id));
      if (cache.estimatedSize() == 0) {
        tenantCaches.remove(id.getTenantId(), cache);
      }
    }
  }

  private Cache<GraphCacheKey, RelationNode> tenantCache(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      return null;
    }
    return tenantCaches.computeIfAbsent(
        tenantId,
        id ->
            Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .expireAfterAccess(Duration.ofMinutes(15))
                .build());
  }

  private void incrementCounter(String tenantId, boolean hit) {
    if (meterRegistry == null || tenantId == null || tenantId.isBlank()) {
      return;
    }
    ConcurrentMap<String, Counter> counters = hit ? tenantHitCounters : tenantMissCounters;
    Counter counter =
        counters.computeIfAbsent(
            tenantId,
            id ->
                meterRegistry.counter(
                    "metacat.metadata.graph.cache", "result", hit ? "hit" : "miss", "tenant", id));
    counter.increment();
  }
}
