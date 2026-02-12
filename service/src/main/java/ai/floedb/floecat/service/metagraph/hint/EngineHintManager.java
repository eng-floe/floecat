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

package ai.floedb.floecat.service.metagraph.hint;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Centralized cache and dispatch layer for engine-specific hint providers. */
@ApplicationScoped
public class EngineHintManager {

  private static final Logger LOG = Logger.getLogger(EngineHintManager.class);

  private final List<EngineHintProvider> providers;
  private final Cache<HintCacheKey, EngineHint> cache;
  private final Observability observability;
  private final CacheMetrics cacheMetrics;
  private final MetricId weightMetric;

  @Inject
  public EngineHintManager(
      Instance<EngineHintProvider> providers,
      Observability observability,
      @ConfigProperty(name = "floecat.metadata.hint.cache-max-weight", defaultValue = "67108864")
          long maxWeightBytes) {
    this(
        providers == null ? List.of() : providers.stream().toList(),
        observability,
        maxWeightBytes,
        false); // always async in prod
  }

  /** Production constructor */
  EngineHintManager(
      List<EngineHintProvider> providers, Observability observability, long maxWeightBytes) {
    this(providers, observability, maxWeightBytes, false);
  }

  /** Test-only constructor enabling synchronous eviction */
  EngineHintManager(
      List<EngineHintProvider> providers,
      Observability observability,
      long maxWeightBytes,
      boolean forceSynchronous) {
    this.providers = List.copyOf(providers == null ? List.of() : providers);
    this.observability = Objects.requireNonNull(observability, "observability");
    this.cacheMetrics =
        new CacheMetrics(observability, "service", "engine-hint-cache", "engine-hint-cache");
    this.weightMetric = ServiceMetrics.Hint.CACHE_WEIGHT;

    long bounded = Math.max(1, Math.min(Integer.MAX_VALUE, maxWeightBytes));

    Weigher<HintCacheKey, EngineHint> weigher =
        (k, v) -> (int) Math.min(Integer.MAX_VALUE, v.sizeBytes());

    var builder =
        Caffeine.<HintCacheKey, EngineHint>newBuilder()
            .maximumWeight(bounded)
            .weigher(weigher)
            .expireAfterAccess(Duration.ofMinutes(30));

    if (forceSynchronous) {
      builder = builder.executor(Runnable::run);
    }

    this.cache = builder.build();
    observability.gauge(weightMetric, () -> weightedSize(cache), "Engine hint cache weight bytes");
  }

  private static double weightedSize(Cache<HintCacheKey, EngineHint> cache) {
    return cache
        .policy()
        .eviction()
        .flatMap(
            eviction ->
                eviction.weightedSize().isPresent()
                    ? Optional.of(eviction.weightedSize().getAsLong())
                    : Optional.empty())
        .orElse(0L);
  }

  /** Retrieves (or computes) the hint for the request, returning empty when no provider applies. */
  public Optional<EngineHint> get(
      GraphNode node, EngineKey engineKey, String payloadType, String correlationId) {
    Objects.requireNonNull(engineKey, "engineKey");
    Objects.requireNonNull(payloadType, "payloadType");
    EngineHintKey engineHintKey =
        new EngineHintKey(engineKey.engineKind(), engineKey.engineVersion(), payloadType);
    return get(node, engineHintKey, correlationId);
  }

  /** Retrieves (or computes) the hint for the request, returning empty when no provider applies. */
  public Optional<EngineHint> get(
      GraphNode node, EngineHintKey engineHintKey, String correlationId) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(engineHintKey, "engineHintKey");
    Objects.requireNonNull(engineHintKey.payloadType(), "payloadType");

    EngineKey engineKey = new EngineKey(engineHintKey.engineKind(), engineHintKey.engineVersion());
    return getInternal(node, engineKey, engineHintKey.payloadType(), correlationId, engineHintKey);
  }

  private Optional<EngineHint> getInternal(
      GraphNode node,
      EngineKey engineKey,
      String payloadType,
      String correlationId,
      EngineHintKey engineHintKey) {
    Optional<EngineHintProvider> provider = selectProvider(node, payloadType, engineKey);
    if (provider.isEmpty()) {
      return Optional.empty();
    }

    String fingerprint = provider.get().fingerprint(node, engineKey, payloadType);
    HintCacheKey key = new HintCacheKey(node.id(), node.version(), engineHintKey, fingerprint);
    EngineHint cached = cache.getIfPresent(key);
    if (cached != null) {
      recordHit();
      return Optional.of(cached);
    }
    recordMiss();
    Optional<EngineHint> computed;
    try {
      computed = provider.get().compute(node, engineKey, payloadType, correlationId);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Engine hint provider failed (payload=%s, engine=%s)", payloadType, engineKey);
      throw e;
    }
    if (computed.isEmpty()) {
      return Optional.empty();
    }
    cache.put(key, computed.get());
    return computed;
  }

  /** Evicts every cached hint related to the provided resource. */
  public void invalidate(ResourceId resourceId) {
    if (resourceId == null) {
      return;
    }
    cache.asMap().keySet().removeIf(key -> key.resourceId().equals(resourceId));
  }

  private Optional<EngineHintProvider> selectProvider(
      GraphNode node, String payloadType, EngineKey engineKey) {
    return providers.stream()
        .filter(p -> p.supports(node.kind(), payloadType) && p.isAvailable(engineKey))
        .findFirst();
  }

  /** Test-only: exposes the Caffeine cache for inspection in unit tests. */
  Cache<HintCacheKey, EngineHint> cache() {
    return cache;
  }

  private void recordHit() {
    cacheMetrics.recordHit();
  }

  private void recordMiss() {
    cacheMetrics.recordMiss();
  }

  static final record HintCacheKey(
      ResourceId resourceId, long pointerVersion, EngineHintKey engineHintKey, String fingerprint) {

    HintCacheKey {
      Objects.requireNonNull(resourceId, "resourceId");
      Objects.requireNonNull(engineHintKey, "engineHintKey");
      Objects.requireNonNull(fingerprint, "fingerprint");
    }
  }
}
