package ai.floedb.metacat.service.query.graph.hint;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.query.graph.model.EngineHint;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import ai.floedb.metacat.service.query.graph.model.RelationNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
  private final Counter hitCounter;
  private final Counter missCounter;

  @Inject
  public EngineHintManager(
      Instance<EngineHintProvider> providers,
      MeterRegistry meterRegistry,
      @ConfigProperty(name = "metacat.metadata.hint.cache-max-weight", defaultValue = "67108864")
          long maxWeightBytes) {
    this(
        providers == null ? List.of() : providers.stream().toList(), meterRegistry, maxWeightBytes);
  }

  EngineHintManager(
      List<EngineHintProvider> providers, MeterRegistry meterRegistry, long maxWeightBytes) {
    this.providers = List.copyOf(providers == null ? List.of() : providers);
    long boundedWeight = Math.max(1, Math.min(Integer.MAX_VALUE, maxWeightBytes));
    Weigher<HintCacheKey, EngineHint> weigher =
        (key, hint) -> (int) Math.min(Integer.MAX_VALUE, hint.sizeBytes());
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(boundedWeight)
            .weigher(weigher)
            .expireAfterAccess(Duration.ofMinutes(30))
            .build();
    if (meterRegistry != null) {
      this.hitCounter = meterRegistry.counter("metacat.metadata.hint.cache", "result", "hit");
      this.missCounter = meterRegistry.counter("metacat.metadata.hint.cache", "result", "miss");
      meterRegistry.gauge(
          "metacat.metadata.hint.cache.weight_bytes", cache, EngineHintManager::weightedSize);
    } else {
      this.hitCounter = null;
      this.missCounter = null;
    }
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
      RelationNode node, EngineKey engineKey, String hintType, String correlationId) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(engineKey, "engineKey");
    Objects.requireNonNull(hintType, "hintType");

    Optional<EngineHintProvider> provider = selectProvider(node, hintType, engineKey);
    if (provider.isEmpty()) {
      return Optional.empty();
    }

    String fingerprint = provider.get().fingerprint(node, engineKey, hintType);
    HintCacheKey key =
        new HintCacheKey(node.id(), node.version(), engineKey, hintType, fingerprint);
    EngineHint cached = cache.getIfPresent(key);
    if (cached != null) {
      if (hitCounter != null) {
        hitCounter.increment();
      }
      return Optional.of(cached);
    }
    if (missCounter != null) {
      missCounter.increment();
    }
    EngineHint computed;
    try {
      computed = provider.get().compute(node, engineKey, hintType, correlationId);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Engine hint provider failed (hint=%s, engine=%s)", hintType, engineKey);
      throw e;
    }
    if (computed == null) {
      return Optional.empty();
    }
    cache.put(key, computed);
    return Optional.of(computed);
  }

  /** Evicts every cached hint related to the provided resource. */
  public void invalidate(ResourceId resourceId) {
    if (resourceId == null) {
      return;
    }
    cache.asMap().keySet().removeIf(key -> key.resourceId().equals(resourceId));
  }

  private Optional<EngineHintProvider> selectProvider(
      RelationNode node, String hintType, EngineKey engineKey) {
    return providers.stream()
        .filter(p -> p.supports(node.kind(), hintType) && p.isAvailable(engineKey))
        .findFirst();
  }

  private record HintCacheKey(
      ResourceId resourceId,
      long pointerVersion,
      EngineKey engineKey,
      String hintType,
      String fingerprint) {

    private HintCacheKey {
      Objects.requireNonNull(resourceId, "resourceId");
      Objects.requireNonNull(engineKey, "engineKey");
      Objects.requireNonNull(hintType, "hintType");
      Objects.requireNonNull(fingerprint, "fingerprint");
    }
  }
}
