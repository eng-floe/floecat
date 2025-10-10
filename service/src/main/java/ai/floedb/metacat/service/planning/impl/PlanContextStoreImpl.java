package ai.floedb.metacat.service.planning.impl;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import ai.floedb.metacat.service.planning.PlanContextStore;

@ApplicationScoped
public class PlanContextStoreImpl implements PlanContextStore {

  @ConfigProperty(name = "metacat.plan.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  @ConfigProperty(name = "metacat.plan.ended-grace-ms", defaultValue = "15000")
  long endedGraceMs;

  @ConfigProperty(name = "metacat.plan.max-size", defaultValue = "10000")
  long maxSize;

  @ConfigProperty(name = "metacat.plan.safety-expiry-minutes", defaultValue = "10")
  long safetyExpiryMinutes;

  private final AtomicLong versionGen = new AtomicLong(1);
  private final Clock clock = Clock.systemUTC();

  private Cache<String, PlanContext> cache;

  @PostConstruct
  void init() {
    cache = Caffeine.newBuilder()
      .maximumSize(Math.max(1, maxSize))
      .expireAfterWrite(Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)))
      .recordStats()
      .build();
  }

  @Override
  public Optional<PlanContext> get(String planId) {
    PlanContext ctx = cache.getIfPresent(planId);
    if (ctx == null) return Optional.empty();

    long now = clock.millis();
    if (now > ctx.getExpiresAtMs() && ctx.getState() == PlanContext.State.ACTIVE) {
      long ver = versionGen.incrementAndGet();
      PlanContext expired = ctx.asExpired(ver);
      cache.put(planId, expired);
      return Optional.of(expired);
    }
    return Optional.of(ctx);
  }

  @Override
  public void put(PlanContext ctx) {
    cache.asMap().compute(ctx.getPlanId(), (k, existing) -> existing != null ? existing : ctx);
  }

  @Override
  public Optional<PlanContext> extendLease(String planId, long requestedExpiresAtMs) {
    final long now = clock.millis();
    return Optional.ofNullable(cache.asMap().computeIfPresent(planId, (k, cur) -> {
      if (cur.getState() != PlanContext.State.ACTIVE) return cur;
      long newExp = Math.max(cur.getExpiresAtMs(), Math.max(now, requestedExpiresAtMs));
      if (newExp == cur.getExpiresAtMs()) return cur;
      return cur.extendLease(newExp, versionGen.incrementAndGet());
    }));
  }

  @Override
  public Optional<PlanContext> end(String planId, boolean commit) {
    final long newExp = clock.millis() + endedGraceMs;
    return Optional.ofNullable(cache.asMap().computeIfPresent(planId, (k, cur) -> {
      if (cur.getState() == PlanContext.State.ENDED_ABORT || cur.getState() == PlanContext.State.ENDED_COMMIT) return cur;
      return cur.end(commit, newExp, versionGen.incrementAndGet());
    }));
  }

  @Override
  public boolean delete(String planId) {
    return cache.asMap().remove(planId) != null;
  }

  @Override
  public long size() {
    return cache.estimatedSize();
  }

  @PreDestroy
  @Override
  public void close() {
    cache.invalidateAll();
  }
}