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

package ai.floedb.floecat.service.query.impl;

import ai.floedb.floecat.service.query.QueryContextStore;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Caffeine-backed in-memory implementation of QueryContextStore.
 *
 * <p>Notes: - put() inserts only when absent — does NOT overwrite. - replace() overwrites existing
 * contexts, required for DescribeInputs(). - expiration logic is applied eagerly under get().
 */
@ApplicationScoped
public class QueryContextStoreImpl implements QueryContextStore {

  @ConfigProperty(name = "floecat.query.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  @ConfigProperty(name = "floecat.query.ended-grace-ms", defaultValue = "15000")
  long endedGraceMs;

  @ConfigProperty(name = "floecat.query.max-size", defaultValue = "10000")
  long maxSize;

  @ConfigProperty(name = "floecat.query.safety-expiry-minutes", defaultValue = "10")
  long safetyExpiryMinutes;

  private final AtomicLong versionGen = new AtomicLong(1);
  private final Clock clock = Clock.systemUTC();

  private Cache<String, QueryContext> cache;

  @PostConstruct
  void init() {
    cache =
        Caffeine.newBuilder()
            .maximumSize(Math.max(1, maxSize))
            .expireAfterWrite(Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)))
            .recordStats()
            .build();
  }

  @Override
  public Optional<QueryContext> get(String queryId) {
    QueryContext ctx = cache.getIfPresent(queryId);
    if (ctx == null) {
      return Optional.empty();
    }

    long now = clock.millis();
    if (now > ctx.getExpiresAtMs() && ctx.getState() == QueryContext.State.ACTIVE) {
      long ver = versionGen.incrementAndGet();
      QueryContext expired = ctx.asExpired(ver);
      cache.put(queryId, expired);
      return Optional.of(expired);
    }

    return Optional.of(ctx);
  }

  @Override
  public void put(QueryContext ctx) {
    // Insert only if absent
    cache.asMap().compute(ctx.getQueryId(), (k, existing) -> existing != null ? existing : ctx);
  }

  @Override
  public Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs) {
    final long now = clock.millis();

    return Optional.ofNullable(
        cache
            .asMap()
            .computeIfPresent(
                queryId,
                (k, ctx) -> {
                  if (ctx.getState() != QueryContext.State.ACTIVE) {
                    return ctx;
                  }

                  long newExp = Math.max(ctx.getExpiresAtMs(), Math.max(now, requestedExpiresAtMs));
                  if (newExp == ctx.getExpiresAtMs()) {
                    return ctx;
                  }

                  return ctx.extendLease(newExp, versionGen.incrementAndGet());
                }));
  }

  @Override
  public Optional<QueryContext> end(String queryId, boolean commit) {
    final long newExp = clock.millis() + endedGraceMs;

    return Optional.ofNullable(
        cache
            .asMap()
            .computeIfPresent(
                queryId,
                (k, ctx) -> {
                  if (ctx.getState() == QueryContext.State.ENDED_ABORT
                      || ctx.getState() == QueryContext.State.ENDED_COMMIT) {
                    return ctx;
                  }

                  return ctx.end(commit, newExp, versionGen.incrementAndGet());
                }));
  }

  @Override
  public boolean delete(String queryId) {
    return cache.asMap().remove(queryId) != null;
  }

  @Override
  public long size() {
    return cache.estimatedSize();
  }

  /**
   * Overwrite an existing QueryContext with a new version.
   *
   * <p>This is used by DescribeInputs(), GetCatalogBundle(), etc., to store metadata filled later
   * in the query lifecycle.
   */
  @Override
  public void replace(QueryContext ctx) {
    cache.asMap().put(ctx.getQueryId(), ctx);
  }

  @Override
  public Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
    return Optional.ofNullable(
        cache
            .asMap()
            .computeIfPresent(
                queryId,
                (k, ctx) -> {
                  QueryContext updated = fn.apply(ctx);
                  if (updated == null || updated == ctx) {
                    return ctx;
                  }
                  return updated.toBuilder().version(versionGen.incrementAndGet()).build();
                }));
  }

  @PreDestroy
  @Override
  public void close() {
    cache.invalidateAll();
  }
}
