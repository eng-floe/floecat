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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.cache.CaffeineStateCache;
import ai.floedb.floecat.cache.StateCache;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import com.github.benmanes.caffeine.cache.RemovalCause;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
  // Package-private and non-final so unit tests can substitute a controllable clock. Production
  // uses the system clock.
  Clock clock = Clock.systemUTC();

  private StateCache<String, QueryContext> cache;
  private StateCache<String, ScanSession> scanSessionCache;
  private final Map<String, String> handleToQuery = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    cache =
        CaffeineStateCache.<String, QueryContext>builder()
            // Query contexts are an internal optimization. Snapshot retention is the safety
            // boundary, so evicting an active context does not make immutable data collectible
            // earlier or change the correctness contract.
            .maximumWeight(Math.max(1, maxSize))
            .weigher((String k, QueryContext ctx) -> ctx != null && ctx.isActive() ? 0 : 1)
            .expireAfterWrite(Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)))
            .recordStats()
            .removalListener(
                (String key, QueryContext ctx, RemovalCause cause) -> {
                  if (ctx != null) {
                    cleanupScanHandles(ctx);
                  }
                })
            .build();
    scanSessionCache =
        CaffeineStateCache.<String, ScanSession>builder()
            .maximumSize(Math.max(1, maxSize))
            .expireAfterWrite(Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)))
            .recordStats()
            .removalListener(
                (String handle, ScanSession session, RemovalCause cause) -> {
                  if (handle != null) {
                    handleToQuery.remove(handle);
                  }
                })
            .build();
  }

  @Override
  public Optional<QueryContext> get(String queryId) {
    // The lazy ACTIVE→EXPIRED transition must be atomic with respect to concurrent writers. A
    // read-then-put (as an earlier version did) could overwrite a context that update() had just
    // committed resolved selections into with a stale pre-resolution version marked EXPIRED.
    QueryContext ctx =
        cache.computeIfPresent(
            queryId,
            (k, cur) -> {
              if (clock.millis() > cur.getExpiresAtMs()
                  && cur.getState() == QueryContext.State.ACTIVE) {
                return cur.asExpired(versionGen.incrementAndGet());
              }
              return cur;
            });
    return Optional.ofNullable(ctx);
  }

  @Override
  public void put(QueryContext ctx) {
    putIfAbsent(ctx);
  }

  @Override
  public boolean putIfAbsent(QueryContext ctx) {
    boolean inserted = cache.putIfAbsent(ctx.getQueryId(), ctx) == null;
    return inserted;
  }

  @Override
  public Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs) {
    final long now = clock.millis();

    QueryContext updated =
        cache.computeIfPresent(
            queryId,
            (k, ctx) -> {
              if (ctx.getState() != QueryContext.State.ACTIVE) {
                // Leave a lazily-EXPIRED / ended context in place; the method returns empty
                // below.
                return ctx;
              }

              long newExp = Math.max(ctx.getExpiresAtMs(), Math.max(now, requestedExpiresAtMs));
              if (newExp == ctx.getExpiresAtMs()) {
                return ctx;
              }

              return ctx.extendLease(newExp, versionGen.incrementAndGet());
            });
    // A renew against a non-ACTIVE (lazily-EXPIRED) context must surface as NOT_FOUND, not a false
    // success carrying the stale lease — the caller treats empty as not-found.
    if (updated == null || updated.getState() != QueryContext.State.ACTIVE) {
      return Optional.empty();
    }
    return Optional.of(updated);
  }

  @Override
  public Optional<QueryContext> end(String queryId, boolean commit) {
    final long newExp = clock.millis() + endedGraceMs;

    return Optional.ofNullable(
        cache.computeIfPresent(
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
    QueryContext ctx = cache.remove(queryId);
    if (ctx != null) {
      cleanupScanHandles(ctx);
    }
    return ctx != null;
  }

  @Override
  public long size() {
    return cache.estimatedSize();
  }

  /**
   * Overwrite an existing QueryContext with a new version.
   *
   * <p>This is used by DescribeInputs(), GetUserObjects(), etc., to store metadata filled later in
   * the query lifecycle.
   */
  @Override
  public void replace(QueryContext ctx) {
    cache.put(ctx.getQueryId(), ctx);
  }

  @Override
  public Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
    Optional<QueryContext> result =
        Optional.ofNullable(
            cache.computeIfPresent(
                queryId,
                (k, ctx) -> {
                  QueryContext updated = fn.apply(ctx);
                  if (updated == null || updated == ctx) {
                    return ctx;
                  }
                  return updated.toBuilder().version(versionGen.incrementAndGet()).build();
                }));
    return result;
  }

  @Override
  public ScanHandle createScanSession(String correlationId, ScanSession session) {
    String id = UUID.randomUUID().toString();
    ScanSession stored =
        ScanSession.builder()
            .handleId(id)
            .queryId(session.queryId())
            .tableId(session.tableId())
            .snapshotId(session.snapshotId())
            .statsGeneration(session.statsGeneration())
            .tableInfo(session.tableInfo())
            .requiredColumns(session.requiredColumns())
            .predicates(session.predicates())
            .includeColumnStats(session.includeColumnStats())
            .excludePartitionDataJson(session.excludePartitionDataJson())
            .targetBatchItems(session.targetBatchItems())
            .targetBatchBytes(session.targetBatchBytes())
            .build();
    // Install the bookkeeping BEFORE attaching the handle to the context, so the eviction
    // listener (which removes by these maps) can never fire on a handle that is in the context but
    // not yet in the maps — that ordering would leak a handleToQuery + scanSessionCache entry. If
    // the context is already gone, roll the bookkeeping back before failing.
    handleToQuery.put(id, session.queryId());
    scanSessionCache.put(id, stored);
    boolean present =
        update(
                session.queryId(),
                ctx -> ctx.toBuilder().scanHandles(addScanHandle(ctx.scanHandles(), id)).build())
            .isPresent();
    if (!present) {
      handleToQuery.remove(id);
      scanSessionCache.remove(id);
      throw GrpcErrors.notFound(
          correlationId, QUERY_NOT_FOUND, Map.of("query_id", session.queryId()));
    }
    return ScanHandle.newBuilder().setId(id).build();
  }

  @Override
  public Optional<ScanSession> getScanSession(ScanHandle handle) {
    if (handle == null) {
      return Optional.empty();
    }
    String queryId = handleToQuery.get(handle.getId());
    if (queryId == null) {
      return Optional.empty();
    }
    QueryContext ctx = cache.getIfPresent(queryId);
    if (ctx == null) {
      handleToQuery.remove(handle.getId());
      return Optional.empty();
    }
    return Optional.ofNullable(scanSessionCache.getIfPresent(handle.getId()));
  }

  @Override
  public void removeScanSession(ScanHandle handle) {
    if (handle == null) {
      return;
    }
    String queryId = handleToQuery.remove(handle.getId());
    if (queryId == null) {
      return;
    }
    cache.computeIfPresent(
        queryId,
        (k, ctx) -> {
          Set<String> updated = new HashSet<>(ctx.scanHandles());
          updated.remove(handle.getId());
          return ctx.toBuilder().scanHandles(updated).build();
        });
    scanSessionCache.remove(handle.getId());
  }

  private Set<String> addScanHandle(Set<String> existing, String id) {
    Set<String> updated = new HashSet<>(existing);
    updated.add(id);
    return Set.copyOf(updated);
  }

  private void cleanupScanHandles(QueryContext ctx) {
    for (String handle : ctx.scanHandles()) {
      handleToQuery.remove(handle);
      scanSessionCache.remove(handle);
    }
  }

  @PreDestroy
  @Override
  public void close() {
    cache.invalidateAll();
    scanSessionCache.invalidateAll();
  }
}
