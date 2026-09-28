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
 * Process-local QueryContextStore. put() inserts only when absent; update() changes a context
 * atomically; get() applies the lazy lease expiry. Active contexts are never evicted by size.
 */
@ApplicationScoped
public class QueryContextStoreImpl implements QueryContextStore {

  @ConfigProperty(name = "floecat.query.ended-grace-ms", defaultValue = "15000")
  long endedGraceMs;

  @ConfigProperty(name = "floecat.query.max-size", defaultValue = "10000")
  long maxSize;

  @ConfigProperty(name = "floecat.query.safety-expiry-minutes", defaultValue = "10")
  long safetyExpiryMinutes;

  private static final Duration LEASE_MARGIN = Duration.ofSeconds(5);

  /** Bounds the scans one active query can hold open. */
  static final int MAX_OPEN_SCANS_PER_QUERY = 1024;

  private final AtomicLong versionGen = new AtomicLong(1);
  // Package-private and non-final so unit tests can substitute a controllable clock. Production
  // uses the system clock.
  Clock clock = Clock.systemUTC();
  // Package-private so unit tests can run cache maintenance synchronously.
  java.util.concurrent.Executor cacheExecutor = java.util.concurrent.ForkJoinPool.commonPool();

  private StateCache<String, QueryContext> cache;
  // Scan sessions live exactly as long as their query: released when it ends, expires, or leaves
  // the cache, never bounded on their own.
  private final Map<String, ScanSession> scanSessions = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    cache =
        CaffeineStateCache.<String, QueryContext>builder()
            // Active contexts are required to serve follow-up query RPCs. Zero weight keeps the
            // configured bound applicable to terminal contexts without evicting a live query;
            // safetyExpiryMinutes remains the bound for abandoned active contexts.
            .maximumWeight(Math.max(1, maxSize))
            .weigher((String k, QueryContext ctx) -> ctx != null && ctx.isActive() ? 0 : 1)
            .expireAfterWrite(Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)))
            .executor(cacheExecutor)
            .removalListener(
                (String key, QueryContext ctx, RemovalCause cause) -> {
                  // A replaced context is the same query's next version, which keeps its handles.
                  if (ctx != null && cause != RemovalCause.REPLACED) {
                    cleanupScanHandles(ctx);
                  }
                })
            .build();
  }

  @Override
  public Optional<QueryContext> get(String queryId) {
    QueryContext ctx = cache.getIfPresent(queryId);
    if (ctx != null && ctx.isActive() && clock.millis() > ctx.getExpiresAtMs()) {
      // The lazy ACTIVE->EXPIRED transition is atomic with concurrent updates; a read that needs no
      // transition leaves the entry, and its write time, alone.
      ctx =
          cache.computeIfPresent(
              queryId,
              (k, cur) ->
                  cur.isActive() && clock.millis() > cur.getExpiresAtMs()
                      ? cur.asExpired(versionGen.incrementAndGet())
                      : cur);
    }
    if (ctx != null && !ctx.isActive()) {
      cleanupScanHandles(ctx);
    }
    return Optional.ofNullable(ctx);
  }

  @Override
  public long maxLeaseMs() {
    // Caffeine refreshes a context's write time only once a second has passed, so a lease stays a
    // margin inside the safety expiry.
    return Duration.ofMinutes(Math.max(1, safetyExpiryMinutes)).minus(LEASE_MARGIN).toMillis();
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

              if (now > ctx.getExpiresAtMs()) {
                return ctx.asExpired(versionGen.incrementAndGet());
              }

              long newExp = Math.max(ctx.getExpiresAtMs(), Math.max(now, requestedExpiresAtMs));
              if (newExp == ctx.getExpiresAtMs()) {
                return ctx;
              }

              return ctx.extendLease(newExp, versionGen.incrementAndGet());
            });
    // A renew against a non-ACTIVE (lazily-EXPIRED) context must surface as NOT_FOUND, not a false
    // success carrying the stale lease — the caller treats empty as not-found.
    if (updated == null) {
      return Optional.empty();
    }
    if (!updated.isActive()) {
      cleanupScanHandles(updated);
      return Optional.empty();
    }
    return Optional.of(updated);
  }

  @Override
  public Optional<QueryContext> end(String queryId, boolean commit) {
    final long newExp = clock.millis() + endedGraceMs;

    QueryContext ended =
        cache.computeIfPresent(
            queryId,
            (k, ctx) -> {
              if (ctx.getState() == QueryContext.State.ENDED_ABORT
                  || ctx.getState() == QueryContext.State.ENDED_COMMIT) {
                return ctx;
              }

              return ctx.end(commit, newExp, versionGen.incrementAndGet());
            });
    if (ended != null) {
      // An ended query serves no further scans.
      cleanupScanHandles(ended);
    }
    return Optional.ofNullable(ended);
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

  @Override
  public Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
    final long now = clock.millis();
    Optional<QueryContext> result =
        Optional.ofNullable(
            cache.computeIfPresent(
                queryId,
                (k, ctx) -> {
                  // Keep both lifecycle checks in the atomic update seam. Callers must not mutate
                  // a context that ended or expired after their preceding get().
                  if (!ctx.isActive()) {
                    return ctx;
                  }
                  if (now > ctx.getExpiresAtMs()) {
                    return ctx.asExpired(versionGen.incrementAndGet());
                  }
                  QueryContext updated = fn.apply(ctx);
                  if (updated == null || updated == ctx) {
                    return ctx;
                  }
                  return updated.toBuilder().version(versionGen.incrementAndGet()).build();
                }));
    if (result.isPresent() && !result.orElseThrow().isActive()) {
      // Preserve the terminal context so callers can report QUERY_NOT_ACTIVE rather than
      // collapsing an ended or expired query into QUERY_NOT_FOUND. Its scan handles are no longer
      // usable, so release them as part of the transition.
      cleanupScanHandles(result.orElseThrow());
    }
    return result;
  }

  @Override
  public ScanHandle createScanSession(String correlationId, ScanSession session) {
    String id = UUID.randomUUID().toString();
    // Register the session before attaching it, so a concurrent end or eviction that cleans the
    // context's handles can always find it; only an active query takes a new scan.
    scanSessions.put(id, session.withHandleId(id));
    // Why the attach was refused, or null once attached.
    io.grpc.StatusRuntimeException[] refused = {
      GrpcErrors.notFound(correlationId, QUERY_NOT_FOUND, Map.of("query_id", session.queryId()))
    };
    Optional<QueryContext> updated =
        update(
            session.queryId(),
            ctx -> {
              if (ctx.scanHandles().size() >= MAX_OPEN_SCANS_PER_QUERY) {
                refused[0] =
                    GrpcErrors.preconditionFailed(
                        correlationId,
                        QUERY_SCAN_LIMIT,
                        Map.of(
                            "query_id",
                            session.queryId(),
                            "limit",
                            Integer.toString(MAX_OPEN_SCANS_PER_QUERY)));
                return ctx;
              }
              refused[0] = null;
              return ctx.toBuilder().scanHandles(addScanHandle(ctx.scanHandles(), id)).build();
            });
    if (updated.isPresent() && !updated.orElseThrow().isActive()) {
      refused[0] =
          GrpcErrors.preconditionFailed(
              correlationId, QUERY_NOT_ACTIVE, Map.of("query_id", session.queryId()));
    }
    if (refused[0] != null) {
      scanSessions.remove(id);
      throw refused[0];
    }
    return ScanHandle.newBuilder().setId(id).build();
  }

  @Override
  public Optional<ScanSession> getScanSession(ScanHandle handle) {
    ScanSession session = handle == null ? null : scanSessions.get(handle.getId());
    if (session == null) {
      return Optional.empty();
    }
    // Through get() so an elapsed lease expires the query, releasing its scans.
    QueryContext ctx = get(session.queryId()).orElse(null);
    if (ctx == null || !ctx.isActive()) {
      scanSessions.remove(handle.getId());
      return Optional.empty();
    }
    return Optional.of(session);
  }

  @Override
  public void removeScanSession(ScanHandle handle) {
    ScanSession session = handle == null ? null : scanSessions.remove(handle.getId());
    if (session == null) {
      return;
    }
    cache.computeIfPresent(
        session.queryId(),
        (k, ctx) -> {
          Set<String> updated = new HashSet<>(ctx.scanHandles());
          updated.remove(handle.getId());
          return ctx.toBuilder().scanHandles(updated).build();
        });
  }

  private Set<String> addScanHandle(Set<String> existing, String id) {
    Set<String> updated = new HashSet<>(existing);
    updated.add(id);
    return Set.copyOf(updated);
  }

  int openScanSessions() {
    return scanSessions.size();
  }

  private void cleanupScanHandles(QueryContext ctx) {
    for (String handle : ctx.scanHandles()) {
      scanSessions.remove(handle);
    }
  }

  @PreDestroy
  @Override
  public void close() {
    cache.invalidateAll();
    scanSessions.clear();
  }
}
