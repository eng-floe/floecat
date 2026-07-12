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

import ai.floedb.floecat.query.rpc.RelationPin;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
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
  // Package-private and non-final so unit tests can substitute a controllable clock (e.g. to
  // exercise resolving-pin grace expiry deterministically). Production uses the system clock.
  Clock clock = Clock.systemUTC();

  private Cache<String, QueryContext> cache;
  private Cache<String, ScanSession> scanSessionCache;
  private final Map<String, String> handleToQuery = new ConcurrentHashMap<>();

  // Blobs of pins still being resolved, keyed by query id, held as transient GC roots from
  // construction until the RPC releases them (after the pins are persisted into a context, which
  // then roots them). Release is the primary bound; the grace is a fail-safe so a crashed/abandoned
  // resolution cannot leak roots, and must exceed the longest resolve→persist so an in-flight slow
  // RPC is never pruned before it releases. Pruned opportunistically on register (and on GC read),
  // so the map is self-maintaining regardless of the blob-GC schedule.
  @ConfigProperty(name = "floecat.query.resolving-pin-grace-ms", defaultValue = "600000")
  long resolvingPinGraceMs;

  private final Map<String, ResolvingPinBlobs> resolvingPinBlobs = new ConcurrentHashMap<>();

  private record ResolvingPinBlobs(Set<String> uris, long expiresAtMs) {}

  /** Test-only visibility into the resolving-root map size (bounds are behavior, not structure). */
  int resolvingPinEntryCount() {
    return resolvingPinBlobs.size();
  }

  @PostConstruct
  void init() {
    cache =
        Caffeine.newBuilder()
            // A committed context is a durable GC root: while a query is ACTIVE its pinned blobs
            // must survive, and CasBlobGc treats referencedPinBlobUris() as live roots. Size
            // eviction would silently unroot a live query's pins under load and let GC delete a
            // blob it is still reading. So ACTIVE contexts weigh 0 — they are bounded solely by
            // the safety-expiry TTL (the intended bound, releasing an abandoned query's pins);
            // only terminal contexts count toward the size cap.
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
        Caffeine.newBuilder()
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
    // committed pins into with a stale pre-pin version marked EXPIRED — losing the pins after
    // their transient resolving roots were already dropped, leaving the blobs rooted by neither.
    QueryContext ctx =
        cache
            .asMap()
            .computeIfPresent(
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
    boolean inserted =
        cache.asMap().compute(ctx.getQueryId(), (k, existing) -> existing != null ? existing : ctx)
            == ctx;
    if (inserted) {
      // The stored context now roots its pins; drop their transient resolving registrations.
      dropResolvingPinsRootedBy(ctx);
    }
    return inserted;
  }

  @Override
  public Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs) {
    final long now = clock.millis();

    QueryContext updated =
        cache
            .asMap()
            .computeIfPresent(
                queryId,
                (k, ctx) -> {
                  if (ctx.getState() != QueryContext.State.ACTIVE) {
                    // Leave a lazily-EXPIRED / ended context in place (removal is owned by the
                    // GC/pin lifecycle, not this read-shaped renew); the method returns empty
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
    QueryContext ctx = cache.asMap().remove(queryId);
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
  public Set<String> referencedPinBlobUris() {
    Set<String> uris = new HashSet<>();
    // Pins still being resolved (transient roots) plus the pins of every committed context. A blob
    // stays in the resolving set until well after it is persisted (the grace outlives the short
    // resolve→persist), so there is no handoff gap between the two sets and their order does not
    // matter. Expired resolving entries are pruned here, on the GC read path.
    long now = clock.millis();
    resolvingPinBlobs.values().removeIf(entry -> now > entry.expiresAtMs());
    for (ResolvingPinBlobs entry : resolvingPinBlobs.values()) {
      uris.addAll(entry.uris());
    }
    for (QueryContext ctx : cache.asMap().values()) {
      addPinBlobUris(uris, ctx);
    }
    // Live scan streams froze a stats generation at initScan and read its manifest blob per
    // generation-scoped page: the frozen manifest must survive the scan even after a replace-all
    // superseded it (the generation's record blobs stay pointer-rooted via retention).
    for (ScanSession session : scanSessionCache.asMap().values()) {
      String frozen = session.statsGeneration();
      if (frozen != null && !frozen.isEmpty()) {
        uris.add(frozen);
      }
    }
    return uris;
  }

  /**
   * Add every immutable blob URI a table pin references to {@code uris}: the pinned root (the
   * object all reads follow refs out of), plus the copied table/snapshot/constraints refs.
   */
  private static void addPinBlobUris(Set<String> uris, QueryContext ctx) {
    for (RelationPin relationPin : ctx.parseRelationPins("gc").getPinsList()) {
      if (relationPin.hasTablePin()) {
        uris.addAll(
            ai.floedb.floecat.service.query.QueryPins.gcRootUris(relationPin.getTablePin()));
      }
    }
  }

  @Override
  public void registerResolvingPinBlobs(String queryId, Collection<String> blobUris) {
    if (queryId == null || queryId.isEmpty() || blobUris == null) {
      return;
    }
    Set<String> clean = new HashSet<>();
    for (String uri : blobUris) {
      addIfPresent(clean, uri);
    }
    if (clean.isEmpty()) {
      return;
    }
    long now = clock.millis();
    // Expiry pruning normally happens on the GC read path (referencedPinBlobUris), which the blob
    // GC calls regularly — not per-pin here. But deployments can run with CAS blob GC disabled
    // (e.g. the reconciler-executor profile), and entries from queries that fail before any commit
    // are only ever released by expiry — so once the map holds more entries than there can be live
    // queries, fall back to pruning here to keep it bounded without a GC schedule.
    if (resolvingPinBlobs.size() >= maxSize) {
      resolvingPinBlobs.values().removeIf(entry -> now > entry.expiresAtMs());
    }
    long expiresAt = now + resolvingPinGraceMs;
    resolvingPinBlobs.merge(
        queryId,
        new ResolvingPinBlobs(Set.copyOf(clean), expiresAt),
        (existing, added) -> {
          Set<String> merged = new HashSet<>(existing.uris());
          merged.addAll(added.uris());
          return new ResolvingPinBlobs(Set.copyOf(merged), added.expiresAtMs());
        });
  }

  /**
   * Drop transient resolving-pin roots that are now rooted by a just-committed context, so the
   * lifecycle is bound to the pin commit rather than the fail-safe grace.
   *
   * <p>Only the committed context's <b>own</b> resolving registration is touched — the entry keyed
   * by its query id, which is exactly the key under which its pins were registered during
   * resolution. Keying on the query id (not the committing RPC's correlation id, which changes
   * across BeginQuery/DescribeInputs/GetUserObjects) is what makes register and release agree, so a
   * pin resolved on a later RPC is actually released on commit instead of lingering until the grace
   * expires. Dropping across <i>all</i> queries was unsafe: when two concurrent queries pin the
   * same physical blob (same table + snapshot), one query's commit would remove that shared URI
   * from the other's still-resolving registration; if the committing query then ended and expired
   * from the cache before the second query committed, the blob would be rooted by neither and
   * {@code CasBlobGc} could delete a blob still in use. Scoping the drop to the committing query's
   * own entry keeps every other in-flight query's registration intact.
   *
   * <p>URI-precise within that entry, so a not-yet-committed URI (e.g. a later streaming chunk)
   * stays protected, and the entry is dropped when it empties out.
   */
  @Override
  public void discardResolvingPins(QueryContext ctx) {
    // Same mechanism a successful insert uses, but for a context that will NOT be stored: release
    // exactly the resolving-pin blobs this query registered so a rejected BeginQuery (id collision)
    // does not hold them rooted until the grace expires.
    dropResolvingPinsRootedBy(ctx);
  }

  private void dropResolvingPinsRootedBy(QueryContext committed) {
    if (committed == null || resolvingPinBlobs.isEmpty()) {
      return;
    }
    String queryId = committed.getQueryId();
    if (queryId == null || queryId.isEmpty()) {
      // No query id to match the resolving registration against; leave the transient roots to
      // expire via the fail-safe grace rather than risk unrooting another query's shared blob.
      return;
    }
    Set<String> rooted = new HashSet<>();
    addPinBlobUris(rooted, committed);
    if (rooted.isEmpty()) {
      return;
    }
    resolvingPinBlobs.computeIfPresent(
        queryId,
        (cid, entry) -> {
          if (entry.uris().stream().noneMatch(rooted::contains)) {
            return entry;
          }
          Set<String> remaining = new HashSet<>(entry.uris());
          remaining.removeAll(rooted);
          return remaining.isEmpty()
              ? null
              : new ResolvingPinBlobs(Set.copyOf(remaining), entry.expiresAtMs());
        });
  }

  private static void addIfPresent(Set<String> uris, String uri) {
    if (uri != null && !uri.isBlank()) {
      uris.add(uri);
    }
  }

  /**
   * Overwrite an existing QueryContext with a new version.
   *
   * <p>This is used by DescribeInputs(), GetUserObjects(), etc., to store metadata filled later in
   * the query lifecycle.
   */
  @Override
  public void replace(QueryContext ctx) {
    cache.asMap().put(ctx.getQueryId(), ctx);
    dropResolvingPinsRootedBy(ctx);
  }

  @Override
  public Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
    Optional<QueryContext> result =
        Optional.ofNullable(
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
    // The updated context roots its pins; drop their transient resolving registrations.
    result.ifPresent(this::dropResolvingPinsRootedBy);
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
      scanSessionCache.invalidate(id);
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
    cache
        .asMap()
        .computeIfPresent(
            queryId,
            (k, ctx) -> {
              Set<String> updated = new HashSet<>(ctx.scanHandles());
              updated.remove(handle.getId());
              return ctx.toBuilder().scanHandles(updated).build();
            });
    scanSessionCache.invalidate(handle.getId());
  }

  private Set<String> addScanHandle(Set<String> existing, String id) {
    Set<String> updated = new HashSet<>(existing);
    updated.add(id);
    return Set.copyOf(updated);
  }

  private void cleanupScanHandles(QueryContext ctx) {
    for (String handle : ctx.scanHandles()) {
      handleToQuery.remove(handle);
      scanSessionCache.invalidate(handle);
    }
  }

  @PreDestroy
  @Override
  public void close() {
    cache.invalidateAll();
  }
}
