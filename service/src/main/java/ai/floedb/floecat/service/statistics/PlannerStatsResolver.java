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

package ai.floedb.floecat.service.statistics;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

/**
 * Planner-facing STORE resolution for stats: generation ordering, per-target completeness matching,
 * the pinned → newest → stale fallback ladder, the planner stats cache with its generation-scoped
 * keyspaces and invalidation, and the per-batch lookup diagnostics.
 *
 * <p>Invariants this class maintains:
 *
 * <ul>
 *   <li>the pinned generation is the primary source (query-consistent); when the pin froze no
 *       generation the live/newest generation is primary;
 *   <li>the newest (live active) generation is consulted only as same-snapshot gap-fill — richer
 *       stats for identical data, never weakened snapshot consistency;
 *   <li>a partial record is still served (the planner degrades per stat) and never falls through to
 *       stale or capture;
 *   <li>hits are cached under the generation actually served; records are cached whole and
 *       completeness is re-judged per read, so one query's lesser need never masks another's richer
 *       need.
 * </ul>
 *
 * <p>Capture policy (bounded sync capture, async enqueue) stays in {@link StatsOrchestrator}, which
 * owns this resolver and runs it as the store rungs of the planner ladder.
 */
final class PlannerStatsResolver {

  private static final Logger LOG = Logger.getLogger(PlannerStatsResolver.class);
  // Same tags as StatsOrchestrator so the extraction does not move any dashboard series.
  private static final String COMPONENT = "service";
  private static final String OPERATION = "stats_orchestrator";

  /**
   * Fully-qualified cache key for a single column-stats record.
   *
   * <p>All four fields are required to guarantee no cross-contamination:
   *
   * <ul>
   *   <li>{@code accountId} — isolates tenants.
   *   <li>{@code tableId} — isolates tables within an account.
   *   <li>{@code snapshotId} — isolates snapshots; new stats captures produce new snapshot IDs, so
   *       stale entries from old snapshots are never served for new-snapshot queries.
   *   <li>{@code storageId} — the {@link ai.floedb.floecat.stats.identity.StatsTargetIdentity}
   *       storage key that uniquely identifies the target (column, table-level, file, etc.) within
   *       a snapshot.
   * </ul>
   */
  private record StatsCacheKey(
      String accountId, String tableId, long snapshotId, String generationToken, String storageId) {
    private StatsCacheKey {
      generationToken = generationToken == null ? "" : generationToken;
    }
  }

  /**
   * Maximum total byte weight of the stats cache.
   *
   * <p>256 MB accommodates roughly:
   *
   * <ul>
   *   <li>~200,000 scalar-only records at ~1 KB each (typical planner path today), or
   *   <li>~2,500 sketch-bearing records at ~100 KB each (theta k=4096 + tuple v2).
   * </ul>
   *
   * <p>Using weight rather than entry count prevents OOM when sketch-carrying {@link
   * TargetStatsRecord}s are common — a record with k=4096 theta + tuple sketches is ~100× larger
   * than a scalar record, so a count-only cap of 500,000 could allow ~50 GB of in-memory sketch
   * data if every entry held full sketch payloads.
   */
  private static final long STATS_CACHE_MAX_WEIGHT_BYTES = 256L * 1024 * 1024; // 256 MB

  /**
   * Weigher: approximate JVM heap cost per cache entry.
   *
   * <p>Uses {@link com.google.protobuf.AbstractMessage#getSerializedSize()} as a proxy for the
   * proto object's heap footprint. Proto objects typically occupy 1–2× their wire size in heap
   * (field objects, repeated-field lists, etc.), so the true heap usage may be larger; the weight
   * budget accounts for this by being set conservatively. A small fixed overhead (64 B) is added
   * per entry for cache bookkeeping and the key object.
   *
   * <p>Caffeine requires the weight to fit in an {@code int}. Records larger than {@link
   * Integer#MAX_VALUE} bytes are treated as maximum weight, effectively evicting them immediately —
   * an acceptable safety valve for pathologically large records.
   */
  private static int cacheWeight(StatsCacheKey key, TargetStatsRecord record) {
    long size = record.getSerializedSize() + 64L; // +64 for key object + cache overhead
    return (int) Math.min(size, Integer.MAX_VALUE);
  }

  /**
   * Application-scoped, snapshot-safe stats cache, bounded by byte weight.
   *
   * <p>Lifetime: the JVM process (survives all queries and connections).
   *
   * <p>Invalidation: snapshot IDs isolate normal new-snapshot captures, but full-rescan/finalize
   * paths can replace stats for the same snapshot ID. Successful mutation paths must explicitly
   * invalidate affected entries through this resolver; TTL is only a backstop for memory and missed
   * invalidation bugs.
   *
   * <p>Only positive hits (present records) are cached. Absent results are not cached because they
   * may be under active synchronous capture and could appear within seconds.
   *
   * <p>Bounded by {@link #STATS_CACHE_MAX_WEIGHT_BYTES} rather than entry count: a single
   * sketch-bearing record can be ~100 KB while a scalar record is ~1 KB; a count-only cap would
   * allow unbounded memory growth as sketch payloads become common.
   */
  private final Cache<StatsCacheKey, TargetStatsRecord> statsCache =
      Caffeine.newBuilder()
          .maximumWeight(STATS_CACHE_MAX_WEIGHT_BYTES)
          .weigher((Weigher<StatsCacheKey, TargetStatsRecord>) PlannerStatsResolver::cacheWeight)
          .expireAfterWrite(10, TimeUnit.MINUTES) // Backstop; mutation paths invalidate eagerly.
          .build();

  private final StatsStore statsStore;
  private final Observability observability;
  private final Consumer<StatsSyncOutcome> hitObserver;

  /**
   * @param statsStore the persisted stats store all rungs read from
   * @param observability metric sink; may be null (tests), in which case counters no-op
   * @param hitObserver invoked once per store/cache hit so the orchestrator's sync-outcome
   *     telemetry stays the single owner of that counter
   */
  PlannerStatsResolver(
      StatsStore statsStore, Observability observability, Consumer<StatsSyncOutcome> hitObserver) {
    this.statsStore = statsStore;
    this.observability = observability;
    this.hitObserver = hitObserver;
  }

  /**
   * Outcome of the store rungs for one planner batch: the results served so far ({@code resolved},
   * in ladder-insertion order), the targets no store rung could serve ({@code stillMissing}, ready
   * for the orchestrator's capture rung), the batch diagnostics the capture rung records onto and
   * the orchestrator emits exactly once, and the normalized pinned-generation token ({@code ""}
   * when the pin froze none).
   */
  record Resolution(
      Map<String, StatsResolutionResult> resolved,
      List<StatsCaptureRequest> stillMissing,
      PlannerLookupDiagnostics diagnostics,
      String pinnedGeneration) {}

  /**
   * Store rungs of the planner batch ladder — cache, pinned/primary generation, newest gap-fill,
   * stale — leaving capture to the caller. Callers guarantee a non-empty batch sharing one
   * table/snapshot; the full ladder contract is documented on {@link
   * StatsOrchestrator#resolvePlannerBatchInGeneration(java.util.List, Optional, java.util.Map,
   * boolean, long)}.
   */
  Resolution resolveFromStore(
      List<StatsCaptureRequest> requests,
      Optional<String> pinnedGenerationToken,
      Map<String, Predicate<TargetStatsRecord>> completenessByStorageId,
      boolean staleOk) {
    // All requests must share the same tableId and snapshotId (grouped upstream by TableWork).
    StatsCaptureRequest first = requests.get(0);
    String pinnedGeneration = pinnedGenerationToken.filter(token -> !token.isBlank()).orElse("");
    java.util.function.Function<String, java.util.function.Predicate<TargetStatsRecord>>
        completenessFor = key -> completenessByStorageId.getOrDefault(key, record -> true);
    PlannerLookupDiagnostics diagnostics = new PlannerLookupDiagnostics();
    // Primary keyspace/reader: the pinned generation when the pin froze one, else the live/newest
    // generation (empty token). A new snapshotId is always a cache miss, so stats from a different
    // snapshot are never returned for the current query.
    String primaryToken = pinnedGeneration;
    TargetBatchReader primaryBatch =
        pinnedGeneration.isBlank()
            ? statsStore::getTargetStatsBatch
            : (tableId, snapshotId, targets) ->
                statsStore.getTargetStatsBatchInGeneration(
                    tableId, snapshotId, pinnedGeneration, targets);
    TargetReader primaryTarget =
        pinnedGeneration.isBlank()
            ? statsStore::getTargetStats
            : (tableId, snapshotId, target) ->
                statsStore.getTargetStatsInGeneration(
                    tableId, snapshotId, pinnedGeneration, target);

    // 1. Cache check in the primary keyspace. A cached record that fails its completeness
    // predicate reads as a miss — the store may hold an enriched generation this query can use —
    // but is NOT evicted: it still satisfies lesser needs, and records are immutable per
    // generation so re-reads converge on the same bytes.
    java.util.Map<String, StatsResolutionResult> out =
        new java.util.LinkedHashMap<>(requests.size());
    java.util.List<StatsCaptureRequest> cacheMisses = new java.util.ArrayList<>();
    // Cached-but-incomplete records from the primary keyspace. Each is itself a valid partial hit
    // (the planner degrades per stat), kept as a last-resort candidate so that if the store re-read
    // for a richer record fails — an unreadable frozen manifest — or comes back empty, an
    // incomplete
    // PINNED record still serves rather than degrading to stale/capture, honouring the ladder's
    // "a partial pinned record is a hit, never a stale/capture trigger" invariant.
    java.util.Map<String, TargetStatsRecord> cachedPartial = new java.util.LinkedHashMap<>();
    for (StatsCaptureRequest req : requests) {
      String key = storageId(req);
      TargetStatsRecord cached = statsCache.getIfPresent(cacheKeyFor(req, primaryToken));
      if (cached != null && completenessFor.apply(key).test(cached)) {
        hitObserver.accept(StatsSyncOutcome.HIT);
        diagnostics.record(PlannerLookupOutcome.CACHE_HIT);
        out.put(key, StatsResolutionResult.hit(cached));
      } else {
        if (cached != null) {
          // Present but incomplete for THIS need: not a hit here, but a valid degraded fallback.
          cachedPartial.put(key, cached);
        }
        cacheMisses.add(req);
      }
    }

    if (cacheMisses.isEmpty()) {
      return new Resolution(out, List.of(), diagnostics, pinnedGeneration);
    }

    // 2. Primary store read: the pinned generation (query-consistent), or live/newest if no pin.
    java.util.Map<String, StatsResolutionResult> primaryHits =
        readPlannerBatchIsolated(
            first.tableId(),
            first.snapshotId(),
            cacheMisses,
            primaryBatch,
            primaryTarget,
            StatsResolutionResult::hit);

    // Pinned records that exist but fail their completeness predicate: candidates the newest
    // gap-fill may replace, and the answer of last resort if it cannot — a partial record is
    // still a hit (the planner degrades per stat), never a stale/capture trigger.
    java.util.Map<String, TargetStatsRecord> partialPinned = new java.util.LinkedHashMap<>();
    java.util.List<StatsCaptureRequest> misses = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : cacheMisses) {
      String key = storageId(req);
      StatsResolutionResult hit = primaryHits.get(key);
      if (hit != null && hit.hasStats()) {
        TargetStatsRecord record = hit.stats().get();
        boolean satisfies = completenessFor.apply(key).test(record);
        if (satisfies || pinnedGeneration.isBlank()) {
          // Complete — or partial with no pin, where the primary IS the newest generation and no
          // richer same-snapshot source exists: serve as-is, the planner degrades per stat.
          servePlannerHit(
              out,
              diagnostics,
              req,
              key,
              record,
              primaryToken,
              satisfies ? PlannerLookupOutcome.PRIMARY_HIT : PlannerLookupOutcome.PARTIAL);
        } else {
          partialPinned.put(key, record);
          misses.add(req);
        }
      } else if (hit != null && hit.outcome() == StatsSyncOutcome.FAILED) {
        if (pinnedGeneration.isBlank()) {
          diagnostics.record(PlannerLookupOutcome.FAILED);
          out.put(key, hit);
        } else {
          // A pinned-generation read failure (e.g. an unreadable frozen manifest) must not zero
          // planning quality for the batch: the newest generation of the same snapshot is an
          // independent read path with no frozen manifest involved, so fall through to the
          // gap-fill. If that fails too, ITS failure is the terminal one.
          misses.add(req);
        }
      } else {
        misses.add(req);
      }
    }

    // A miss whose pinned store read failed or returned nothing, but for which we hold an
    // incomplete cached record, keeps that cached record as its partial candidate: the store
    // yielded no (fresher) partial to prefer, so the cached one is the last-resort hit that keeps
    // an incomplete pinned record off the stale/capture path. Store-derived partials already in
    // partialPinned win — they are read from the same pinned generation, only fresher.
    for (StatsCaptureRequest req : misses) {
      String key = storageId(req);
      if (!partialPinned.containsKey(key) && cachedPartial.containsKey(key)) {
        partialPinned.put(key, cachedPartial.get(key));
      }
    }

    // 3. Newest gap-fill — only when a specific generation was pinned. Serves targets the pinned
    // generation lacks (prevents NOT_FOUND) or holds only partially (prevents a needless
    // downgrade), from the newest generation of the SAME snapshot; cached under the empty token.
    java.util.List<StatsCaptureRequest> afterFill = new java.util.ArrayList<>();
    if (!pinnedGeneration.isBlank() && !misses.isEmpty()) {
      java.util.Map<String, StatsResolutionResult> fillHits =
          readPlannerBatchIsolated(
              first.tableId(),
              first.snapshotId(),
              misses,
              statsStore::getTargetStatsBatch,
              statsStore::getTargetStats,
              StatsResolutionResult::hit);
      for (StatsCaptureRequest req : misses) {
        String key = storageId(req);
        StatsResolutionResult hit = fillHits.get(key);
        TargetStatsRecord partial = partialPinned.get(key);
        boolean newestSatisfies =
            hit != null && hit.hasStats() && completenessFor.apply(key).test(hit.stats().get());
        if (newestSatisfies) {
          servePlannerHit(
              out, diagnostics, req, key, hit.stats().get(), "", PlannerLookupOutcome.NEWEST_FILL);
        } else if (partial != null) {
          // Newest is no more complete than the pin: between equally incomplete records,
          // consistency prefers the pinned generation the scan reads.
          servePlannerHit(
              out, diagnostics, req, key, partial, primaryToken, PlannerLookupOutcome.PARTIAL);
        } else if (hit != null && hit.hasStats()) {
          // Pin has nothing at all; a partial newest record beats stale or capture.
          servePlannerHit(
              out, diagnostics, req, key, hit.stats().get(), "", PlannerLookupOutcome.PARTIAL);
        } else if (hit != null && hit.outcome() == StatsSyncOutcome.FAILED) {
          diagnostics.record(PlannerLookupOutcome.FAILED);
          out.put(key, hit);
        } else {
          afterFill.add(req);
        }
      }
    } else {
      afterFill.addAll(misses);
    }

    // 4. Stale fallback — BEFORE sync capture.
    // Single batch call: finds the latest snapshot with stats once (O(1) pointer read),
    // then fetches all missing targets from that snapshot in parallel. Replaces N×O(prefix scan).
    java.util.List<StatsCaptureRequest> stillMissing = new java.util.ArrayList<>();
    if (staleOk && !afterFill.isEmpty()) {
      java.util.Map<String, StatsResolutionResult> staleBatch =
          readPlannerBatchIsolated(
              first.tableId(),
              first.snapshotId(),
              afterFill,
              statsStore::getStaleTargetStatsBatch,
              statsStore::getStaleTargetStats,
              record -> StatsResolutionResult.staleHit(record, "stale_before_sync"));
      for (StatsCaptureRequest req : afterFill) {
        String key = storageId(req);
        StatsResolutionResult stale = staleBatch.get(key);
        if (stale != null && stale.hasStats()) {
          diagnostics.record(PlannerLookupOutcome.STALE_HIT);
          out.put(key, stale);
        } else if (stale != null && stale.outcome() == StatsSyncOutcome.FAILED) {
          diagnostics.record(PlannerLookupOutcome.FAILED);
          out.put(key, stale);
        } else {
          stillMissing.add(req);
        }
      }
    } else {
      stillMissing.addAll(afterFill);
    }

    return new Resolution(out, stillMissing, diagnostics, pinnedGeneration);
  }

  /**
   * Store rungs of the single-target planner lookup: the pinned generation for the pinned snapshot
   * (query-consistent; a pinned read failure falls through rather than failing the lookup), then
   * the newest (live active) generation only to fill a target the pinned generation lacks. Empty
   * means no store rung could serve; the caller decides whether to capture. Does not touch the
   * planner cache, matching the historical single-target path.
   */
  Optional<TargetStatsRecord> resolveSingleFromStore(
      StatsCaptureRequest request, Optional<String> pinnedGenerationToken) {
    String pinnedGeneration = pinnedGenerationToken.filter(token -> !token.isBlank()).orElse("");

    // Primary: the pinned generation (query-consistent), or live/newest when the pin froze none.
    Optional<TargetStatsRecord> primary;
    if (pinnedGeneration.isBlank()) {
      primary = readStore(request);
    } else {
      try {
        primary =
            statsStore.getTargetStatsInGeneration(
                request.tableId(), request.snapshotId(), pinnedGeneration, request.target());
      } catch (RuntimeException e) {
        // A pinned-generation read failure (e.g. an unreadable frozen manifest) must not fail the
        // lookup outright: the newest generation of the same snapshot is an independent read path
        // with no frozen manifest involved — treat the pin as a miss and let the gap-fill below
        // serve, matching the batch path's fallback.
        LOG.debugf(
            e,
            "pinned-generation read failed for %s; falling through to newest",
            storageId(request));
        primary = Optional.empty();
      }
    }
    if (primary.isPresent()) {
      return primary;
    }

    // Newest gap-fill — only when a specific generation was pinned; the pinned generation lacks
    // this target, so the newest generation backstops it before capture.
    if (!pinnedGeneration.isBlank()) {
      return readStore(request);
    }
    return Optional.empty();
  }

  /** Invalidates every cached target for one table snapshot. */
  void invalidateStatsCache(ResourceId tableId, long snapshotId) {
    if (tableId == null) {
      return;
    }
    statsCache
        .asMap()
        .keySet()
        .removeIf(
            key ->
                key.accountId().equals(tableId.getAccountId())
                    && key.tableId().equals(tableId.getId())
                    && key.snapshotId() == snapshotId);
  }

  /** Invalidates one cached target for one table snapshot. */
  void invalidateStatsCache(ResourceId tableId, long snapshotId, StatsTarget target) {
    if (tableId == null || target == null) {
      return;
    }
    String storageId = StatsTargetIdentity.storageId(target);
    statsCache
        .asMap()
        .keySet()
        .removeIf(
            key ->
                key.accountId().equals(tableId.getAccountId())
                    && key.tableId().equals(tableId.getId())
                    && key.snapshotId() == snapshotId
                    && key.storageId().equals(storageId));
  }

  /** Invalidates cached targets represented by successfully persisted records. */
  void invalidateStatsCache(ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    if (tableId == null || records == null || records.isEmpty()) {
      return;
    }
    java.util.Set<String> storageIds = new java.util.HashSet<>();
    for (TargetStatsRecord record : records) {
      if (record != null && record.hasTarget()) {
        storageIds.add(StatsTargetIdentity.storageId(record.getTarget()));
      }
    }
    if (storageIds.isEmpty()) {
      return;
    }
    // One O(cacheSize) pass matching any of the records' targets, not one full scan per record —
    // a wide-table recompute (hundreds of columns) would otherwise scan the cache once per column.
    statsCache
        .asMap()
        .keySet()
        .removeIf(
            key ->
                key.accountId().equals(tableId.getAccountId())
                    && key.tableId().equals(tableId.getId())
                    && key.snapshotId() == snapshotId
                    && storageIds.contains(key.storageId()));
  }

  /** Cache key for one request under a specific served-generation token ("" = live/newest). */
  private StatsCacheKey cacheKeyFor(StatsCaptureRequest req, String generationToken) {
    return new StatsCacheKey(
        req.tableId().getAccountId(),
        req.tableId().getId(),
        req.snapshotId(),
        generationToken,
        storageId(req));
  }

  /**
   * Serve one resolved planner target: write the record through to the cache keyspace it was served
   * from ({@code cacheToken} — the pinned generation's token, or "" for the live/newest
   * generation), count the ladder rung that produced it, and emit the hit. The single definition of
   * "serve" for every rung of {@link #resolveFromStore}, so caching, telemetry, and result emission
   * can never drift apart between rungs.
   */
  private void servePlannerHit(
      java.util.Map<String, StatsResolutionResult> out,
      PlannerLookupDiagnostics diagnostics,
      StatsCaptureRequest req,
      String key,
      TargetStatsRecord record,
      String cacheToken,
      PlannerLookupOutcome outcome) {
    statsCache.put(cacheKeyFor(req, cacheToken), record);
    hitObserver.accept(StatsSyncOutcome.HIT);
    diagnostics.record(outcome);
    out.put(key, StatsResolutionResult.hit(record));
  }

  private Map<String, StatsResolutionResult> readPlannerBatchIsolated(
      ResourceId tableId,
      long snapshotId,
      List<StatsCaptureRequest> requests,
      TargetBatchReader batchReader,
      TargetReader targetReader,
      Function<TargetStatsRecord, StatsResolutionResult> hitMapper) {
    if (requests == null || requests.isEmpty()) {
      return Map.of();
    }

    List<StatsTarget> targets = targetsOf(requests);
    try {
      Map<String, Optional<TargetStatsRecord>> batchHits =
          batchReader.read(tableId, snapshotId, targets);
      Map<String, StatsResolutionResult> out = new LinkedHashMap<>();
      for (StatsCaptureRequest request : requests) {
        String key = storageId(request);
        Optional<TargetStatsRecord> hit = batchHits == null ? Optional.empty() : batchHits.get(key);
        out.put(
            key,
            hit != null && hit.isPresent()
                ? hitMapper.apply(hit.get())
                : StatsResolutionResult.skipped("batch_store_miss"));
      }
      return java.util.Collections.unmodifiableMap(out);
    } catch (RuntimeException batchError) {
      if (requests.size() == 1) {
        return readPlannerTargetIsolated(
            tableId, snapshotId, requests.get(0), targetReader, hitMapper, batchError);
      }
      int mid = requests.size() / 2;
      Map<String, StatsResolutionResult> out = new LinkedHashMap<>();
      out.putAll(
          readPlannerBatchIsolated(
              tableId, snapshotId, requests.subList(0, mid), batchReader, targetReader, hitMapper));
      out.putAll(
          readPlannerBatchIsolated(
              tableId,
              snapshotId,
              requests.subList(mid, requests.size()),
              batchReader,
              targetReader,
              hitMapper));
      return java.util.Collections.unmodifiableMap(out);
    }
  }

  private Map<String, StatsResolutionResult> readPlannerTargetIsolated(
      ResourceId tableId,
      long snapshotId,
      StatsCaptureRequest request,
      TargetReader targetReader,
      Function<TargetStatsRecord, StatsResolutionResult> hitMapper,
      RuntimeException batchError) {
    String key = storageId(request);
    try {
      Optional<TargetStatsRecord> record = targetReader.read(tableId, snapshotId, request.target());
      return Map.of(
          key,
          record.map(hitMapper).orElseGet(() -> StatsResolutionResult.skipped("batch_store_miss")));
    } catch (RuntimeException targetError) {
      LOG.debugf(
          targetError,
          "planner stats target read failed after batch isolation table=%s target=%s",
          tableId,
          key);
      String message =
          targetError.getMessage() == null ? batchError.getMessage() : targetError.getMessage();
      return Map.of(key, StatsResolutionResult.failed(message));
    }
  }

  private static List<StatsTarget> targetsOf(List<StatsCaptureRequest> requests) {
    return requests.stream().map(StatsCaptureRequest::target).toList();
  }

  private static String storageId(StatsCaptureRequest request) {
    return StatsTargetIdentity.storageId(request.target());
  }

  private Optional<TargetStatsRecord> readStore(StatsCaptureRequest request) {
    // Fast path: authoritative persisted read.
    Optional<TargetStatsRecord> out =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target());
    incrementCounter(
        out.isPresent()
            ? ServiceMetrics.Stats.STORE_HITS_TOTAL
            : ServiceMetrics.Stats.STORE_MISSES_TOTAL,
        1,
        Tag.of(TagKey.SCOPE, "orchestrator"));
    return out;
  }

  @FunctionalInterface
  private interface TargetBatchReader {
    Map<String, Optional<TargetStatsRecord>> read(
        ResourceId tableId, long snapshotId, List<StatsTarget> targets);
  }

  @FunctionalInterface
  private interface TargetReader {
    Optional<TargetStatsRecord> read(ResourceId tableId, long snapshotId, StatsTarget target);
  }

  /**
   * The ladder rung that resolved (or failed) one planner target — the diagnostics vocabulary for
   * {@link StatsOrchestrator#resolvePlannerBatchInGeneration}. Emitted per target as a {@code
   * result}-tagged count of {@code PLANNER_LOOKUP_OUTCOMES_TOTAL} and summarized in one DEBUG line
   * per table batch, so "which rung served this query's stats" is a metric query or a log grep, not
   * archaeology.
   */
  enum PlannerLookupOutcome {
    /** Served from the cache keyspace of the primary generation. */
    CACHE_HIT,
    /** The primary generation (pinned, or live when unpinned) satisfied the need. */
    PRIMARY_HIT,
    /** The pinned generation lacked the target or capability; the newest generation satisfied. */
    NEWEST_FILL,
    /** Served a record that does not satisfy the full need (planner degrades per stat). */
    PARTIAL,
    /** No record at the pinned snapshot; served from an earlier snapshot. */
    STALE_HIT,
    /** Missing everywhere; a bounded sync capture produced the record. */
    CAPTURED,
    /** Missing everywhere; capture is pending (async mode, budget exhausted, or in flight). */
    CAPTURE_PENDING,
    /** A rung failed outright (store error surfaced as FAILED). */
    FAILED
  }

  /**
   * Per-batch outcome accumulator for the planner ladder: counts each target's outcome, then emits
   * them once — as {@code result}-tagged counter increments and a single DEBUG summary line. One
   * instance per {@link #resolveFromStore} call; never shared across threads.
   */
  final class PlannerLookupDiagnostics {
    private final java.util.EnumMap<PlannerLookupOutcome, Integer> counts =
        new java.util.EnumMap<>(PlannerLookupOutcome.class);

    void record(PlannerLookupOutcome outcome) {
      counts.merge(outcome, 1, Integer::sum);
    }

    void emit(ResourceId tableId, long snapshotId, String pinnedGeneration, int requested) {
      for (Map.Entry<PlannerLookupOutcome, Integer> entry : counts.entrySet()) {
        incrementCounter(
            ServiceMetrics.Stats.PLANNER_LOOKUP_OUTCOMES_TOTAL,
            entry.getValue(),
            Tag.of(TagKey.RESULT, entry.getKey().name().toLowerCase(java.util.Locale.ROOT)));
      }
      // The greppable per-batch summary: which generation the batch read and how each target
      // resolved. "generation=live" means the pin froze none and the live generation was primary.
      LOG.debugf(
          "planner_stats lookup table=%s snapshot=%d generation=%s requested=%d outcomes=%s",
          tableId.getId(),
          snapshotId,
          pinnedGeneration.isBlank() ? "live" : pinnedGeneration,
          requested,
          counts);
    }
  }

  /**
   * Counter with the orchestrator's component/operation tags; no-ops when observability is absent
   * (tests).
   */
  private void incrementCounter(MetricId metric, double amount, Tag... tags) {
    if (observability == null) {
      return;
    }
    Tag[] baseTags = {Tag.of(TagKey.COMPONENT, COMPONENT), Tag.of(TagKey.OPERATION, OPERATION)};
    Tag[] merged = new Tag[baseTags.length + tags.length];
    System.arraycopy(baseTags, 0, merged, 0, baseTags.length);
    System.arraycopy(tags, 0, merged, baseTags.length, tags.length);
    observability.counter(metric, amount, merged);
  }
}
