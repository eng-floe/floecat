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
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Central stats orchestration coordinator.
 *
 * <p>This class serves two roles:
 *
 * <ul>
 *   <li>query-time resolution policy via {@link #resolve(StatsCaptureRequest)}
 *   <li>explicit capture execution via {@link #triggerBatch(StatsCaptureBatchRequest)}
 * </ul>
 *
 * <p>Resolution is sync-first when {@code floecat.stats.sync.enabled=true} (default): on a store
 * miss the orchestrator attempts a bounded synchronous capture before falling back to async
 * enqueue. The outcome is encoded in {@link StatsResolutionResult} so callers can inspect quality.
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
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
      String accountId, String tableId, long snapshotId, String storageId) {}

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
   * invalidate affected entries through this orchestrator; TTL is only a backstop for memory and
   * missed invalidation bugs.
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
          .weigher((Weigher<StatsCacheKey, TargetStatsRecord>) StatsOrchestrator::cacheWeight)
          .expireAfterWrite(10, TimeUnit.MINUTES) // Backstop; mutation paths invalidate eagerly.
          .build();

  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final ConnectorRepository connectorRepository;
  private final StatsSyncCapture statsSyncCapture;
  private final boolean syncEnabled;
  private final ConcurrentMap<TableKey, String> lastEnqueuedJobByTable = new ConcurrentHashMap<>();
  private final Observability observability;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      ConnectorRepository connectorRepository,
      StatsSyncCapture statsSyncCapture,
      @ConfigProperty(name = "floecat.stats.sync.enabled", defaultValue = "true")
          boolean syncEnabled,
      Instance<Observability> observability) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.connectorRepository = connectorRepository;
    this.statsSyncCapture = statsSyncCapture;
    this.syncEnabled = syncEnabled;
    this.observability =
        observability == null || observability.isUnsatisfied() ? null : observability.get();
  }

  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      ConnectorRepository connectorRepository) {
    this(
        statsStore,
        reconcileJobStore,
        tableRepository,
        connectorRepository,
        new StatsSyncCapture(reconcileJobStore, connectorRepository),
        true,
        null);
  }

  /** Invalidates every cached target for one table snapshot. */
  public void invalidateStatsCache(ResourceId tableId, long snapshotId) {
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
  public void invalidateStatsCache(ResourceId tableId, long snapshotId, StatsTarget target) {
    if (tableId == null || target == null) {
      return;
    }
    statsCache.invalidate(
        new StatsCacheKey(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            StatsTargetIdentity.storageId(target)));
  }

  /** Invalidates cached targets represented by successfully persisted records. */
  public void invalidateStatsCache(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    if (records == null || records.isEmpty()) {
      return;
    }
    for (TargetStatsRecord record : records) {
      if (record != null && record.hasTarget()) {
        invalidateStatsCache(tableId, snapshotId, record.getTarget());
      }
    }
  }

  /**
   * Unified query-time resolution path.
   *
   * <p>Order: persisted store hit → bounded sync capture (if enabled and budget present) → async
   * enqueue fallback. The returned {@link StatsResolutionResult} always carries a {@link
   * StatsSyncOutcome} so callers can inspect quality without inspecting the Optional payload.
   */
  public StatsResolutionResult resolve(StatsCaptureRequest request) {
    long startNanos = System.nanoTime();
    Optional<TargetStatsRecord> stored = readStore(request);
    if (stored.isPresent()) {
      observeSyncOutcome(StatsSyncOutcome.HIT, startNanos);
      return StatsResolutionResult.hit(stored.get());
    }

    if (syncEnabled
        && request.executionMode() == StatsExecutionMode.SYNC
        && request.latencyBudget().isPresent()) {
      StatsSyncOutcome syncOutcome = attemptSyncCapture(request);
      observeSyncOutcome(syncOutcome, startNanos);

      if (syncOutcome == StatsSyncOutcome.CAPTURED) {
        Optional<TargetStatsRecord> afterCapture = readStore(request);
        if (afterCapture.isPresent()) {
          return StatsResolutionResult.captured(afterCapture.get());
        }
        // Capture succeeded per job store but record not yet visible — schedule follow-up.
        enqueueAsyncFollowUp(request, "sync_followup_partial");
        return StatsResolutionResult.partial(
            "sync capture succeeded but store record not visible; async follow-up enqueued");
      }

      String followUpReason =
          syncOutcome == StatsSyncOutcome.TIMEOUT
              ? "sync_followup_timeout"
              : "sync_followup_failed";
      enqueueAsyncFollowUp(request, followUpReason);
      String detail =
          syncOutcome == StatsSyncOutcome.TIMEOUT
              ? "sync capture timed out; async follow-up enqueued"
              : "sync capture failed; async follow-up enqueued";
      return syncOutcome == StatsSyncOutcome.TIMEOUT
          ? StatsResolutionResult.timeout(detail)
          : StatsResolutionResult.failed(detail);
    }

    enqueueAsyncCaptureBatch(List.of(request));
    String skipReason =
        request.executionMode() != StatsExecutionMode.SYNC
            ? "async_mode"
            : request.latencyBudget().isEmpty() ? "no_budget" : "sync_disabled";
    observeSyncOutcome(StatsSyncOutcome.SKIPPED, startNanos);
    return StatsResolutionResult.skipped(skipReason);
  }

  /** Returns the newest available stats at or before the requested snapshot, if any. */
  public Optional<TargetStatsRecord> resolveStale(StatsCaptureRequest request) {
    return statsStore.getStaleTargetStats(
        request.tableId(), request.snapshotId(), request.target());
  }

  /**
   * Planner-optimised batch resolution with <em>stale-before-sync</em> ordering.
   *
   * <p>For each request:
   *
   * <ol>
   *   <li>Batch read all targets from the store in a single call (eliminates N×1 store round-trips
   *       from the old per-target resolve loop).
   *   <li>Stale fallback for misses — if {@code staleOk}, check the stale store <em>before</em>
   *       attempting sync capture. This lets the planner use existing (possibly older) stats rather
   *       than blocking on a sync capture that may timeout.
   *   <li>Sync capture for targets still missing, per-target, while {@code deadlineNanos} allows.
   *   <li>Async enqueue for remaining misses.
   * </ol>
   *
   * @param requests one request per target, all for the same table/snapshot
   * @param staleOk whether to return stats from a prior snapshot when exact stats are absent
   * @param deadlineNanos absolute deadline (from {@link System#nanoTime()}) for sync captures
   * @return map from {@code StatsTargetIdentity.storageId} to resolution result, in request order
   */
  public java.util.Map<String, StatsResolutionResult> resolvePlannerBatch(
      java.util.List<StatsCaptureRequest> requests, boolean staleOk, long deadlineNanos) {
    if (requests == null || requests.isEmpty()) {
      return java.util.Map.of();
    }
    // All requests must share the same tableId and snapshotId (grouped upstream by TableWork).
    StatsCaptureRequest first = requests.get(0);

    // 1. Cache check — serve hits without touching DynamoDB.
    // Cache key includes (accountId, tableId, snapshotId, storageId): a new snapshotId is always
    // a cache miss, so stats from a different snapshot are never returned for the current query.
    java.util.Map<String, StatsResolutionResult> out =
        new java.util.LinkedHashMap<>(requests.size());
    java.util.List<StatsCaptureRequest> cacheMisses = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : requests) {
      String key = storageId(req);
      StatsCacheKey cacheKey =
          new StatsCacheKey(
              req.tableId().getAccountId(), req.tableId().getId(), req.snapshotId(), key);
      TargetStatsRecord cached = statsCache.getIfPresent(cacheKey);
      if (cached != null) {
        observeSyncOutcome(StatsSyncOutcome.HIT);
        out.put(key, StatsResolutionResult.hit(cached));
      } else {
        cacheMisses.add(req);
      }
    }

    if (cacheMisses.isEmpty()) {
      return java.util.Collections.unmodifiableMap(out);
    }

    // 2. Batch store read for cache misses only.
    java.util.Map<String, StatsResolutionResult> batchHits =
        readPlannerBatchIsolated(
            first.tableId(),
            first.snapshotId(),
            cacheMisses,
            statsStore::getTargetStatsBatch,
            statsStore::getTargetStats,
            StatsResolutionResult::hit);

    java.util.List<StatsCaptureRequest> misses = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : cacheMisses) {
      String key = storageId(req);
      StatsResolutionResult hit = batchHits.get(key);
      if (hit != null && hit.hasStats()) {
        // Write-through: only cache positive hits; absent results may appear soon via sync capture.
        StatsCacheKey cacheKey =
            new StatsCacheKey(
                req.tableId().getAccountId(), req.tableId().getId(), req.snapshotId(), key);
        statsCache.put(cacheKey, hit.stats().get());
        observeSyncOutcome(StatsSyncOutcome.HIT);
        out.put(key, StatsResolutionResult.hit(hit.stats().get()));
      } else if (hit != null && hit.outcome() == StatsSyncOutcome.FAILED) {
        out.put(key, hit);
      } else {
        misses.add(req);
      }
    }

    // 3. Stale fallback for misses — BEFORE sync capture.
    // Single batch call: finds the latest snapshot with stats once (O(1) pointer read),
    // then fetches all missing targets from that snapshot in parallel. Replaces N×O(prefix scan).
    java.util.List<StatsCaptureRequest> stillMissing = new java.util.ArrayList<>();
    if (staleOk && !misses.isEmpty()) {
      java.util.Map<String, StatsResolutionResult> staleBatch =
          readPlannerBatchIsolated(
              first.tableId(),
              first.snapshotId(),
              misses,
              statsStore::getStaleTargetStatsBatch,
              statsStore::getStaleTargetStats,
              record -> StatsResolutionResult.staleHit(record, "stale_before_sync"));
      for (StatsCaptureRequest req : misses) {
        String key = storageId(req);
        StatsResolutionResult stale = staleBatch.get(key);
        if (stale != null && stale.hasStats()) {
          out.put(key, stale);
        } else if (stale != null && stale.outcome() == StatsSyncOutcome.FAILED) {
          out.put(key, stale);
        } else {
          stillMissing.add(req);
        }
      }
    } else {
      stillMissing.addAll(misses);
    }

    // 4. Sync capture for still-missing targets (per-target within deadline).
    java.util.List<StatsCaptureRequest> asyncQueue = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : stillMissing) {
      String key = storageId(req);
      if (!syncEnabled || req.executionMode() != StatsExecutionMode.SYNC) {
        asyncQueue.add(req);
        out.put(key, StatsResolutionResult.skipped("async_mode"));
        continue;
      }
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        asyncQueue.add(req);
        out.put(key, StatsResolutionResult.timeout("latency_budget_exhausted"));
        continue;
      }
      StatsCaptureRequest withBudget =
          StatsCaptureRequest.builder(req.tableId(), req.snapshotId(), req.target())
              .columnSelectors(req.columnSelectors())
              .requestedKinds(req.requestedKinds())
              .executionMode(req.executionMode())
              .connectorType(req.connectorType())
              .latencyBudget(java.util.Optional.of(java.time.Duration.ofNanos(remainingNanos)))
              .samplingRequested(req.samplingRequested())
              .build();
      long startNanos = System.nanoTime();
      StatsSyncOutcome syncOutcome = attemptSyncCapture(withBudget);
      observeSyncOutcome(syncOutcome, startNanos);
      if (syncOutcome == StatsSyncOutcome.CAPTURED) {
        Optional<TargetStatsRecord> afterCapture = readStore(withBudget);
        if (afterCapture.isPresent()) {
          out.put(key, StatsResolutionResult.captured(afterCapture.get()));
        } else {
          asyncQueue.add(req);
          out.put(
              key,
              StatsResolutionResult.partial(
                  "sync capture succeeded but store record not visible; async follow-up enqueued"));
        }
      } else {
        asyncQueue.add(req);
        out.put(
            key,
            syncOutcome == StatsSyncOutcome.TIMEOUT
                ? StatsResolutionResult.timeout("sync capture timed out; async follow-up enqueued")
                : StatsResolutionResult.failed("sync capture failed; async follow-up enqueued"));
      }
    }

    // 5. Enqueue async captures for all misses that didn't sync.
    if (!asyncQueue.isEmpty()) {
      enqueueAsyncCaptureBatch(asyncQueue);
    }

    return java.util.Collections.unmodifiableMap(out);
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
   * Batch variant of {@link #resolve(StatsCaptureRequest)}.
   *
   * <p>Requests are resolved in-order with the same semantics as single-item resolve.
   */
  public List<StatsResolutionResult> resolveBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureRequest> requests = batchRequest.requests();
    incrementCounter(
        ServiceMetrics.Stats.BATCH_ITEMS_TOTAL,
        requests.size(),
        Tag.of(TagKey.SCOPE, "orchestrator"));
    List<StatsResolutionResult> resolved = new ArrayList<>(requests.size());
    ArrayList<StatsCaptureRequest> unresolvedForAsync = new ArrayList<>();

    for (StatsCaptureRequest request : requests) {
      Optional<TargetStatsRecord> stored = readStore(request);
      if (stored.isPresent()) {
        resolved.add(StatsResolutionResult.hit(stored.get()));
        continue;
      }
      // Sync is not attempted in batch mode — individual sync would serialize all requests.
      // Callers needing sync semantics should use the single-item resolve().
      unresolvedForAsync.add(request);
      resolved.add(StatsResolutionResult.skipped("batch_async_fallback"));
    }

    if (!unresolvedForAsync.isEmpty()) {
      incrementCounter(
          ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
          1,
          Tag.of(TagKey.TRIGGER, "async_followup"),
          Tag.of(TagKey.SCOPE, "orchestrator"));
      enqueueAsyncCaptureBatch(unresolvedForAsync);
    }
    return List.copyOf(resolved);
  }

  private StatsSyncOutcome attemptSyncCapture(StatsCaptureRequest request) {
    try {
      Optional<Table> table = tableRepository.getById(request.tableId());
      if (table.isEmpty()) {
        LOG.debugf("stats_sync_capture skipped: missing table=%s", request.tableId());
        return StatsSyncOutcome.FAILED;
      }
      if (!table.get().hasUpstream() || !table.get().getUpstream().hasConnectorId()) {
        LOG.debugf("stats_sync_capture skipped: no upstream connector table=%s", request.tableId());
        return StatsSyncOutcome.FAILED;
      }
      String connectorId = table.get().getUpstream().getConnectorId().getId();
      if (connectorId == null || connectorId.isBlank()) {
        LOG.debugf("stats_sync_capture skipped: blank connectorId table=%s", request.tableId());
        return StatsSyncOutcome.FAILED;
      }

      ReconcileScope.ScopedCaptureRequest scopedReq =
          new ReconcileScope.ScopedCaptureRequest(
              table.get().getResourceId().getId(),
              request.snapshotId(),
              ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.encode(request.target()),
              List.copyOf(request.columnSelectors()));
      ReconcileCapturePolicy policy = capturePolicyFor(List.of(request));
      ReconcileScope scope =
          ReconcileScope.of(
              List.of(), table.get().getResourceId().getId(), List.of(scopedReq), policy);

      return statsSyncCapture.capture(
          request.tableId().getAccountId(), connectorId, scope, request.latencyBudget().get());
    } catch (RuntimeException e) {
      LOG.warnf(e, "stats_sync_capture attempt threw for table=%s", request.tableId());
      return StatsSyncOutcome.FAILED;
    }
  }

  private void enqueueAsyncFollowUp(StatsCaptureRequest request, String reason) {
    LOG.debugf(
        "stats_async_followup reason=%s table=%s snapshot=%d",
        reason, request.tableId(), request.snapshotId());
    incrementCounter(
        ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
        1,
        Tag.of(TagKey.TRIGGER, reason),
        Tag.of(TagKey.SCOPE, "orchestrator"));
    enqueueAsyncCaptureBatch(List.of(request));
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

  private List<StatsCaptureBatchItemResult> enqueueAsyncCaptureBatch(
      List<StatsCaptureRequest> requests) {
    if (requests == null || requests.isEmpty()) {
      return List.of();
    }
    ArrayList<StatsCaptureBatchItemResult> results =
        new ArrayList<>(java.util.Collections.nCopies(requests.size(), null));
    Map<TableKey, List<IndexedRequest>> groupedByTable = new LinkedHashMap<>();
    for (int i = 0; i < requests.size(); i++) {
      StatsCaptureRequest request = requests.get(i);
      Optional<StatsCaptureBatchItemResult> unsupported = unsupportedTarget(request);
      if (unsupported.isPresent()) {
        results.set(i, unsupported.get());
        continue;
      }
      if (request.snapshotId() < 0L) {
        results.set(
            i,
            recordAsyncSkip(
                request,
                "invalid_snapshot_id",
                "Skipping async enqueue because snapshot id is invalid"));
        continue;
      }
      groupedByTable
          .computeIfAbsent(tableKey(request), ignored -> new ArrayList<>())
          .add(new IndexedRequest(i, toAsyncRequest(request)));
    }
    for (List<IndexedRequest> groupedRequests : groupedByTable.values()) {
      for (IndexedResult result : enqueueAsyncGroup(groupedRequests)) {
        results.set(result.index(), result.result());
      }
    }
    return List.copyOf(results);
  }

  private static Optional<StatsCaptureBatchItemResult> unsupportedTarget(
      StatsCaptureRequest request) {
    if (request == null || request.target() == null) {
      return Optional.empty();
    }
    return switch (request.target().getTargetCase()) {
      case FILE ->
          Optional.of(
              StatsCaptureBatchItemResult.uncapturable(
                  request,
                  "file targets are not valid unified capture execution requests; reconcile execution is file-group scoped"));
      case EXPRESSION ->
          Optional.of(
              StatsCaptureBatchItemResult.uncapturable(
                  request,
                  "expression targets are recognized but not yet implemented in unified capture"));
      case TARGET_NOT_SET ->
          Optional.of(
              StatsCaptureBatchItemResult.uncapturable(
                  request, "target is required for unified capture"));
      case COMPOSITE ->
          Optional.of(
              StatsCaptureBatchItemResult.uncapturable(
                  request, "composite column targets are not yet implemented in unified capture"));
      case TABLE, COLUMN -> Optional.empty();
    };
  }

  private List<IndexedResult> enqueueAsyncGroup(List<IndexedRequest> groupedRequests) {
    if (groupedRequests == null || groupedRequests.isEmpty()) {
      return List.of();
    }
    StatsCaptureRequest first = groupedRequests.getFirst().request();
    try {
      Optional<Table> table = tableRepository.getById(first.tableId());
      if (table.isEmpty()) {
        return recordAsyncSkipGroup(
            groupedRequests, "missing_table", "Skipping async enqueue because table lookup failed");
      }
      if (!table.get().hasUpstream()) {
        return recordAsyncSkipGroup(
            groupedRequests,
            "missing_upstream",
            "Skipping async enqueue because table has no upstream");
      }
      if (!table.get().getUpstream().hasConnectorId()) {
        return recordAsyncSkipGroup(
            groupedRequests,
            "missing_connector_id",
            "Skipping async enqueue because upstream connector id is missing");
      }
      if (table.get().getUpstream().getConnectorId().getId().isBlank()) {
        return recordAsyncSkipGroup(
            groupedRequests,
            "blank_connector_id",
            "Skipping async enqueue because upstream connector id is blank");
      }
      ResourceId connectorId =
          connectorResourceId(
              first.tableId().getAccountId(), table.get().getUpstream().getConnectorId().getId());
      if (!connectorRepository.existsById(connectorId)) {
        return recordAsyncSkipGroup(
            groupedRequests,
            "missing_connector",
            "Skipping async enqueue because canonical connector lookup failed");
      }
      LinkedHashSet<ReconcileScope.ScopedCaptureRequest> captureRequests = new LinkedHashSet<>();
      for (IndexedRequest indexedRequest : groupedRequests) {
        StatsCaptureRequest request = indexedRequest.request();
        captureRequests.add(
            new ReconcileScope.ScopedCaptureRequest(
                table.get().getResourceId().getId(),
                request.snapshotId(),
                ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.encode(request.target()),
                List.copyOf(request.columnSelectors())));
      }
      ReconcileCapturePolicy capturePolicy =
          capturePolicyFor(groupedRequests.stream().map(IndexedRequest::request).toList());
      ReconcileScope scope =
          ReconcileScope.of(
              List.of(),
              table.get().getResourceId().getId(),
              List.copyOf(captureRequests),
              capturePolicy);
      String jobId =
          reconcileJobStore.enqueue(
              first.tableId().getAccountId(),
              table.get().getUpstream().getConnectorId().getId(),
              false,
              ReconcilerService.CaptureMode.CAPTURE_ONLY,
              scope);
      lastEnqueuedJobByTable.put(tableKey(first), jobId);
      LOG.infof(
          "stats_enqueue outcome=QUEUED table=%s snapshots=%d targets=%d reason=%s group_size=%d job=%s",
          first.tableId(),
          (int)
              captureRequests.stream()
                  .map(ReconcileScope.ScopedCaptureRequest::snapshotId)
                  .distinct()
                  .count(),
          captureRequests.size(),
          "missing_or_degraded_sync_capture",
          groupedRequests.size(),
          jobId);
      return groupedRequests.stream()
          .map(
              indexedRequest ->
                  new IndexedResult(
                      indexedRequest.index(),
                      StatsCaptureBatchItemResult.queued(
                          indexedRequest.request(), "enqueued reconcile capture")))
          .toList();
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed enqueuing async stats capture table=%s requests=%d",
          first.tableId(),
          groupedRequests.size());
      incrementCounter(
          ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
          1,
          Tag.of(TagKey.TRIGGER, "async_skip"),
          Tag.of(TagKey.REASON, "enqueue_failure"),
          Tag.of(TagKey.SCOPE, "orchestrator"));
      return groupedRequests.stream()
          .map(
              indexedRequest ->
                  new IndexedResult(
                      indexedRequest.index(),
                      StatsCaptureBatchItemResult.degraded(
                          indexedRequest.request(), "failed to enqueue reconcile capture")))
          .toList();
    }
  }

  private static ResourceId connectorResourceId(String accountId, String connectorId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(connectorId)
        .setKind(ResourceKind.RK_CONNECTOR)
        .build();
  }

  private List<IndexedResult> recordAsyncSkipGroup(
      List<IndexedRequest> groupedRequests, String reason, String message) {
    return groupedRequests.stream()
        .map(
            indexedRequest ->
                new IndexedResult(
                    indexedRequest.index(),
                    recordAsyncSkip(indexedRequest.request(), reason, message)))
        .toList();
  }

  private StatsCaptureBatchItemResult recordAsyncSkip(
      StatsCaptureRequest request, String reason, String message) {
    LOG.warnf(
        "%s table=%s snapshot=%d reason=%s",
        message, request.tableId(), request.snapshotId(), reason);
    incrementCounter(
        ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
        1,
        Tag.of(TagKey.TRIGGER, "async_skip"),
        Tag.of(TagKey.REASON, reason),
        Tag.of(TagKey.SCOPE, "orchestrator"));
    return StatsCaptureBatchItemResult.uncapturable(request, reason);
  }

  private static TableKey tableKey(StatsCaptureRequest request) {
    return new TableKey(request.tableId().getAccountId(), request.tableId().getId());
  }

  private StatsCaptureRequest toAsyncRequest(StatsCaptureRequest request) {
    if (request.executionMode() == StatsExecutionMode.ASYNC) {
      return request;
    }
    return StatsCaptureRequest.builder(request.tableId(), request.snapshotId(), request.target())
        .columnSelectors(request.columnSelectors())
        .requestedKinds(request.requestedKinds())
        .executionMode(StatsExecutionMode.ASYNC)
        .connectorType(request.connectorType())
        .correlationId(request.correlationId())
        .samplingRequested(request.samplingRequested())
        .latencyBudget(request.latencyBudget())
        .build();
  }

  /**
   * Explicit capture triggers now enqueue unified reconcile capture work rather than routing
   * through the removed stats-engine framework.
   */
  public StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureBatchItemResult> items = enqueueAsyncCaptureBatch(batchRequest.requests());
    for (StatsCaptureBatchItemResult item : items) {
      incrementCounter(
          ServiceMetrics.Stats.BATCH_ITEMS_TOTAL,
          1,
          Tag.of(TagKey.RESULT, item.outcome().name()),
          Tag.of(TagKey.SCOPE, "orchestrator"));
    }
    LOG.infof(
        "stats_trigger_batch outcomes=%s group_size=%d", summarizeOutcomes(items), items.size());
    return StatsCaptureBatchResult.of(items);
  }

  private String summarizeOutcomes(List<StatsCaptureBatchItemResult> items) {
    Map<String, Long> counts = new LinkedHashMap<>();
    for (StatsCaptureBatchItemResult item : items) {
      counts.merge(item.outcome().name(), 1L, Long::sum);
    }
    return counts.toString();
  }

  private void observeSyncOutcome(StatsSyncOutcome outcome, long startNanos) {
    observeSyncOutcome(outcome);
    if (observability != null) {
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
      Tag[] baseTags = {
        Tag.of(TagKey.COMPONENT, COMPONENT),
        Tag.of(TagKey.OPERATION, OPERATION),
        Tag.of(TagKey.RESULT, outcome.name()),
        Tag.of(TagKey.SCOPE, "orchestrator")
      };
      observability.timer(ServiceMetrics.Stats.SYNC_LATENCY, elapsed, baseTags);
    }
  }

  private void observeSyncOutcome(StatsSyncOutcome outcome) {
    incrementCounter(
        ServiceMetrics.Stats.SYNC_OUTCOMES_TOTAL,
        1,
        Tag.of(TagKey.RESULT, outcome.name()),
        Tag.of(TagKey.SCOPE, "orchestrator"));
  }

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

  private static ReconcileCapturePolicy capturePolicyFor(List<StatsCaptureRequest> requests) {
    LinkedHashSet<ReconcileCapturePolicy.Output> outputs = new LinkedHashSet<>();
    LinkedHashMap<String, ReconcileCapturePolicy.Column> columns = new LinkedHashMap<>();
    for (StatsCaptureRequest request : requests) {
      if (request == null) {
        continue;
      }
      switch (request.target().getTargetCase()) {
        case TABLE -> outputs.add(ReconcileCapturePolicy.Output.TABLE_STATS);
        case COLUMN -> {
          outputs.add(ReconcileCapturePolicy.Output.COLUMN_STATS);
          columns.put(
              "#" + request.target().getColumn().getColumnId(),
              new ReconcileCapturePolicy.Column(
                  "#" + request.target().getColumn().getColumnId(), true, false));
        }
        case FILE -> outputs.add(ReconcileCapturePolicy.Output.FILE_STATS);
        case TARGET_NOT_SET, EXPRESSION -> {}
      }
      for (String selector : request.columnSelectors()) {
        if (selector == null || selector.isBlank()) {
          continue;
        }
        columns.put(selector, new ReconcileCapturePolicy.Column(selector, true, false));
      }
    }
    return ReconcileCapturePolicy.of(List.copyOf(columns.values()), Set.copyOf(outputs));
  }

  private record IndexedRequest(int index, StatsCaptureRequest request) {}

  private record IndexedResult(int index, StatsCaptureBatchItemResult result) {}

  private record TableKey(String accountId, String tableId) {}
}
