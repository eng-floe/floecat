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
import ai.floedb.floecat.service.statistics.PlannerStatsResolver.PlannerLookupDiagnostics;
import ai.floedb.floecat.service.statistics.PlannerStatsResolver.PlannerLookupOutcome;
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
 *
 * <p>Planner-facing store resolution — generation ordering, the fallback ladder, and the planner
 * stats cache — lives in {@link PlannerStatsResolver}; capture policy stays here.
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
  private static final String COMPONENT = "service";
  private static final String OPERATION = "stats_orchestrator";

  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final ConnectorRepository connectorRepository;
  private final StatsSyncCapture statsSyncCapture;
  private final boolean syncEnabled;
  private final ConcurrentMap<TableKey, String> lastEnqueuedJobByTable = new ConcurrentHashMap<>();
  private final Observability observability;
  private final PlannerStatsResolver plannerResolver;

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
    this.plannerResolver =
        new PlannerStatsResolver(statsStore, this.observability, this::observePlannerHit);
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
    plannerResolver.invalidateStatsCache(tableId, snapshotId);
  }

  /** Invalidates one cached target for one table snapshot. */
  public void invalidateStatsCache(ResourceId tableId, long snapshotId, StatsTarget target) {
    plannerResolver.invalidateStatsCache(tableId, snapshotId, target);
  }

  /** Invalidates cached targets represented by successfully persisted records. */
  public void invalidateStatsCache(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    plannerResolver.invalidateStatsCache(tableId, snapshotId, records);
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
    return captureAndResolve(request, startNanos);
  }

  /**
   * Single-target resolution that honors the pinned stats generation, mirroring {@link
   * #resolvePlannerBatchInGeneration}: the pinned generation for the pinned snapshot (query
   * consistent), then the newest (live active) generation only to fill a target the pinned
   * generation lacks (so an incomplete pinned generation never yields NOT_FOUND), then bounded
   * capture. When the pin froze no generation the live/newest generation is the primary source.
   * Used by the planner's per-relation stats reads.
   */
  public StatsResolutionResult resolveInGeneration(
      StatsCaptureRequest request, Optional<String> pinnedGenerationToken) {
    long startNanos = System.nanoTime();
    Optional<TargetStatsRecord> stored =
        plannerResolver.resolveSingleFromStore(request, pinnedGenerationToken);
    if (stored.isPresent()) {
      observeSyncOutcome(StatsSyncOutcome.HIT, startNanos);
      return StatsResolutionResult.hit(stored.get());
    }
    return captureAndResolve(request, startNanos);
  }

  /** Bounded sync capture, then async-enqueue fallback, for a store miss. */
  private StatsResolutionResult captureAndResolve(StatsCaptureRequest request, long startNanos) {
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
    return resolvePlannerBatchInGeneration(requests, Optional.empty(), staleOk, deadlineNanos);
  }

  /**
   * Planner batch resolution that honors the query's pinned stats generation, with presence-only
   * completeness: a target that exists in the pinned generation is a hit. Delegates to {@link
   * #resolvePlannerBatchInGeneration(java.util.List, Optional, java.util.Map, boolean, long)} with
   * no per-target completeness predicates.
   */
  public java.util.Map<String, StatsResolutionResult> resolvePlannerBatchInGeneration(
      java.util.List<StatsCaptureRequest> requests,
      Optional<String> pinnedGenerationToken,
      boolean staleOk,
      long deadlineNanos) {
    return resolvePlannerBatchInGeneration(
        requests, pinnedGenerationToken, java.util.Map.of(), staleOk, deadlineNanos);
  }

  /**
   * Planner batch resolution that honors the query's pinned stats generation and the planner's
   * per-target completeness needs.
   *
   * <p>The pin freezes a stats generation for the query's lifetime (as the scan path does), so
   * plans are stable and reproducible for a given pin. "Exists in the pinned generation" alone is
   * too coarse a hit rule, though: generations enrich over time (a finalize can add sketch payloads
   * to a snapshot whose earlier generation was scalar-only), so a pinned record that lacks a
   * requested capability must not stop resolution — the planner would consume it downgraded while a
   * richer record for the SAME snapshot exists. For each target the lookup order is:
   *
   * <ol>
   *   <li>cache hit in the pinned generation's keyspace (the live/newest keyspace when the pin
   *       froze no generation), only if the cached record satisfies the target's completeness
   *       predicate;
   *   <li>the pinned generation for the pinned snapshot — the primary source; a record that fails
   *       its predicate is held as a PARTIAL candidate rather than served;
   *   <li>the newest (live active) generation of the SAME pinned snapshot, consulted for targets
   *       the pinned generation lacks or serves only partially. Never a newer snapshot: this is
   *       richer stats for identical data, not weakened snapshot consistency. If newest satisfies,
   *       it wins; if not, the pinned partial is served (consistency prefers the pin between
   *       equally incomplete records) — partial records never fall through to stale or capture;
   *   <li>stale stats from a snapshot &le; the pinned snapshot (when {@code staleOk}), for targets
   *       with no record at the pinned snapshot at all;
   *   <li>sync/async capture.
   * </ol>
   *
   * <p>Hits are cached under the generation actually served — the pinned generation under its own
   * token, newest-fill under the empty token — so the two never contaminate each other. Records are
   * cached whole and completeness is re-evaluated per read, so one query's lesser need never masks
   * another's richer need. When the pin froze no generation the primary source is the live/newest
   * generation (empty token) and the fill step is skipped: a partial primary record is served
   * as-is, because no richer same-snapshot source exists.
   *
   * @param pinnedGenerationToken the generation frozen on the query pin, read as the primary
   *     source; empty when the pin froze no generation (then the live/newest generation is primary)
   * @param completenessByStorageId per-target completeness predicate keyed by {@code
   *     StatsTargetIdentity.storageId}; a target with no entry treats presence as complete. Kept as
   *     plain predicates so this class stays independent of the planner request model — callers
   *     derive them from the request's needs (see {@code PlannerStatsResultMaterializer}).
   */
  public java.util.Map<String, StatsResolutionResult> resolvePlannerBatchInGeneration(
      java.util.List<StatsCaptureRequest> requests,
      Optional<String> pinnedGenerationToken,
      java.util.Map<String, java.util.function.Predicate<TargetStatsRecord>>
          completenessByStorageId,
      boolean staleOk,
      long deadlineNanos) {
    if (requests == null || requests.isEmpty()) {
      return java.util.Map.of();
    }
    // All requests must share the same tableId and snapshotId (grouped upstream by TableWork).
    StatsCaptureRequest first = requests.get(0);
    // Store rungs (cache → pinned/primary → newest gap-fill → stale) live in the resolver; only
    // capture policy remains here.
    PlannerStatsResolver.Resolution resolution =
        plannerResolver.resolveFromStore(
            requests, pinnedGenerationToken, completenessByStorageId, staleOk);
    PlannerLookupDiagnostics diagnostics = resolution.diagnostics();
    java.util.Map<String, StatsResolutionResult> out =
        new java.util.LinkedHashMap<>(resolution.resolved());

    // 5. Sync capture for still-missing targets (per-target within deadline).
    java.util.List<StatsCaptureRequest> asyncQueue = new java.util.ArrayList<>();
    for (StatsCaptureRequest req : resolution.stillMissing()) {
      String key = storageId(req);
      if (!syncEnabled || req.executionMode() != StatsExecutionMode.SYNC) {
        asyncQueue.add(req);
        diagnostics.record(PlannerLookupOutcome.CAPTURE_PENDING);
        out.put(key, StatsResolutionResult.skipped("async_mode"));
        continue;
      }
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        asyncQueue.add(req);
        diagnostics.record(PlannerLookupOutcome.CAPTURE_PENDING);
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
          diagnostics.record(PlannerLookupOutcome.CAPTURED);
          out.put(key, StatsResolutionResult.captured(afterCapture.get()));
        } else {
          asyncQueue.add(req);
          diagnostics.record(PlannerLookupOutcome.CAPTURE_PENDING);
          out.put(
              key,
              StatsResolutionResult.partial(
                  "sync capture succeeded but store record not visible; async follow-up enqueued"));
        }
      } else {
        asyncQueue.add(req);
        diagnostics.record(
            syncOutcome == StatsSyncOutcome.TIMEOUT
                ? PlannerLookupOutcome.CAPTURE_PENDING
                : PlannerLookupOutcome.FAILED);
        out.put(
            key,
            syncOutcome == StatsSyncOutcome.TIMEOUT
                ? StatsResolutionResult.timeout("sync capture timed out; async follow-up enqueued")
                : StatsResolutionResult.failed("sync capture failed; async follow-up enqueued"));
      }
    }

    // 6. Enqueue async captures for all misses that didn't sync.
    if (!asyncQueue.isEmpty()) {
      enqueueAsyncCaptureBatch(asyncQueue);
    }

    diagnostics.emit(
        first.tableId(), first.snapshotId(), resolution.pinnedGeneration(), requests.size());
    return java.util.Collections.unmodifiableMap(out);
  }

  private static String storageId(StatsCaptureRequest request) {
    return StatsTargetIdentity.storageId(request.target());
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

  /**
   * Planner-hit callback for {@link PlannerStatsResolver}: counts a HIT without a latency sample.
   */
  private void observePlannerHit(StatsSyncOutcome outcome) {
    observeSyncOutcome(outcome);
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
