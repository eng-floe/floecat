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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
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
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
  private static final String COMPONENT = "service";
  private static final String OPERATION = "stats_orchestrator";
  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final StatsSyncCapture statsSyncCapture;
  private final boolean syncEnabled;
  private final ConcurrentMap<TableKey, String> lastEnqueuedJobByTable = new ConcurrentHashMap<>();
  private final Observability observability;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      StatsSyncCapture statsSyncCapture,
      @ConfigProperty(name = "floecat.stats.sync.enabled", defaultValue = "true")
          boolean syncEnabled,
      Instance<Observability> observability) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.statsSyncCapture = statsSyncCapture;
    this.syncEnabled = syncEnabled;
    this.observability =
        observability == null || observability.isUnsatisfied() ? null : observability.get();
  }

  public StatsOrchestrator(
      StatsStore statsStore, ReconcileJobStore reconcileJobStore, TableRepository tableRepository) {
    this(
        statsStore,
        reconcileJobStore,
        tableRepository,
        new StatsSyncCapture(reconcileJobStore),
        true,
        null);
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

      // P0_SYNC: query-time bounded capture. This is the only code path that assigns P0.
      // The lane key (accountId:tableId) serializes concurrent sync requests for the same table.
      String laneKey =
          statsLaneKey(request.tableId().getAccountId(), table.get().getResourceId().getId());
      ReconcileExecutionPolicy syncPolicy =
          ReconcileExecutionPolicy.of(
              StatsPriorityClass.P0_SYNC, laneKey, Map.of("enqueue_reason", "sync_capture"));

      return statsSyncCapture.capture(
          request.tableId().getAccountId(),
          connectorId,
          scope,
          request.latencyBudget().get(),
          syncPolicy);
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
    // P2_REPAIR: async follow-up after a sync outcome of PARTIAL, TIMEOUT, or FAILED.
    // These jobs fill missing coverage without blocking the planner again.
    enqueueAsyncCaptureBatch(List.of(request), StatsPriorityClass.P2_REPAIR, reason);
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

  /**
   * Enqueues a batch of async capture requests.
   *
   * <p>Callers that need P3_BACKGROUND (the default for background and batch-trigger paths) should
   * use the single-argument overload. The {@link #enqueueAsyncFollowUp} path passes P2_REPAIR.
   */
  private List<StatsCaptureBatchItemResult> enqueueAsyncCaptureBatch(
      List<StatsCaptureRequest> requests) {
    // P3_BACKGROUND: routine background refresh and explicit trigger paths.
    return enqueueAsyncCaptureBatch(requests, StatsPriorityClass.P3_BACKGROUND, "background_async");
  }

  private List<StatsCaptureBatchItemResult> enqueueAsyncCaptureBatch(
      List<StatsCaptureRequest> requests, StatsPriorityClass priorityClass, String enqueueReason) {
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
      for (IndexedResult result :
          enqueueAsyncGroup(groupedRequests, priorityClass, enqueueReason)) {
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
      case TABLE, COLUMN -> Optional.empty();
    };
  }

  private List<IndexedResult> enqueueAsyncGroup(
      List<IndexedRequest> groupedRequests,
      StatsPriorityClass priorityClass,
      String enqueueReason) {
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

      // Build the execution policy: lane key serialises concurrent jobs for the same table;
      // enqueue_reason carries the scheduling intent for metrics and debugging.
      String laneKey =
          statsLaneKey(first.tableId().getAccountId(), table.get().getResourceId().getId());
      ReconcileExecutionPolicy executionPolicy =
          ReconcileExecutionPolicy.of(
              priorityClass, laneKey, Map.of("enqueue_reason", enqueueReason));

      String jobId =
          reconcileJobStore.enqueue(
              first.tableId().getAccountId(),
              table.get().getUpstream().getConnectorId().getId(),
              false,
              ReconcilerService.CaptureMode.CAPTURE_ONLY,
              scope,
              executionPolicy,
              "");
      lastEnqueuedJobByTable.put(tableKey(first), jobId);
      LOG.infof(
          "stats_enqueue outcome=QUEUED table=%s snapshots=%d targets=%d reason=%s priority=%s group_size=%d job=%s",
          first.tableId(),
          (int)
              captureRequests.stream()
                  .map(ReconcileScope.ScopedCaptureRequest::snapshotId)
                  .distinct()
                  .count(),
          captureRequests.size(),
          enqueueReason,
          priorityClass,
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

  /**
   * Returns the scheduling lane key for a stats-driven enqueue.
   *
   * <p>The lane key ({@code accountId:tableId}) is used to serialise concurrent jobs that target
   * the same table, and to enable WRR fairness across tables within the same priority class (phase
   * 2). It is stored in {@link ReconcileExecutionPolicy#lane()} and is visible in metrics.
   */
  private static String statsLaneKey(String accountId, String tableId) {
    return accountId + ":" + tableId;
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
    incrementCounter(
        ServiceMetrics.Stats.SYNC_OUTCOMES_TOTAL,
        1,
        Tag.of(TagKey.RESULT, outcome.name()),
        Tag.of(TagKey.SCOPE, "orchestrator"));
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

  /**
   * Builds the capture policy for a list of stats requests.
   *
   * <p>{@link ReconcileCapturePolicy.Output#PARQUET_PAGE_INDEX} is intentionally never included
   * here. Parquet page-index construction (Floescan) is always async (EXEC_FILE_GROUP scoped) and
   * is enqueued separately. Including it in a sync-capture policy would violate the latency budget
   * contract.
   */
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
