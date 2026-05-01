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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * <p>Resolution is store-first, then async enqueue fallback when data is still missing.
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
  private static final String COMPONENT = "service";
  private static final String OPERATION = "stats_orchestrator";
  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final ConcurrentMap<TableKey, String> lastEnqueuedJobByTable = new ConcurrentHashMap<>();
  private final Observability observability;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      Instance<Observability> observability) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.observability =
        observability == null || observability.isUnsatisfied() ? null : observability.get();
  }

  public StatsOrchestrator(
      StatsStore statsStore, ReconcileJobStore reconcileJobStore, TableRepository tableRepository) {
    this(statsStore, reconcileJobStore, tableRepository, null);
  }

  /**
   * Unified query-time resolution path.
   *
   * <p>Order: persisted store hit, then async enqueue fallback for misses.
   */
  public Optional<TargetStatsRecord> resolve(StatsCaptureRequest request) {
    Optional<TargetStatsRecord> stored = readStore(request);
    if (stored.isPresent()) {
      return stored;
    }
    enqueueAsyncCaptureBatch(List.of(request));
    return Optional.empty();
  }

  /**
   * Batch variant of {@link #resolve(StatsCaptureRequest)}.
   *
   * <p>Requests are resolved in-order with the same semantics as single-item resolve.
   */
  public List<Optional<TargetStatsRecord>> resolveBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureRequest> requests = batchRequest.requests();
    incrementCounter(
        ServiceMetrics.Stats.BATCH_ITEMS_TOTAL,
        requests.size(),
        Tag.of(TagKey.SCOPE, "orchestrator"));
    List<Optional<TargetStatsRecord>> resolved = new ArrayList<>(requests.size());
    ArrayList<StatsCaptureRequest> unresolvedForAsync = new ArrayList<>();

    for (StatsCaptureRequest request : requests) {
      Optional<TargetStatsRecord> stored = readStore(request);
      resolved.add(stored);
      if (stored.isPresent()) {
        continue;
      }
      unresolvedForAsync.add(request);
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
