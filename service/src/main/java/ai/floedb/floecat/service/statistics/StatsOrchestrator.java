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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.engine.StatsEngineRegistry;
import ai.floedb.floecat.stats.identity.StatsTargetScopeCodec;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Central stats orchestration coordinator.
 *
 * <p>This class serves two roles:
 *
 * <ul>
 *   <li>query-time resolution policy via {@link #resolve(StatsCaptureRequest)}
 *   <li>explicit capture execution via {@link #trigger(StatsCaptureRequest)}
 * </ul>
 *
 * <p>Resolution is store-first, then optional synchronous capture, then async enqueue fallback when
 * data is still missing.
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
  private static final String METRIC_BATCH_ITEMS_TOTAL = "floecat_stats_batch_items_total";
  private static final String METRIC_BATCH_GROUPS_TOTAL = "floecat_stats_batch_groups_total";
  private static final String METRIC_STORE_HITS_TOTAL = "floecat_stats_store_hits_total";
  private static final String METRIC_STORE_MISSES_TOTAL = "floecat_stats_store_misses_total";

  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final StatsEngineRegistry statsEngineRegistry;
  private final MeterRegistry meterRegistry;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      StatsEngineRegistry statsEngineRegistry,
      Instance<MeterRegistry> meterRegistry) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.statsEngineRegistry = statsEngineRegistry;
    this.meterRegistry =
        meterRegistry == null || meterRegistry.isUnsatisfied() ? null : meterRegistry.get();
  }

  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      StatsEngineRegistry statsEngineRegistry) {
    this(statsStore, reconcileJobStore, tableRepository, statsEngineRegistry, null);
  }

  /**
   * Unified query-time resolution path.
   *
   * <p>Order: persisted store hit, then sync capture for SYNC requests, then async enqueue fallback
   * for misses that have at least one registry-capable ASYNC engine candidate.
   */
  public Optional<TargetStatsRecord> resolve(StatsCaptureRequest request) {
    return resolveBatch(StatsCaptureBatchRequest.of(request)).getFirst();
  }

  /**
   * Batch variant of {@link #resolve(StatsCaptureRequest)}.
   *
   * <p>Requests are resolved in-order with the same semantics as single-item resolve.
   */
  public List<Optional<TargetStatsRecord>> resolveBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureRequest> requests = batchRequest.requests();
    incrementCounter(METRIC_BATCH_ITEMS_TOTAL, requests.size());
    List<Optional<TargetStatsRecord>> resolved = new ArrayList<>(requests.size());
    List<Integer> syncMissingIndexes = new ArrayList<>();
    List<StatsCaptureRequest> syncMissingRequests = new ArrayList<>();
    List<StatsCaptureRequest> unresolvedForAsync = new ArrayList<>();

    for (StatsCaptureRequest request : requests) {
      Optional<TargetStatsRecord> stored = readStore(request);
      resolved.add(stored);
      if (stored.isPresent()) {
        continue;
      }
      if (request.executionMode() == StatsExecutionMode.SYNC) {
        syncMissingIndexes.add(resolved.size() - 1);
        syncMissingRequests.add(request);
      } else {
        unresolvedForAsync.add(request);
      }
    }

    if (!syncMissingRequests.isEmpty()) {
      incrementCounter(METRIC_BATCH_GROUPS_TOTAL, 1, "type", "sync_capture");
      StatsCaptureBatchResult syncCaptureResult =
          triggerBatch(StatsCaptureBatchRequest.of(syncMissingRequests));
      for (int i = 0; i < syncCaptureResult.results().size(); i++) {
        StatsCaptureBatchItemResult item = syncCaptureResult.results().get(i);
        int resolvedIndex = syncMissingIndexes.get(i);
        if (item.outcome() == StatsTriggerOutcome.CAPTURED) {
          resolved.set(resolvedIndex, item.captureResult().map(StatsCaptureResult::record));
          continue;
        }
        unresolvedForAsync.add(item.request());
      }
    }

    if (!unresolvedForAsync.isEmpty()) {
      incrementCounter(METRIC_BATCH_GROUPS_TOTAL, 1, "type", "async_followup");
      tryEnqueueAsyncCaptureBatch(unresolvedForAsync);
    }
    return List.copyOf(resolved);
  }

  private Optional<TargetStatsRecord> readStore(StatsCaptureRequest request) {
    // Fast path: authoritative persisted read.
    Optional<TargetStatsRecord> out =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target());
    incrementCounter(out.isPresent() ? METRIC_STORE_HITS_TOTAL : METRIC_STORE_MISSES_TOTAL, 1);
    return out;
  }

  private void tryEnqueueAsyncCaptureBatch(List<StatsCaptureRequest> requests) {
    if (requests == null || requests.isEmpty()) {
      return;
    }
    Map<TableKey, List<StatsCaptureRequest>> groupedByTable = new LinkedHashMap<>();
    Map<AsyncCandidateKey, Boolean> asyncCandidateCache = new LinkedHashMap<>();
    for (StatsCaptureRequest request : requests) {
      if (request.snapshotId() < 0L) {
        continue;
      }
      StatsCaptureRequest asyncRequest = toAsyncRequest(request);
      AsyncCandidateKey candidateKey = AsyncCandidateKey.from(asyncRequest);
      boolean hasCandidates =
          asyncCandidateCache.computeIfAbsent(
              candidateKey, ignored -> hasAsyncCandidates(asyncRequest));
      if (!hasCandidates) {
        continue;
      }
      groupedByTable
          .computeIfAbsent(tableKey(asyncRequest), ignored -> new ArrayList<>())
          .add(asyncRequest);
    }
    for (List<StatsCaptureRequest> groupedRequests : groupedByTable.values()) {
      enqueueAsyncGroup(groupedRequests);
    }
  }

  private void enqueueAsyncGroup(List<StatsCaptureRequest> groupedRequests) {
    if (groupedRequests == null || groupedRequests.isEmpty()) {
      return;
    }
    StatsCaptureRequest first = groupedRequests.getFirst();
    try {
      Optional<Table> table = tableRepository.getById(first.tableId());
      if (table.isEmpty()
          || !table.get().hasUpstream()
          || !table.get().getUpstream().hasConnectorId()
          || table.get().getUpstream().getConnectorId().getId().isBlank()) {
        return;
      }
      List<List<String>> namespaceScope =
          table.get().getUpstream().getNamespacePathCount() == 0
              ? List.of()
              : List.of(table.get().getUpstream().getNamespacePathList());
      String tableDisplay =
          table.get().getUpstream().getTableDisplayName().isBlank()
              ? table.get().getDisplayName()
              : table.get().getUpstream().getTableDisplayName();
      LinkedHashSet<Long> snapshotIds = new LinkedHashSet<>();
      LinkedHashSet<String> statsTargets = new LinkedHashSet<>();
      for (StatsCaptureRequest request : groupedRequests) {
        snapshotIds.add(request.snapshotId());
        statsTargets.add(StatsTargetScopeCodec.encode(request.target()));
      }
      ReconcileScope scope =
          ReconcileScope.of(
              namespaceScope,
              tableDisplay,
              List.of(),
              List.copyOf(snapshotIds),
              List.copyOf(statsTargets));
      reconcileJobStore.enqueue(
          first.tableId().getAccountId(),
          table.get().getUpstream().getConnectorId().getId(),
          false,
          ReconcilerService.CaptureMode.STATS_ONLY,
          scope);
      LOG.infof(
          "stats_enqueue outcome=%s table=%s snapshots=%d targets=%d reason=%s group_size=%d",
          StatsTriggerOutcome.QUEUED,
          first.tableId(),
          snapshotIds.size(),
          statsTargets.size(),
          "missing_or_degraded_sync_capture",
          groupedRequests.size());
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed enqueuing async stats capture table=%s requests=%d",
          first.tableId(),
          groupedRequests.size());
    }
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

  private boolean hasAsyncCandidates(StatsCaptureRequest asyncRequest) {
    List<StatsCaptureEngine> candidates = statsEngineRegistry.candidates(asyncRequest);
    return !candidates.isEmpty();
  }

  /**
   * Executes one explicit trigger attempt through the registry.
   *
   * <p>This path does not perform store-read short-circuiting and does not enqueue fallback work.
   * It is intended for control-plane callers that explicitly request capture now.
   */
  public StatsTriggerResult trigger(StatsCaptureRequest request) {
    StatsCaptureBatchResult batchResult = triggerBatch(StatsCaptureBatchRequest.of(request));
    return toTriggerResult(batchResult.results().getFirst());
  }

  /**
   * Executes explicit capture trigger attempts for each request in the batch.
   *
   * <p>Each item is routed independently using registry capability + priority policy. Results are
   * returned in the same order as requests.
   */
  public StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
    StatsCaptureBatchResult registryResult = statsEngineRegistry.captureBatch(batchRequest);
    Map<StatsTriggerOutcome, Integer> outcomeCounts = new LinkedHashMap<>();
    registryResult
        .results()
        .forEach(
            item -> {
              StatsTriggerResult triggerResult = toTriggerResult(item);
              outcomeCounts.merge(triggerResult.outcome(), 1, Integer::sum);
              incrementCounter(
                  METRIC_BATCH_ITEMS_TOTAL, 1, "outcome", triggerResult.outcome().name());
              logTriggerOutcome(item.request(), triggerResult);
            });
    LOG.infof(
        "stats_trigger_batch outcome=%s group_size=%d",
        outcomeCounts, batchRequest.requests().size());
    return registryResult;
  }

  private static StatsTriggerResult toTriggerResult(StatsCaptureBatchItemResult item) {
    return switch (item.outcome()) {
      case CAPTURED ->
          StatsTriggerResult.captured(
              item.captureResult()
                  .orElseThrow(
                      () -> new IllegalStateException("captured outcome missing payload")));
      case QUEUED -> StatsTriggerResult.queued(item.detail());
      case UNCAPTURABLE -> StatsTriggerResult.uncapturable(item.detail());
      case DEGRADED -> StatsTriggerResult.degraded(item.detail());
    };
  }

  private void logTriggerOutcome(StatsCaptureRequest request, StatsTriggerResult result) {
    if (result.outcome() == StatsTriggerOutcome.CAPTURED) {
      LOG.debugf(
          "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
          result.outcome(), request.tableId(), request.snapshotId(), result.detail());
      return;
    }
    LOG.warnf(
        "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
        result.outcome(), request.tableId(), request.snapshotId(), result.detail());
  }

  private void incrementCounter(String metricName, double amount, String... tags) {
    if (meterRegistry == null) {
      return;
    }
    meterRegistry.counter(metricName, tags).increment(amount);
  }

  private record TableKey(String accountId, String tableId) {}

  private record AsyncCandidateKey(
      String connectorType, String targetCase, StatsExecutionMode mode) {
    private static AsyncCandidateKey from(StatsCaptureRequest request) {
      return new AsyncCandidateKey(
          request.connectorType(),
          request.target().getTargetCase().name(),
          request.executionMode());
    }
  }
}
