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
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
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

  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final StatsEngineRegistry statsEngineRegistry;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      StatsEngineRegistry statsEngineRegistry) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.statsEngineRegistry = statsEngineRegistry;
  }

  /**
   * Unified query-time resolution path.
   *
   * <p>Order: persisted store hit, then sync capture for SYNC requests, then async enqueue fallback
   * for misses that have at least one registry-capable ASYNC engine candidate.
   */
  public Optional<TargetStatsRecord> resolve(StatsCaptureRequest request) {
    Optional<TargetStatsRecord> stored = readStore(request);
    if (stored.isPresent()) {
      return stored;
    }

    if (request.executionMode() == StatsExecutionMode.SYNC) {
      // PR6 behavior: for SYNC requests, attempt one inline capture pass before queue fallback.
      // PR10 insertion point: after enqueue, wait up to latency budget and re-read store.
      CaptureAttempt attempt = captureAttempt(request);
      if (attempt.result().isPresent()) {
        return attempt.result().map(StatsCaptureResult::record);
      }
      maybeEnqueue(request, attempt.supported());
      return Optional.empty();
    }
    maybeEnqueue(request, true);
    return Optional.empty();
  }

  /**
   * Batch variant of {@link #resolve(StatsCaptureRequest)}.
   *
   * <p>Requests are resolved in-order with the same semantics as single-item resolve.
   */
  public List<Optional<TargetStatsRecord>> resolveBatch(StatsCaptureBatchRequest batchRequest) {
    return batchRequest.requests().stream().map(this::resolve).toList();
  }

  private Optional<TargetStatsRecord> readStore(StatsCaptureRequest request) {
    // Fast path: authoritative persisted read.
    return statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target());
  }

  private void maybeEnqueue(StatsCaptureRequest request, boolean enqueueEligible) {
    if (!enqueueEligible) {
      return;
    }
    tryEnqueueAsyncCapture(request);
  }

  private void tryEnqueueAsyncCapture(StatsCaptureRequest request) {
    // Delta tables can legitimately use snapshot id 0.
    if (request.snapshotId() < 0L) {
      return;
    }
    StatsCaptureRequest asyncRequest = toAsyncRequest(request);
    if (!hasAsyncCandidates(asyncRequest)) {
      return;
    }
    try {
      Optional<Table> table = tableRepository.getById(request.tableId());
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
      ReconcileScope scope = ReconcileScope.of(namespaceScope, tableDisplay, List.of());
      reconcileJobStore.enqueue(
          request.tableId().getAccountId(),
          table.get().getUpstream().getConnectorId().getId(),
          false,
          ReconcilerService.CaptureMode.STATS_ONLY,
          scope);
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed enqueuing async stats capture table=%s snapshot=%s",
          request.tableId(),
          request.snapshotId());
    }
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
    return candidates != null && !candidates.isEmpty();
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
    registryResult
        .results()
        .forEach(item -> logTriggerOutcome(item.request(), toTriggerResult(item)));
    return registryResult;
  }

  private CaptureAttempt captureAttempt(StatsCaptureRequest request) {
    try {
      return new CaptureAttempt(statsEngineRegistry.capture(request), true);
    } catch (StatsUnsupportedTargetException unsupported) {
      LOG.debugf(
          "Stats target not supported for capture table=%s snapshot=%s target=%s",
          request.tableId(), request.snapshotId(), unsupported.targetType());
      return new CaptureAttempt(Optional.empty(), false);
    }
  }

  private record CaptureAttempt(Optional<StatsCaptureResult> result, boolean supported) {}

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
}
