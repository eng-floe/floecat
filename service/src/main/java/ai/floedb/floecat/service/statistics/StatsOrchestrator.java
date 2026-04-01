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
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
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
 *   <li>explicit capture execution via {@link #capture(StatsCaptureRequest)}
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
    // Fast path: authoritative persisted read.
    Optional<TargetStatsRecord> stored =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target());
    if (stored.isPresent()) {
      return stored;
    }

    // PR6 behavior: for SYNC requests, attempt one inline capture pass before queue fallback.
    // PR10 insertion point: after enqueue below, wait up to latency budget and re-read store.
    boolean enqueueEligible = true;
    if (request.executionMode() == StatsExecutionMode.SYNC) {
      CaptureAttempt attempt = captureAttempt(request);
      if (attempt.result().isPresent()) {
        return attempt.result().map(StatsCaptureResult::record);
      }
      enqueueEligible = attempt.supported();
    }

    // Miss fallback: schedule table-scoped async stats-only capture for the requested snapshot
    // when the registry advertises ASYNC support for the requested target.
    if (enqueueEligible) {
      tryEnqueueAsyncCapture(request);
    }
    return Optional.empty();
  }

  private void tryEnqueueAsyncCapture(StatsCaptureRequest request) {
    // Delta tables can legitimately use snapshot id 0.
    if (request.snapshotId() < 0L) {
      return;
    }
    if (!isAsyncCapturableByRegistry(request)) {
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
      ReconcileScope scope =
          ReconcileScope.of(namespaceScope, tableDisplay, List.of(), List.of(request.snapshotId()));
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

  private boolean isAsyncCapturableByRegistry(StatsCaptureRequest request) {
    StatsCaptureRequest asyncRequest =
        request.executionMode() == StatsExecutionMode.ASYNC
            ? request
            : StatsCaptureRequest.builder(request.tableId(), request.snapshotId(), request.target())
                .columnSelectors(request.columnSelectors())
                .requestedKinds(request.requestedKinds())
                .executionMode(StatsExecutionMode.ASYNC)
                .connectorType(request.connectorType())
                .correlationId(request.correlationId())
                .samplingRequested(request.samplingRequested())
                .latencyBudget(request.latencyBudget())
                .build();
    List<StatsCaptureEngine> candidates = statsEngineRegistry.candidates(asyncRequest);
    return candidates != null && !candidates.isEmpty();
  }

  /** Executes one capture attempt via the shared registry-backed control plane. */
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    return captureAttempt(request).result();
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
}
