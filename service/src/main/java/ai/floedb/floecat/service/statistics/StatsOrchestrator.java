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
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final Duration DEFAULT_SYNC_IN_FLIGHT_WAIT = Duration.ofMillis(20);
  private static final Duration MAX_SYNC_IN_FLIGHT_WAIT = Duration.ofMillis(250);
  private static final Duration DEFAULT_SYNC_POST_CAPTURE_HOLD = Duration.ofMillis(250);
  private static final Duration MAX_SYNC_POST_CAPTURE_HOLD = Duration.ofSeconds(1);

  private final StatsStore statsStore;
  private final ReconcileJobStore reconcileJobStore;
  private final TableRepository tableRepository;
  private final StatsEngineRegistry statsEngineRegistry;
  private final ConcurrentMap<SyncCaptureKey, InFlightCapture> syncCaptureInFlight =
      new ConcurrentHashMap<>();

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
      CaptureAttempt attempt = captureAttemptSingleflight(request);
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
    List<StatsCaptureRequest> requests = batchRequest.requests();
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
      tryEnqueueAsyncCaptureBatch(unresolvedForAsync);
    }
    return List.copyOf(resolved);
  }

  /** Reads the authoritative persisted record for the exact request identity. */
  private Optional<TargetStatsRecord> readStore(StatsCaptureRequest request) {
    // Fast path: authoritative persisted read.
    return statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target());
  }

  /**
   * Enqueues async follow-up capture when a miss remains and at least one async-capable engine is
   * available.
   */
  private void maybeEnqueue(StatsCaptureRequest request, boolean enqueueEligible) {
    if (!enqueueEligible) {
      return;
    }
    tryEnqueueAsyncCaptureBatch(List.of(request));
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
      LinkedHashSet<String> tableColumns = new LinkedHashSet<>();
      LinkedHashSet<Long> snapshotIds = new LinkedHashSet<>();
      LinkedHashSet<String> statsTargets = new LinkedHashSet<>();
      for (StatsCaptureRequest request : groupedRequests) {
        tableColumns.addAll(request.columnSelectors());
        snapshotIds.add(request.snapshotId());
        statsTargets.add(StatsTargetScopeCodec.encode(request.target()));
      }
      ReconcileScope scope =
          ReconcileScope.of(
              namespaceScope,
              tableDisplay,
              List.copyOf(tableColumns),
              List.copyOf(snapshotIds),
              List.copyOf(statsTargets));
      reconcileJobStore.enqueue(
          first.tableId().getAccountId(),
          table.get().getUpstream().getConnectorId().getId(),
          false,
          ReconcilerService.CaptureMode.STATS_ONLY,
          scope);
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

  /** Returns an ASYNC-mode clone of the request while preserving identity and selector fields. */
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

  /** Returns true when the registry currently has at least one async candidate for the request. */
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

  /**
   * Executes one immediate registry capture attempt.
   *
   * <p>Unsupported targets are mapped to {@code supported=false} without raising hard errors to
   * callers.
   */
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

  /**
   * Performs a singleflight SYNC capture keyed by table/snapshot/target/connector.
   *
   * <p>Concurrent callers for the same key either execute the capture once or join the in-flight
   * attempt via {@link #awaitInFlightCapture(StatsCaptureRequest, CompletableFuture)}.
   */
  private CaptureAttempt captureAttemptSingleflight(StatsCaptureRequest request) {
    SyncCaptureKey key = SyncCaptureKey.of(request);
    while (true) {
      InFlightCapture inFlight = syncCaptureInFlight.get(key);
      if (inFlight != null) {
        if (inFlight.isExpired(System.nanoTime())) {
          syncCaptureInFlight.remove(key, inFlight);
          continue;
        }
        return awaitInFlightCapture(request, inFlight.future());
      }
      InFlightCapture created = InFlightCapture.inFlight();
      InFlightCapture prior = syncCaptureInFlight.putIfAbsent(key, created);
      if (prior != null) {
        continue;
      }
      try {
        CaptureAttempt attempt = captureAttempt(request);
        created.complete(attempt);
        if (attempt.result().isPresent()) {
          long holdNanos = syncPostCaptureHoldNanos(request);
          created.extendHold(holdNanos);
          scheduleInFlightEviction(key, created, holdNanos);
        } else {
          created.expireNow();
        }
        return attempt;
      } catch (RuntimeException e) {
        created.completeExceptionally(e);
        created.expireNow();
        throw e;
      } finally {
        if (created.isExpired(System.nanoTime())) {
          syncCaptureInFlight.remove(key, created);
        }
      }
    }
  }

  /**
   * Waits briefly for an in-flight capture started by another caller.
   *
   * <p>If waiting is interrupted, times out, or the in-flight capture fails, this method does not
   * recapture immediately to avoid duplicate concurrent capture storms.
   */
  private CaptureAttempt awaitInFlightCapture(
      StatsCaptureRequest request, CompletableFuture<CaptureAttempt> inFlight) {
    long waitMillis = syncInFlightWaitMillis(request);
    try {
      return inFlight.get(waitMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return CaptureAttempt.emptySupported();
    } catch (ExecutionException | TimeoutException ignored) {
      return CaptureAttempt.emptySupported();
    }
  }

  /**
   * Computes a bounded join timeout for SYNC singleflight waits.
   *
   * <p>Values are clamped to a positive duration in {@code [1ms, 250ms]} with a default of 20ms.
   */
  private long syncInFlightWaitMillis(StatsCaptureRequest request) {
    Duration wait = request.latencyBudget().orElse(DEFAULT_SYNC_IN_FLIGHT_WAIT);
    if (wait.isNegative() || wait.isZero()) {
      wait = DEFAULT_SYNC_IN_FLIGHT_WAIT;
    }
    if (wait.compareTo(MAX_SYNC_IN_FLIGHT_WAIT) > 0) {
      wait = MAX_SYNC_IN_FLIGHT_WAIT;
    }
    return Math.max(1L, wait.toMillis());
  }

  /**
   * Computes how long a completed capture stays reusable for duplicate SYNC calls.
   *
   * <p>This bridges the post-capture/pre-persist visibility window so callers avoid immediate
   * recapture while store writes are still in flight.
   */
  private long syncPostCaptureHoldNanos(StatsCaptureRequest request) {
    Duration hold = request.latencyBudget().orElse(DEFAULT_SYNC_POST_CAPTURE_HOLD);
    if (hold.isNegative() || hold.isZero()) {
      hold = DEFAULT_SYNC_POST_CAPTURE_HOLD;
    }
    if (hold.compareTo(MAX_SYNC_POST_CAPTURE_HOLD) > 0) {
      hold = MAX_SYNC_POST_CAPTURE_HOLD;
    }
    return hold.toNanos();
  }

  /** Schedules best-effort timed cleanup for completed singleflight entries. */
  private void scheduleInFlightEviction(
      SyncCaptureKey key, InFlightCapture capture, long holdNanos) {
    long delayMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(holdNanos));
    CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
        .execute(() -> syncCaptureInFlight.remove(key, capture));
  }

  private record CaptureAttempt(Optional<StatsCaptureResult> result, boolean supported) {
    private static CaptureAttempt emptySupported() {
      return new CaptureAttempt(Optional.empty(), true);
    }
  }

  private static final class InFlightCapture {
    private final CompletableFuture<CaptureAttempt> future = new CompletableFuture<>();
    private volatile long validUntilNanos = Long.MAX_VALUE;

    private static InFlightCapture inFlight() {
      return new InFlightCapture();
    }

    private CompletableFuture<CaptureAttempt> future() {
      return future;
    }

    private boolean isExpired(long nowNanos) {
      return nowNanos >= validUntilNanos;
    }

    private void extendHold(long holdNanos) {
      validUntilNanos = System.nanoTime() + Math.max(1L, holdNanos);
    }

    private void expireNow() {
      validUntilNanos = 0L;
    }

    private void complete(CaptureAttempt attempt) {
      future.complete(attempt);
    }

    private void completeExceptionally(Throwable throwable) {
      future.completeExceptionally(throwable);
    }
  }

  private record SyncCaptureKey(
      ai.floedb.floecat.common.rpc.ResourceId tableId,
      long snapshotId,
      ai.floedb.floecat.catalog.rpc.StatsTarget target,
      String connectorType) {
    /** Builds the dedupe key used by SYNC singleflight capture coalescing. */
    private static SyncCaptureKey of(StatsCaptureRequest request) {
      return new SyncCaptureKey(
          request.tableId(), request.snapshotId(), request.target(), request.connectorType());
    }
  }

  private record TableKey(String accountId, String tableId) {}

  private record AsyncCandidateKey(
      String connectorType,
      String targetCase,
      StatsExecutionMode mode,
      List<StatsKind> requestedKinds,
      boolean samplingRequested) {
    private static AsyncCandidateKey from(StatsCaptureRequest request) {
      List<StatsKind> normalizedKinds = request.requestedKinds().stream().sorted().toList();
      return new AsyncCandidateKey(
          request.connectorType(),
          request.target().getTargetCase().name(),
          request.executionMode(),
          normalizedKinds,
          request.samplingRequested());
    }
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

  /** Emits operator-facing trigger outcome logs with severity by outcome class. */
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
