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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
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
 *   <li>explicit capture execution via {@link #triggerBatch(StatsCaptureBatchRequest)}
 * </ul>
 *
 * <p>Resolution is store-first, then optional synchronous capture, then async enqueue fallback when
 * data is still missing.
 */
@ApplicationScoped
public class StatsOrchestrator {

  private static final Logger LOG = Logger.getLogger(StatsOrchestrator.class);
  private static final String COMPONENT = "service";
  private static final String OPERATION = "stats_orchestrator";
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
  private final Observability observability;

  @Inject
  public StatsOrchestrator(
      StatsStore statsStore,
      ReconcileJobStore reconcileJobStore,
      TableRepository tableRepository,
      StatsEngineRegistry statsEngineRegistry,
      Instance<Observability> observability) {
    this.statsStore = statsStore;
    this.reconcileJobStore = reconcileJobStore;
    this.tableRepository = tableRepository;
    this.statsEngineRegistry = statsEngineRegistry;
    this.observability =
        observability == null || observability.isUnsatisfied() ? null : observability.get();
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
    Optional<TargetStatsRecord> stored = readStore(request);
    if (stored.isPresent()) {
      return stored;
    }

    if (request.executionMode() == StatsExecutionMode.SYNC) {
      CaptureAttempt attempt = captureAttemptSingleflight(request);
      if (attempt.result().isPresent()) {
        return attempt.result().map(StatsCaptureResult::record);
      }
      if (attempt.enqueueEligible()) {
        tryEnqueueAsyncCaptureBatch(List.of(request));
      }
      return Optional.empty();
    }

    tryEnqueueAsyncCaptureBatch(List.of(request));
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
      incrementCounter(
          ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
          1,
          Tag.of(TagKey.TRIGGER, "sync_capture"),
          Tag.of(TagKey.SCOPE, "orchestrator"));
      List<CaptureAttempt> attempts = captureAttemptsSingleflightBatch(syncMissingRequests);
      for (int i = 0; i < attempts.size(); i++) {
        int resolvedIndex = syncMissingIndexes.get(i);
        CaptureAttempt attempt = attempts.get(i);
        if (attempt.result().isPresent()) {
          resolved.set(resolvedIndex, attempt.result().map(StatsCaptureResult::record));
          continue;
        }
        if (attempt.enqueueEligible()) {
          unresolvedForAsync.add(syncMissingRequests.get(i));
        }
      }
    }

    if (!unresolvedForAsync.isEmpty()) {
      incrementCounter(
          ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
          1,
          Tag.of(TagKey.TRIGGER, "async_followup"),
          Tag.of(TagKey.SCOPE, "orchestrator"));
      tryEnqueueAsyncCaptureBatch(unresolvedForAsync);
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

  private void tryEnqueueAsyncCaptureBatch(List<StatsCaptureRequest> requests) {
    if (requests == null || requests.isEmpty()) {
      return;
    }
    Map<TableKey, List<StatsCaptureRequest>> groupedByTable = new LinkedHashMap<>();
    for (StatsCaptureRequest request : requests) {
      if (request.snapshotId() < 0L) {
        recordAsyncSkip(
            request,
            "invalid_snapshot_id",
            "Skipping async enqueue because snapshot id is invalid");
        continue;
      }
      StatsCaptureRequest asyncRequest = toAsyncRequest(request);
      if (!hasAsyncCandidates(asyncRequest)) {
        recordAsyncSkip(
            asyncRequest,
            "no_async_candidates",
            "Skipping async enqueue because no ASYNC candidate engine supports the request");
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
      if (table.isEmpty()) {
        recordAsyncSkip(
            first, "missing_table", "Skipping async enqueue because table lookup failed");
        return;
      }
      if (!table.get().hasUpstream()) {
        recordAsyncSkip(
            first, "missing_upstream", "Skipping async enqueue because table has no upstream");
        return;
      }
      if (!table.get().getUpstream().hasConnectorId()) {
        recordAsyncSkip(
            first,
            "missing_connector_id",
            "Skipping async enqueue because upstream connector id is missing");
        return;
      }
      if (table.get().getUpstream().getConnectorId().getId().isBlank()) {
        recordAsyncSkip(
            first,
            "blank_connector_id",
            "Skipping async enqueue because upstream connector id is blank");
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
      LinkedHashSet<String> scopedColumns = new LinkedHashSet<>();
      for (StatsCaptureRequest request : groupedRequests) {
        snapshotIds.add(request.snapshotId());
        statsTargets.add(StatsTargetScopeCodec.encode(request.target()));
        scopedColumns.addAll(request.columnSelectors());
      }
      ReconcileScope scope =
          ReconcileScope.of(
              namespaceScope,
              tableDisplay,
              List.copyOf(scopedColumns),
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
      incrementCounter(
          ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
          1,
          Tag.of(TagKey.TRIGGER, "async_skip"),
          Tag.of(TagKey.REASON, "enqueue_failure"),
          Tag.of(TagKey.SCOPE, "orchestrator"));
    }
  }

  private void recordAsyncSkip(StatsCaptureRequest request, String reason, String message) {
    LOG.warnf(
        "%s table=%s snapshot=%d reason=%s",
        message, request.tableId(), request.snapshotId(), reason);
    incrementCounter(
        ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
        1,
        Tag.of(TagKey.TRIGGER, "async_skip"),
        Tag.of(TagKey.REASON, reason),
        Tag.of(TagKey.SCOPE, "orchestrator"));
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
              outcomeCounts.merge(item.outcome(), 1, Integer::sum);
              incrementCounter(
                  ServiceMetrics.Stats.BATCH_ITEMS_TOTAL,
                  1,
                  Tag.of(TagKey.RESULT, item.outcome().name()),
                  Tag.of(TagKey.SCOPE, "orchestrator"));
              logTriggerOutcome(item);
            });
    LOG.infof(
        "stats_trigger_batch outcome=%s group_size=%d",
        outcomeCounts, batchRequest.requests().size());
    return registryResult;
  }

  private CaptureAttempt captureAttempt(StatsCaptureRequest request) {
    StatsCaptureBatchItemResult item =
        triggerBatch(StatsCaptureBatchRequest.of(request)).results().getFirst();
    return captureAttemptFromItem(item);
  }

  private CaptureAttempt captureAttemptFromItem(StatsCaptureBatchItemResult item) {
    return switch (item.outcome()) {
      case CAPTURED -> new CaptureAttempt(item.captureResult(), false);
      case QUEUED -> CaptureAttempt.emptyNotEnqueueEligible();
      case UNCAPTURABLE, DEGRADED -> CaptureAttempt.emptyEnqueueEligible();
    };
  }

  private List<CaptureAttempt> captureAttemptsSingleflightBatch(
      List<StatsCaptureRequest> requests) {
    if (requests.isEmpty()) {
      return List.of();
    }

    List<SyncCaptureKey> keysByIndex = new ArrayList<>(requests.size());
    Map<SyncCaptureKey, StatsCaptureRequest> uniqueRequests = new LinkedHashMap<>();
    for (StatsCaptureRequest request : requests) {
      SyncCaptureKey key = SyncCaptureKey.of(request);
      keysByIndex.add(key);
      uniqueRequests.putIfAbsent(key, request);
    }

    Map<SyncCaptureKey, CompletableFuture<CaptureAttempt>> futuresByKey = new LinkedHashMap<>();
    Map<SyncCaptureKey, InFlightCapture> createdByKey = new LinkedHashMap<>();
    List<StatsCaptureRequest> toCapture = new ArrayList<>();
    for (Map.Entry<SyncCaptureKey, StatsCaptureRequest> entry : uniqueRequests.entrySet()) {
      SyncCaptureKey key = entry.getKey();
      while (true) {
        InFlightCapture inFlight = syncCaptureInFlight.get(key);
        if (inFlight != null) {
          if (inFlight.isExpired(System.nanoTime())) {
            syncCaptureInFlight.remove(key, inFlight);
            continue;
          }
          futuresByKey.put(key, inFlight.future());
          break;
        }
        InFlightCapture created = InFlightCapture.inFlight();
        InFlightCapture prior = syncCaptureInFlight.putIfAbsent(key, created);
        if (prior != null) {
          continue;
        }
        createdByKey.put(key, created);
        futuresByKey.put(key, created.future());
        toCapture.add(entry.getValue());
        break;
      }
    }

    if (!toCapture.isEmpty()) {
      try {
        StatsCaptureBatchResult batchResult = triggerBatch(StatsCaptureBatchRequest.of(toCapture));
        for (int i = 0; i < batchResult.results().size(); i++) {
          StatsCaptureBatchItemResult item = batchResult.results().get(i);
          StatsCaptureRequest request = toCapture.get(i);
          SyncCaptureKey key = SyncCaptureKey.of(request);
          InFlightCapture created = createdByKey.get(key);
          if (created == null) {
            continue;
          }
          CaptureAttempt attempt = captureAttemptFromItem(item);
          created.complete(attempt);
          if (attempt.result().isPresent()) {
            long holdNanos = syncPostCaptureHoldNanos(request);
            created.extendHold(holdNanos);
            scheduleInFlightEviction(key, created, holdNanos);
          } else {
            created.expireNow();
          }
          if (created.isExpired(System.nanoTime())) {
            syncCaptureInFlight.remove(key, created);
          }
        }
      } catch (RuntimeException e) {
        for (Map.Entry<SyncCaptureKey, InFlightCapture> createdEntry : createdByKey.entrySet()) {
          InFlightCapture created = createdEntry.getValue();
          created.completeExceptionally(e);
          created.expireNow();
          syncCaptureInFlight.remove(createdEntry.getKey(), created);
        }
        throw e;
      }
    }

    List<CaptureAttempt> attempts = new ArrayList<>(requests.size());
    for (int i = 0; i < requests.size(); i++) {
      attempts.add(awaitInFlightCapture(requests.get(i), futuresByKey.get(keysByIndex.get(i))));
    }
    return List.copyOf(attempts);
  }

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

  private CaptureAttempt awaitInFlightCapture(
      StatsCaptureRequest request, CompletableFuture<CaptureAttempt> inFlight) {
    long waitMillis = syncInFlightWaitMillis(request);
    try {
      return inFlight.get(waitMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return CaptureAttempt.emptyEnqueueEligible();
    } catch (ExecutionException | TimeoutException ignored) {
      return CaptureAttempt.emptyEnqueueEligible();
    }
  }

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

  private void scheduleInFlightEviction(
      SyncCaptureKey key, InFlightCapture capture, long holdNanos) {
    long delayMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(holdNanos));
    CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
        .execute(() -> syncCaptureInFlight.remove(key, capture));
  }

  private void logTriggerOutcome(StatsCaptureBatchItemResult item) {
    StatsCaptureRequest request = item.request();
    if (item.outcome() == StatsTriggerOutcome.CAPTURED) {
      LOG.debugf(
          "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
          item.outcome(), request.tableId(), request.snapshotId(), item.detail());
      return;
    }
    LOG.warnf(
        "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
        item.outcome(), request.tableId(), request.snapshotId(), item.detail());
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

  private record CaptureAttempt(Optional<StatsCaptureResult> result, boolean enqueueEligible) {
    private static CaptureAttempt emptyEnqueueEligible() {
      return new CaptureAttempt(Optional.empty(), true);
    }

    private static CaptureAttempt emptyNotEnqueueEligible() {
      return new CaptureAttempt(Optional.empty(), false);
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
    private static SyncCaptureKey of(StatsCaptureRequest request) {
      return new SyncCaptureKey(
          request.tableId(), request.snapshotId(), request.target(), request.connectorType());
    }
  }

  private record TableKey(String accountId, String tableId) {}
}
