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

package ai.floedb.floecat.service.statistics.engine.impl;

import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.util.Timestamps;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Shared connector-backed capture flow for native statistics engines.
 *
 * <p>Resolves TABLE, COLUMN, and FILE targets.
 *
 * <p>When a batch includes a TABLE request, native capture may persist additional COLUMN/FILE
 * records returned by the connector as an opportunistic single-pass optimization.
 */
abstract class AbstractNativeStatsCaptureEngine implements StatsCaptureEngine {
  private static final int STATS_PERSIST_QUEUE_CAPACITY = 2_048;
  private static final Executor DEFAULT_STATS_PERSIST_EXECUTOR = buildStatsPersistExecutor();
  private static final Set<String> STATS_PERSIST_IN_FLIGHT = ConcurrentHashMap.newKeySet();

  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config);
  }

  private final Logger log;
  private final TableRepository tableRepository;
  private final ConnectorRepository connectorRepository;
  private final CredentialResolver credentialResolver;
  private final StatsStore statsStore;

  // Package-private test seam.
  ConnectorOpener connectorOpener = ConnectorFactory::create;
  // Package-private test seam.
  Executor persistExecutor = DEFAULT_STATS_PERSIST_EXECUTOR;

  protected AbstractNativeStatsCaptureEngine(
      Logger log,
      TableRepository tableRepository,
      ConnectorRepository connectorRepository,
      CredentialResolver credentialResolver,
      StatsStore statsStore) {
    this.log = log;
    this.tableRepository = tableRepository;
    this.connectorRepository = connectorRepository;
    this.credentialResolver = credentialResolver;
    this.statsStore = statsStore;
  }

  @Override
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    StatsCaptureBatchItemResult item =
        captureBatch(StatsCaptureBatchRequest.of(request)).results().getFirst();
    return item.captureResult();
  }

  @Override
  public StatsCaptureBatchResult captureBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureRequest> requests = batchRequest.requests();
    List<StatsCaptureBatchItemResult> results = new ArrayList<>(requests.size());
    for (int i = 0; i < requests.size(); i++) {
      results.add(null);
    }

    Map<CaptureGroupKey, List<Integer>> grouped = new LinkedHashMap<>();
    for (int i = 0; i < requests.size(); i++) {
      StatsCaptureRequest request = requests.get(i);
      if (request.snapshotId() < 0L) {
        results.set(i, StatsCaptureBatchItemResult.uncapturable(request, "invalid snapshot id"));
        continue;
      }
      grouped
          .computeIfAbsent(
              new CaptureGroupKey(request.tableId(), request.snapshotId(), request.executionMode()),
              ignored -> new ArrayList<>())
          .add(i);
    }

    for (Map.Entry<CaptureGroupKey, List<Integer>> entry : grouped.entrySet()) {
      List<Integer> indexes = entry.getValue();
      StatsCaptureRequest representative = requests.get(indexes.getFirst());
      Optional<CaptureContext> captureContext = resolveContext(representative);
      if (captureContext.isEmpty()) {
        for (int idx : indexes) {
          results.set(
              idx,
              StatsCaptureBatchItemResult.uncapturable(requests.get(idx), "no capture context"));
        }
        continue;
      }

      try (FloecatConnector floecatConnector =
          connectorOpener.open(captureContext.get().resolvedConfig())) {
        Set<String> selectors = unionSelectors(requests, indexes);
        Set<StatsTargetKind> requestedTargetKinds = requestedTargetKinds(requests, indexes);
        List<TargetStatsRecord> capturedRecords =
            floecatConnector.captureSnapshotTargetStats(
                captureContext.get().namespaceFq(),
                captureContext.get().tableDisplayName(),
                representative.tableId(),
                representative.snapshotId(),
                selectors,
                requestedTargetKinds);
        if (capturedRecords == null || capturedRecords.isEmpty()) {
          for (int idx : indexes) {
            results.set(
                idx,
                StatsCaptureBatchItemResult.uncapturable(requests.get(idx), "no capture result"));
          }
          continue;
        }

        Optional<Integer> tableRequestIndex =
            indexes.stream()
                .filter(idx -> requests.get(idx).target().hasTable())
                .filter(idx -> findMatchingRecord(capturedRecords, requests.get(idx)).isPresent())
                .findFirst();

        Set<String> persisted = new HashSet<>();
        // If TABLE target is in this group and was returned by the connector, persist the full
        // bundle atomically. Otherwise, fall through to per-target persistence below.
        if (tableRequestIndex.isPresent()) {
          StatsCaptureRequest tableRequest = requests.get(tableRequestIndex.get());
          Optional<TargetStatsRecord> matchedTable =
              findMatchingRecord(capturedRecords, tableRequest);
          if (matchedTable.isPresent()) {
            TargetStatsRecord canonicalTable =
                withCanonicalMetadata(matchedTable.get(), tableRequest);
            int extraRecords = Math.max(capturedRecords.size() - 1, 0);
            if (extraRecords > 0 && log.isDebugEnabled()) {
              log.debugf(
                  "Native bundle persistence engine=%s table=%s snapshot=%d opportunistic_records=%d",
                  id(), tableRequest.tableId(), tableRequest.snapshotId(), extraRecords);
            }
            persistBundleOnce(capturedRecords, tableRequest, canonicalTable, persisted);
          }
        }

        for (int idx : indexes) {
          StatsCaptureRequest request = requests.get(idx);
          Optional<TargetStatsRecord> matched = findMatchingRecord(capturedRecords, request);
          if (matched.isEmpty()) {
            results.set(
                idx, StatsCaptureBatchItemResult.uncapturable(request, "no capture result"));
            continue;
          }
          TargetStatsRecord canonical = withCanonicalMetadata(matched.get(), request);
          if (!request.target().hasTable()) {
            persistOnce(canonical, request, persisted);
          }
          results.set(
              idx,
              StatsCaptureBatchItemResult.captured(
                  request,
                  StatsCaptureResult.forRecord(
                      id(), canonical, Map.of("source", "native_connector"))));
        }
      } catch (RuntimeException e) {
        log.warnf(
            e,
            "Native stats batch capture failed engine=%s table=%s snapshot=%s batch_size=%d",
            id(),
            representative.tableId(),
            representative.snapshotId(),
            indexes.size());
        for (int idx : indexes) {
          results.set(
              idx,
              StatsCaptureBatchItemResult.degraded(
                  requests.get(idx), "capture failed: " + e.getClass().getSimpleName()));
        }
      }
    }

    return StatsCaptureBatchResult.of(results);
  }

  protected ConnectorConfig decorateConnectorConfig(ConnectorConfig baseConfig) {
    return baseConfig;
  }

  protected abstract boolean supportsKind(ConnectorConfig.Kind kind);

  private ConnectorConfig resolveCredentials(
      ConnectorConfig base, AuthConfig auth, ai.floedb.floecat.common.rpc.ResourceId connectorId) {
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connectorId.getAccountId(), connectorId.getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private Optional<TargetStatsRecord> findMatchingRecord(
      List<TargetStatsRecord> records, StatsCaptureRequest request) {
    StatsTarget target = request.target();
    return switch (target.getTargetCase()) {
      case TABLE -> records.stream().filter(TargetStatsRecord::hasTable).findFirst();
      case COLUMN ->
          records.stream()
              .filter(
                  record ->
                      record.hasScalar()
                          && record.hasTarget()
                          && record.getTarget().hasColumn()
                          && record.getTarget().getColumn().getColumnId()
                              == target.getColumn().getColumnId())
              .findFirst();
      case FILE ->
          records.stream()
              .filter(
                  record ->
                      record.hasFile()
                          && record.hasTarget()
                          && record.getTarget().hasFile()
                          && StatsTargetIdentity.storageId(record.getTarget())
                              .equals(StatsTargetIdentity.storageId(target)))
              .findFirst();
      case TARGET_NOT_SET, EXPRESSION -> Optional.empty();
    };
  }

  private void persistBundleOnce(
      List<TargetStatsRecord> records,
      StatsCaptureRequest tableRequest,
      TargetStatsRecord canonicalTable,
      Set<String> persisted) {
    List<TargetStatsRecord> persistCandidates = new ArrayList<>();
    for (TargetStatsRecord record : records) {
      TargetStatsRecord toPersist =
          record.hasTable() ? canonicalTable : withCanonicalMetadata(record, tableRequest);
      String storageId = StatsTargetIdentity.storageId(toPersist.getTarget());
      if (persisted.add(storageId)) {
        persistCandidates.add(toPersist);
      }
    }
    if (persistCandidates.isEmpty()) {
      return;
    }
    PersistSchedule schedule = enqueueStatsPersistence(persistCandidates);
    publishStatsPersistTelemetry(
        tableRequest, schedule.enqueueNanos(), schedule.enqueuedCount(), schedule.dedupedCount());
  }

  private void persistOnce(
      TargetStatsRecord record, StatsCaptureRequest request, Set<String> persisted) {
    String storageId = StatsTargetIdentity.storageId(record.getTarget());
    if (!persisted.add(storageId)) {
      return;
    }
    PersistSchedule schedule = enqueueStatsPersistence(List.of(record));
    publishStatsPersistTelemetry(
        request, schedule.enqueueNanos(), schedule.enqueuedCount(), schedule.dedupedCount());
  }

  private Set<String> unionSelectors(List<StatsCaptureRequest> requests, List<Integer> indexes) {
    Set<String> selectors = new LinkedHashSet<>();
    for (int idx : indexes) {
      StatsCaptureRequest request = requests.get(idx);
      selectors.addAll(request.columnSelectors());
      if (request.target().hasColumn()) {
        selectors.add("#" + request.target().getColumn().getColumnId());
      }
    }
    return selectors;
  }

  private Set<StatsTargetKind> requestedTargetKinds(
      List<StatsCaptureRequest> requests, List<Integer> indexes) {
    Set<StatsTargetKind> kinds = new LinkedHashSet<>();
    for (int idx : indexes) {
      StatsTarget target = requests.get(idx).target();
      if (target.hasTable()) {
        // Table requests can persist the full native bundle in one pass.
        kinds.add(StatsTargetKind.TABLE);
        kinds.add(StatsTargetKind.COLUMN);
        kinds.add(StatsTargetKind.FILE);
        continue;
      }
      if (target.hasColumn()) {
        kinds.add(StatsTargetKind.COLUMN);
      } else if (target.hasFile()) {
        kinds.add(StatsTargetKind.FILE);
      } else if (target.hasExpression()) {
        kinds.add(StatsTargetKind.EXPRESSION);
      }
    }
    return kinds.isEmpty()
        ? Set.of(StatsTargetKind.TABLE, StatsTargetKind.COLUMN, StatsTargetKind.FILE)
        : Set.copyOf(kinds);
  }

  private Optional<CaptureContext> resolveContext(StatsCaptureRequest request) {
    Optional<Table> table = tableRepository.getById(request.tableId());
    if (table.isEmpty() || !table.get().hasUpstream()) {
      return Optional.empty();
    }
    UpstreamRef upstream = table.get().getUpstream();
    if (!upstream.hasConnectorId()
        || upstream.getNamespacePathCount() == 0
        || upstream.getTableDisplayName().isBlank()) {
      return Optional.empty();
    }

    Optional<Connector> connector = connectorRepository.getById(upstream.getConnectorId());
    if (connector.isEmpty()) {
      return Optional.empty();
    }
    ConnectorConfig baseConfig = ConnectorConfigMapper.fromProto(connector.get());
    if (!supportsKind(baseConfig.kind())) {
      return Optional.empty();
    }
    ConnectorConfig resolved =
        resolveCredentials(
            decorateConnectorConfig(baseConfig),
            connector.get().hasAuth() ? connector.get().getAuth() : AuthConfig.getDefaultInstance(),
            upstream.getConnectorId());
    return Optional.of(
        new CaptureContext(
            String.join(".", upstream.getNamespacePathList()),
            upstream.getTableDisplayName(),
            resolved));
  }

  private record CaptureContext(
      String namespaceFq, String tableDisplayName, ConnectorConfig resolvedConfig) {}

  private record CaptureGroupKey(
      ai.floedb.floecat.common.rpc.ResourceId tableId,
      long snapshotId,
      StatsExecutionMode executionMode) {}

  /**
   * Schedules persistence for captured records with best-effort in-flight dedupe.
   *
   * <p>Scheduling prefers asynchronous execution. If the async queue rejects, persistence falls
   * back to caller-thread execution to preserve SYNC read-after-write behavior.
   */
  private PersistSchedule enqueueStatsPersistence(List<TargetStatsRecord> records) {
    if (records == null || records.isEmpty()) {
      return PersistSchedule.EMPTY;
    }
    long startNs = System.nanoTime();
    int dedupedCount = 0;
    List<PersistEntry> accepted = new ArrayList<>();
    for (TargetStatsRecord record : records) {
      String persistKey = persistInFlightKey(record);
      if (!STATS_PERSIST_IN_FLIGHT.add(persistKey)) {
        dedupedCount++;
        continue;
      }
      accepted.add(new PersistEntry(persistKey, record));
    }
    if (accepted.isEmpty()) {
      return new PersistSchedule(System.nanoTime() - startNs, 0, dedupedCount);
    }
    try {
      persistExecutor.execute(() -> persistAccepted(accepted));
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "Async stats persistence rejected engine=%s enqueued=%d deduped_inflight=%d"
              + " fallback=caller_thread",
          id(),
          accepted.size(),
          dedupedCount);
      persistAccepted(accepted);
      return new PersistSchedule(System.nanoTime() - startNs, accepted.size(), dedupedCount);
    }
    return new PersistSchedule(System.nanoTime() - startNs, accepted.size(), dedupedCount);
  }

  /**
   * Persists accepted records and always releases their in-flight guards.
   *
   * <p>Failures are logged per record and summarized at DEBUG level; they do not propagate to
   * query-time callers.
   */
  private void persistAccepted(List<PersistEntry> accepted) {
    long startNs = System.nanoTime();
    int persistedCount = 0;
    int failedCount = 0;
    for (PersistEntry entry : accepted) {
      try {
        if (persistRecord(entry.record())) {
          persistedCount++;
        } else {
          failedCount++;
          if (log.isDebugEnabled()) {
            log.debugf("async stats persist failed record=%s engine=%s", entry.storageId(), id());
          }
        }
      } finally {
        STATS_PERSIST_IN_FLIGHT.remove(entry.storageId());
      }
    }
    if (log.isDebugEnabled()) {
      log.debugf(
          "engine=%s async stats persist done records=%d persisted=%d failed=%d durationMs=%.1f",
          id(),
          accepted.size(),
          persistedCount,
          failedCount,
          (System.nanoTime() - startNs) / 1_000_000.0);
    }
  }

  /** Writes a single record into the authoritative {@link StatsStore}. */
  private boolean persistRecord(TargetStatsRecord record) {
    try {
      statsStore.putTargetStats(record);
      return true;
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "Failed persisting captured stats record engine=%s target=%s",
          id(),
          record.getTarget());
      return false;
    }
  }

  /** Returns the process-local dedupe key for in-flight persistence of a record identity. */
  private String persistInFlightKey(TargetStatsRecord record) {
    return tableIdentity(record.getTableId())
        + "#"
        + record.getSnapshotId()
        + "#"
        + StatsTargetIdentity.storageId(record.getTarget());
  }

  /** Canonical table identity used as part of async persistence dedupe keys. */
  private String tableIdentity(ResourceId tableId) {
    if (tableId == null) {
      return "unknown";
    }
    return tableId.getAccountId() + "|" + tableId.getKind().name() + "|" + tableId.getId();
  }

  /** Emits span attributes/events for persistence scheduling overhead and dedupe outcome. */
  private void publishStatsPersistTelemetry(
      StatsCaptureRequest request, long statsPersistNanos, int persistedCount, int dedupedCount) {
    if (persistedCount <= 0 && dedupedCount <= 0) {
      return;
    }
    Span parent = Span.current();
    if (parent == null || !parent.getSpanContext().isValid()) {
      return;
    }
    double statsPersistMs = Math.max(0L, statsPersistNanos) / 1_000_000.0;
    Attributes attrs =
        Attributes.builder()
            .put("engine_id", id())
            .put("table_id", request.tableId().getId())
            .put("snapshot_id", request.snapshotId())
            .put("persisted_count", persistedCount)
            .put("deduped_inflight_count", dedupedCount)
            .put("stats_persist_ms", statsPersistMs)
            .build();
    parent.addEvent("floecat.stats.persist", attrs);

    Span persistSpan =
        GlobalOpenTelemetry.getTracer("floecat.service")
            .spanBuilder("floecat.stats.persist")
            .startSpan();
    try {
      persistSpan.setAttribute("engine_id", id());
      persistSpan.setAttribute("table_id", request.tableId().getId());
      persistSpan.setAttribute("snapshot_id", request.snapshotId());
      persistSpan.setAttribute("persisted_count", persistedCount);
      persistSpan.setAttribute("deduped_inflight_count", dedupedCount);
      persistSpan.setAttribute("stats_persist_ms", statsPersistMs);
    } finally {
      persistSpan.end();
    }
  }

  private static Executor buildStatsPersistExecutor() {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            1,
            1,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(STATS_PERSIST_QUEUE_CAPACITY),
            runnable -> {
              Thread thread = new Thread(runnable, "floecat-stats-persist");
              thread.setDaemon(true);
              return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private record PersistEntry(String storageId, TargetStatsRecord record) {}

  private record PersistSchedule(long enqueueNanos, int enqueuedCount, int dedupedCount) {
    private static final PersistSchedule EMPTY = new PersistSchedule(0L, 0, 0);
  }

  private TargetStatsRecord withCanonicalMetadata(
      TargetStatsRecord record, StatsCaptureRequest request) {
    long nowMs = System.currentTimeMillis();
    StatsMetadata.Builder metadata =
        (record.hasMetadata() ? record.getMetadata() : StatsMetadata.getDefaultInstance())
            .toBuilder();

    if (metadata.getProducer() == StatsProducer.SPROD_UNSPECIFIED) {
      metadata.setProducer(StatsProducer.SPROD_SOURCE_NATIVE);
    }
    if (metadata.getCompleteness() == StatsCompleteness.SC_UNSPECIFIED) {
      // Native connector captures are treated as complete unless upstream metadata states
      // otherwise.
      metadata.setCompleteness(StatsCompleteness.SC_COMPLETE);
    }
    metadata.setCaptureMode(
        request.executionMode() == StatsExecutionMode.SYNC
            ? StatsCaptureMode.SCM_SYNC
            : StatsCaptureMode.SCM_ASYNC);

    if (!metadata.hasConfidenceLevel()) {
      // Callers that do not emit confidence default by completeness.
      double defaultConfidence =
          metadata.getCompleteness() == StatsCompleteness.SC_COMPLETE ? 1.0d : 0.5d;
      metadata.setConfidenceLevel(defaultConfidence);
    }
    if (!metadata.hasCoverage()) {
      metadata.setCoverage(StatsCoverage.getDefaultInstance());
    }
    if (!metadata.hasCapturedAt()) {
      // Timestamp reflects Floecat normalization/persistence time when source capture time is
      // absent.
      metadata.setCapturedAt(Timestamps.fromMillis(nowMs));
    }
    if (!metadata.hasRefreshedAt()) {
      // Timestamp reflects Floecat refresh/persistence time when source refresh time is absent.
      metadata.setRefreshedAt(Timestamps.fromMillis(nowMs));
    }

    if (!metadata.containsProperties("method")) {
      metadata.putProperties("method", "connector_native");
    }
    if (!metadata.containsProperties("engine_id")) {
      metadata.putProperties("engine_id", id());
    }

    return record.toBuilder().setMetadata(metadata.build()).build();
  }

  protected static ConnectorConfig applyIcebergOverrides(ConnectorConfig base) {
    if (base.kind() != ConnectorConfig.Kind.ICEBERG) {
      return base;
    }
    Map<String, String> options = new LinkedHashMap<>(base.options());
    String external = options.get("external.metadata-location");
    if (external == null || external.isBlank()) {
      return base;
    }
    options.putIfAbsent("iceberg.source", "filesystem");
    return options.equals(base.options())
        ? base
        : new ConnectorConfig(base.kind(), base.displayName(), base.uri(), options, base.auth());
  }
}
