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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
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
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class StatsOrchestratorTest {

  @Test
  void returnsStoreHitDirectly() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.of(record));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).contains(record);
    verify(registry, never()).capture(any());
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncMissUsesEngineCaptureBeforeEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request))
        .thenReturn(
            Optional.of(StatsCaptureResult.forRecord("native", record, java.util.Map.of())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).contains(record);
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncConcurrentMissesShareInFlightCapture() throws Exception {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .latencyBudget(Optional.of(Duration.ofSeconds(1)))
            .build();

    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());

    AtomicInteger captureCalls = new AtomicInteger();
    CountDownLatch captureStarted = new CountDownLatch(1);
    CountDownLatch releaseCapture = new CountDownLatch(1);
    when(registry.capture(request))
        .thenAnswer(
            ignored -> {
              captureCalls.incrementAndGet();
              captureStarted.countDown();
              assertThat(releaseCapture.await(2, TimeUnit.SECONDS)).isTrue();
              return Optional.of(
                  StatsCaptureResult.forRecord("native", record, java.util.Map.of()));
            });

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CompletableFuture<Optional<TargetStatsRecord>> first =
          CompletableFuture.supplyAsync(() -> orchestrator.resolve(request), pool);
      assertThat(captureStarted.await(2, TimeUnit.SECONDS)).isTrue();
      CompletableFuture<Optional<TargetStatsRecord>> second =
          CompletableFuture.supplyAsync(() -> orchestrator.resolve(request), pool);

      releaseCapture.countDown();

      assertThat(first.get(2, TimeUnit.SECONDS)).contains(record);
      assertThat(second.get(2, TimeUnit.SECONDS)).contains(record);
      assertThat(captureCalls.get()).isEqualTo(1);
      verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void syncSequentialMissesReuseRecentCaptureResultWithoutRecapture() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    AtomicInteger captureCalls = new AtomicInteger();
    when(registry.capture(request))
        .thenAnswer(
            ignored -> {
              captureCalls.incrementAndGet();
              return Optional.of(
                  StatsCaptureResult.forRecord("native", record, java.util.Map.of()));
            });

    Optional<TargetStatsRecord> first = orchestrator.resolve(request);
    Optional<TargetStatsRecord> second = orchestrator.resolve(request);

    assertThat(first).contains(record);
    assertThat(second).contains(record);
    assertThat(captureCalls.get()).isEqualTo(1);
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncTimeoutWhileCaptureInFlightDoesNotFallbackRecapture() throws Exception {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .latencyBudget(Optional.of(Duration.ofMillis(5)))
            .build();

    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());

    AtomicInteger captureCalls = new AtomicInteger();
    CountDownLatch captureStarted = new CountDownLatch(1);
    CountDownLatch releaseCapture = new CountDownLatch(1);
    when(registry.capture(request))
        .thenAnswer(
            ignored -> {
              captureCalls.incrementAndGet();
              captureStarted.countDown();
              assertThat(releaseCapture.await(2, TimeUnit.SECONDS)).isTrue();
              return Optional.of(
                  StatsCaptureResult.forRecord("native", record, java.util.Map.of()));
            });

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<Optional<TargetStatsRecord>> first =
          CompletableFuture.supplyAsync(() -> orchestrator.resolve(request), pool);
      assertThat(captureStarted.await(2, TimeUnit.SECONDS)).isTrue();

      Optional<TargetStatsRecord> second = orchestrator.resolve(request);
      assertThat(second).isEmpty();
      assertThat(captureCalls.get()).isEqualTo(1);

      releaseCapture.countDown();
      assertThat(first.get(2, TimeUnit.SECONDS)).contains(record);

      Optional<TargetStatsRecord> third = orchestrator.resolve(request);
      assertThat(third).contains(record);
      assertThat(captureCalls.get()).isEqualTo(1);
    } finally {
      pool.shutdownNow();
    }
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncMissUnsupportedDoesNotEnqueueWithoutSecondLookup() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request))
        .thenThrow(new StatsUnsupportedTargetException(StatsTargetType.TABLE, request));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(registry, never()).candidates(any());
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissEnqueuesScopedStatsJob() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(registry, never()).capture(any());
    ArgumentCaptor<ai.floedb.floecat.reconciler.jobs.ReconcileScope> scopeCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.jobs.ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationNamespacePaths()).containsExactly(List.of("db"));
    assertThat(scopeCaptor.getValue().destinationTableDisplayName()).isEqualTo("events");
  }

  @Test
  void syncUnsupportedTargetDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.SYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request))
        .thenThrow(new StatsUnsupportedTargetException(StatsTargetType.EXPRESSION, request));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissUnsupportedTargetDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
    verify(registry, never()).capture(any());
  }

  @Test
  void asyncMissWithZeroSnapshotAndNoCandidatesDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            0L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithZeroSnapshotCanEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            0L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            any());
  }

  @Test
  void asyncMissWithNoAsyncCandidatesDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId(), TableFormat.TF_UNSPECIFIED)));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithNoUpstreamDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(request.tableId())
                    .setDisplayName("events")
                    .build()));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithBlankConnectorIdDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(request.tableId())
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(ResourceId.newBuilder().setAccountId("acct").setId(""))
                            .setFormat(TableFormat.TF_ICEBERG)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithExpressionTargetEnqueuesWhenEngineSupportsIt() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            any());
  }

  @Test
  void triggerDelegatesThroughBatchRouting() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    StatsCaptureResult captureResult =
        StatsCaptureResult.forRecord("native", record, java.util.Map.of());
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(StatsCaptureBatchItemResult.captured(request, captureResult))));

    StatsTriggerResult result = orchestrator.trigger(request);

    assertThat(result.outcome()).isEqualTo(StatsTriggerOutcome.CAPTURED);
    assertThat(result.captureResult()).contains(captureResult);
    verify(registry, never()).capture(any());
    verify(registry).captureBatch(any(StatsCaptureBatchRequest.class));
  }

  @Test
  void triggerBatchReturnsRegistryPerItemOutcomes() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    StatsCaptureBatchResult expected =
        StatsCaptureBatchResult.of(
            List.of(StatsCaptureBatchItemResult.uncapturable(request, "target unsupported")));
    when(registry.captureBatch(any())).thenReturn(expected);

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(request));

    assertThat(result).isEqualTo(expected);
  }

  @Test
  void resolveBatchPreservesRequestOrder() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest req1 = request(StatsExecutionMode.SYNC);
    StatsCaptureRequest req2 =
        request(
            StatsExecutionMode.SYNC,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            43L);
    TargetStatsRecord rec1 = tableRecord(req1);
    when(statsStore.getTargetStats(req1.tableId(), req1.snapshotId(), req1.target()))
        .thenReturn(Optional.of(rec1));
    when(statsStore.getTargetStats(req2.tableId(), req2.snapshotId(), req2.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(req2)).thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(StatsCaptureBatchRequest.of(List.of(req1, req2)));

    assertThat(out).hasSize(2);
    assertThat(out.get(0)).contains(rec1);
    assertThat(out.get(1)).isEmpty();
  }

  private static StatsCaptureRequest request(StatsExecutionMode mode) {
    return request(
        mode,
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
        42L);
  }

  private static StatsCaptureRequest request(
      StatsExecutionMode mode, StatsTarget target, long snapshotId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
            snapshotId,
            target)
        .executionMode(mode)
        .connectorType("iceberg")
        .correlationId("corr")
        .build();
  }

  private static TargetStatsRecord tableRecord(StatsCaptureRequest request) {
    return TargetStatsRecord.newBuilder()
        .setTableId(request.tableId())
        .setSnapshotId(request.snapshotId())
        .setTarget(request.target())
        .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
        .build();
  }

  private static Table tableWithUpstream(ResourceId tableId) {
    return tableWithUpstream(tableId, TableFormat.TF_ICEBERG);
  }

  private static Table tableWithUpstream(ResourceId tableId, TableFormat format) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setDisplayName("events")
        .setUpstream(
            ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                .setConnectorId(
                    ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build())
                .setFormat(format)
                .addNamespacePath("db")
                .setTableDisplayName("events")
                .build())
        .build();
  }
}
