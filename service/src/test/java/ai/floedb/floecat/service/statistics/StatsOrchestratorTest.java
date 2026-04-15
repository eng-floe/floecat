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
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.StatsTargetScopeCodec;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
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
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(
                    StatsCaptureBatchItemResult.captured(
                        request,
                        StatsCaptureResult.forRecord("native", record, java.util.Map.of())))));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).contains(record);
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncMissUncapturableEnqueuesAsyncFollowUp() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(StatsCaptureBatchItemResult.uncapturable(request, "unsupported"))));
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
    assertThat(scopeCaptor.getValue().destinationSnapshotIds()).containsExactly(42L);
    assertThat(scopeCaptor.getValue().destinationStatsTargets())
        .containsExactly(StatsTargetScopeCodec.encode(request.target()));
  }

  @Test
  void syncUncapturableExpressionTargetEnqueuesAsync() {
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
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(StatsCaptureBatchItemResult.uncapturable(request, "unsupported"))));
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
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(StatsCaptureBatchItemResult.uncapturable(req2, "no capture result"))));
    when(registry.candidates(any())).thenReturn(List.of());

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(StatsCaptureBatchRequest.of(List.of(req1, req2)));

    assertThat(out).hasSize(2);
    assertThat(out.get(0)).contains(rec1);
    assertThat(out.get(1)).isEmpty();
  }

  @Test
  void resolveBatchEnqueuesOnlyUnresolvedTargetsForAsyncFollowUp() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest tableReq = request(StatsExecutionMode.SYNC);
    StatsCaptureRequest columnReq =
        request(StatsExecutionMode.SYNC, StatsTargetIdentity.columnTarget(7L), 42L);
    TargetStatsRecord tableRecord = tableRecord(tableReq);
    when(statsStore.getTargetStats(tableReq.tableId(), tableReq.snapshotId(), tableReq.target()))
        .thenReturn(Optional.empty());
    when(statsStore.getTargetStats(columnReq.tableId(), columnReq.snapshotId(), columnReq.target()))
        .thenReturn(Optional.empty());
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(
                    StatsCaptureBatchItemResult.captured(
                        tableReq,
                        StatsCaptureResult.forRecord("native", tableRecord, java.util.Map.of())),
                    StatsCaptureBatchItemResult.degraded(columnReq, "partial"))));
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(tableReq.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(tableReq.tableId())));

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(StatsCaptureBatchRequest.of(List.of(tableReq, columnReq)));

    assertThat(out).hasSize(2);
    assertThat(out.get(0)).contains(tableRecord);
    assertThat(out.get(1)).isEmpty();

    ArgumentCaptor<ai.floedb.floecat.reconciler.jobs.ReconcileScope> scopeCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.jobs.ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(tableReq.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationSnapshotIds()).containsExactly(42L);
    assertThat(scopeCaptor.getValue().destinationStatsTargets())
        .containsExactly(StatsTargetScopeCodec.encode(columnReq.target()));
  }

  @Test
  void resolveBatchMixedStoreHitSyncCaptureAndUncapturableEnqueuesOnce() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest storeHitReq = request(StatsExecutionMode.SYNC);
    StatsCaptureRequest syncCapturedReq =
        request(StatsExecutionMode.SYNC, StatsTargetIdentity.columnTarget(7L), 42L);
    StatsCaptureRequest uncapturableReq =
        request(StatsExecutionMode.SYNC, StatsTargetIdentity.columnTarget(9L), 42L);

    TargetStatsRecord storeHitRecord = tableRecord(storeHitReq);
    TargetStatsRecord syncCapturedRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(syncCapturedReq.tableId())
            .setSnapshotId(syncCapturedReq.snapshotId())
            .setTarget(syncCapturedReq.target())
            .setScalar(
                ai.floedb.floecat.catalog.rpc.ScalarStats.newBuilder().setNullCount(1L).build())
            .build();

    when(statsStore.getTargetStats(
            storeHitReq.tableId(), storeHitReq.snapshotId(), storeHitReq.target()))
        .thenReturn(Optional.of(storeHitRecord));
    when(statsStore.getTargetStats(
            syncCapturedReq.tableId(), syncCapturedReq.snapshotId(), syncCapturedReq.target()))
        .thenReturn(Optional.empty());
    when(statsStore.getTargetStats(
            uncapturableReq.tableId(), uncapturableReq.snapshotId(), uncapturableReq.target()))
        .thenReturn(Optional.empty());
    when(registry.captureBatch(any()))
        .thenReturn(
            StatsCaptureBatchResult.of(
                List.of(
                    StatsCaptureBatchItemResult.captured(
                        syncCapturedReq,
                        StatsCaptureResult.forRecord(
                            "native", syncCapturedRecord, java.util.Map.of())),
                    StatsCaptureBatchItemResult.uncapturable(uncapturableReq, "unsupported"))));
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(storeHitReq.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(storeHitReq.tableId())));

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(
            StatsCaptureBatchRequest.of(List.of(storeHitReq, syncCapturedReq, uncapturableReq)));

    assertThat(out).hasSize(3);
    assertThat(out.get(0)).contains(storeHitRecord);
    assertThat(out.get(1)).contains(syncCapturedRecord);
    assertThat(out.get(2)).isEmpty();

    ArgumentCaptor<ai.floedb.floecat.reconciler.jobs.ReconcileScope> scopeCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.jobs.ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(storeHitReq.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationSnapshotIds()).containsExactly(42L);
    assertThat(scopeCaptor.getValue().destinationStatsTargets())
        .containsExactly(StatsTargetScopeCodec.encode(uncapturableReq.target()));
  }

  @Test
  void resolveBatchGroupsAsyncMissesForSameTableIntoSingleEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest req1 =
        request(StatsExecutionMode.ASYNC, StatsTargetIdentity.columnTarget(3L), 42L);
    StatsCaptureRequest req2 =
        request(StatsExecutionMode.ASYNC, StatsTargetIdentity.columnTarget(5L), 43L);
    when(statsStore.getTargetStats(req1.tableId(), req1.snapshotId(), req1.target()))
        .thenReturn(Optional.empty());
    when(statsStore.getTargetStats(req2.tableId(), req2.snapshotId(), req2.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(req1.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(req1.tableId())));

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(StatsCaptureBatchRequest.of(List.of(req1, req2)));

    assertThat(out).hasSize(2);
    assertThat(out.get(0)).isEmpty();
    assertThat(out.get(1)).isEmpty();

    ArgumentCaptor<ai.floedb.floecat.reconciler.jobs.ReconcileScope> scopeCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.jobs.ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(req1.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationSnapshotIds()).containsExactly(42L, 43L);
    assertThat(scopeCaptor.getValue().destinationStatsTargets())
        .containsExactly(
            StatsTargetScopeCodec.encode(req1.target()),
            StatsTargetScopeCodec.encode(req2.target()));
  }

  @Test
  void resolveBatchMultiTableEnqueuesPerTable() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    ResourceId tableA = ResourceId.newBuilder().setAccountId("acct").setId("table-a").build();
    ResourceId tableB = ResourceId.newBuilder().setAccountId("acct").setId("table-b").build();
    StatsTarget target =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    StatsCaptureRequest reqA =
        StatsCaptureRequest.builder(tableA, 42L, target)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr-a")
            .build();
    StatsCaptureRequest reqB =
        StatsCaptureRequest.builder(tableB, 43L, target)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr-b")
            .build();

    when(statsStore.getTargetStats(reqA.tableId(), reqA.snapshotId(), reqA.target()))
        .thenReturn(Optional.empty());
    when(statsStore.getTargetStats(reqB.tableId(), reqB.snapshotId(), reqB.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(tableA)).thenReturn(Optional.of(tableWithUpstream(tableA)));
    when(tableRepository.getById(tableB)).thenReturn(Optional.of(tableWithUpstream(tableB)));

    List<Optional<TargetStatsRecord>> out =
        orchestrator.resolveBatch(StatsCaptureBatchRequest.of(List.of(reqA, reqB)));

    assertThat(out).hasSize(2);
    assertThat(out.get(0)).isEmpty();
    assertThat(out.get(1)).isEmpty();
    verify(jobStore, Mockito.times(2))
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            any());
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
